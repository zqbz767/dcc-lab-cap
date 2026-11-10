"""One replica of the key-value store (Lab: Consistency Models & CAP).

Run a 3-node cluster (node 1 = leaseholder):
  python3 server.py --port 50051 --peers 127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053
  python3 server.py --port 50052 --peers ...same list...
  python3 server.py --port 50053 --peers ...same list...

Design
  * Node 1 (first address in --peers, or --leader) is the Leaseholder for keyspace [a-z].
  * LINEARIZABLE reads are served by the leaseholder; other nodes proxy to it.
    If the leaseholder is unreachable the read FAILS (CP behaviour).
  * FOLLOWER_STALE reads are served from the local state machine. Staleness = time since
    this node last heard from the leaseholder (writes or heartbeats). If it is larger than
    max_staleness_ms the read fails with STALE_READ_EXCEEDED. max_staleness_ms = 0 -> unbounded.
  * QUORUM_CP writes need a majority (N//2+1) of ACKs (the coordinator counts itself);
    otherwise nothing is applied on the coordinator and the reply says ReplicaUnavailableError.
  * ALWAYS_AP writes are applied locally at once; unreachable peers get a hinted handoff that is
    retried by the heartbeat loop, so partitions heal and replicas converge.
  * Conflict resolution = Last-Write-Wins on (timestamp, origin address).
  * SetPartition is the failure-injection hook: the node refuses to talk to the listed peers
    (in both directions), which simulates a network partition deterministically.
"""
import argparse
import threading
import time
from concurrent import futures

import grpc

import kvstore_pb2 as pb
import kvstore_pb2_grpc as pb_grpc

STALE_READ_EXCEEDED = "STALE_READ_EXCEEDED"


def now_us():
    return time.time_ns() // 1000


class KVServicer(pb_grpc.KeyValueStoreServicer):
    def __init__(self, heartbeat_s=0.2, peer_timeout_s=1.0, wan_delay_ms=0):
        self._lock = threading.RLock()
        self._data = {}          # key -> (value, timestamp, origin)
        self._seen = {}          # idempotency_key -> PutReply
        self._hints = {}         # peer -> {key: entry} not yet delivered
        self._blocked = set()    # peers cut off by SetPartition
        self._stubs = {}
        self._channels = []
        self._pool = None
        self._hb_thread = None
        self._stop = threading.Event()
        self._last_sync = time.monotonic()
        self._hb_s = heartbeat_s
        self._peer_timeout = peer_timeout_s
        self._wan_delay_s = wan_delay_ms / 1000.0   # simulated extra latency per cross-node call
        self.addr = None
        self.nodes = []
        self.leader = None

    # ---- setup ---------------------------------------------------------
    def configure(self, addr, nodes, leader=None):
        self.addr = addr
        self.nodes = list(nodes)
        self.leader = leader or self.nodes[0]
        for p in self.peers:
            ch = grpc.insecure_channel(p)
            self._channels.append(ch)
            self._stubs[p] = pb_grpc.KeyValueStoreStub(ch)
            self._hints[p] = {}
        self._pool = futures.ThreadPoolExecutor(max_workers=max(2, 2 * len(self.peers)))
        self._last_sync = time.monotonic()
        self._hb_thread = threading.Thread(target=self._heartbeat_loop, daemon=True)
        self._hb_thread.start()

    def close(self):
        self._stop.set()
        if self._hb_thread:
            self._hb_thread.join(timeout=2)
        if self._pool:
            self._pool.shutdown(wait=False)
        for ch in self._channels:
            ch.close()

    @property
    def peers(self):
        return [n for n in self.nodes if n != self.addr]

    @property
    def is_leader(self):
        return self.addr == self.leader

    @property
    def majority(self):
        return len(self.nodes) // 2 + 1

    def leaseholder_for(self, key):
        """Node 1 is the leaseholder of the whole keyspace [a-z] (single range in this lab)."""
        return self.leader

    def lag_ms(self):
        if self.is_leader:
            return 0.0
        return (time.monotonic() - self._last_sync) * 1000.0

    # ---- state machine (Last-Write-Wins) -------------------------------
    def _merge(self, key, value, ts, origin):
        with self._lock:
            cur = self._data.get(key)
            if cur is None or (ts, origin) > (cur[1], cur[2]):
                self._data[key] = (value, ts, origin)
                return True
            return False

    def _add_hint(self, peer, entry):
        with self._lock:
            cur = self._hints[peer].get(entry[0])
            if cur is None or (entry[2], entry[3]) > (cur[2], cur[3]):
                self._hints[peer][entry[0]] = entry

    # ---- replication ---------------------------------------------------
    def _send(self, peer, entries, heartbeat=False):
        with self._lock:
            if peer in self._blocked:
                return False
        if self._wan_delay_s:
            time.sleep(self._wan_delay_s)
        req = pb.ReplicateRequest(
            sender=self.addr, heartbeat=heartbeat,
            entries=[pb.Entry(key=k, value=v, timestamp=t, origin=o) for k, v, t, o in entries])
        try:
            self._stubs[peer].Replicate(req, timeout=self._peer_timeout)
            return True
        except grpc.RpcError:
            return False

    def _broadcast(self, entries):
        """Send entries to all peers in parallel. Returns (number of acks, failed peers)."""
        futs = {p: self._pool.submit(self._send, p, entries) for p in self.peers}
        acks, failed = 0, []
        for p, f in futs.items():
            if f.result():
                acks += 1
            else:
                failed.append(p)
        return acks, failed

    def _heartbeat_loop(self):
        while not self._stop.wait(self._hb_s):
            for p in self.peers:
                with self._lock:
                    pending = list(self._hints[p].values())
                if self._send(p, pending, heartbeat=True):
                    with self._lock:      # delivered: drop hints unless a newer one arrived meanwhile
                        for e in pending:
                            if self._hints[p].get(e[0]) == e:
                                del self._hints[p][e[0]]

    # ---- RPCs ----------------------------------------------------------
    def Replicate(self, request, context):
        with self._lock:
            blocked = request.sender in self._blocked
        if blocked:
            context.abort(grpc.StatusCode.UNAVAILABLE, "network partition")
        for e in request.entries:
            self._merge(e.key, e.value, e.timestamp, e.origin)
        if request.sender == self.leader:
            with self._lock:
                self._last_sync = time.monotonic()
        return pb.ReplicateReply(ok=True)

    def Put(self, request, context):
        idem = request.idempotency_key
        with self._lock:
            if idem and idem in self._seen:
                return self._seen[idem]          # retry of an already-applied write
        ts = request.timestamp or now_us()
        entry = (request.key, request.value, ts, self.addr)
        total = len(self.nodes)

        if request.strategy == pb.QUORUM_CP:
            peer_acks, failed = self._broadcast([entry])
            acks = 1 + peer_acks
            if acks < self.majority:
                return pb.PutReply(
                    success=False, acks_received=acks,
                    error_message=f"ReplicaUnavailableError: quorum lost "
                                  f"({acks}/{total} acks, need {self.majority})")
            self._merge(*entry)
            for p in failed:                      # committed; let the slow peers catch up later
                self._add_hint(p, entry)
        else:  # ALWAYS_AP: accept on any available node, replicate best-effort
            self._merge(*entry)
            peer_acks, failed = self._broadcast([entry])
            for p in failed:
                self._add_hint(p, entry)
            acks = 1 + peer_acks

        reply = pb.PutReply(success=True, acks_received=acks)
        if idem:
            with self._lock:
                self._seen[idem] = reply
        return reply

    def _local_get(self, key, stale):
        with self._lock:
            cur = self._data.get(key)
        if cur is None:
            return pb.GetReply(found=False, is_stale=stale)
        return pb.GetReply(value=cur[0], found=True, read_timestamp=cur[1], is_stale=stale)

    def Get(self, request, context):
        if request.consistency == pb.LINEARIZABLE:
            leader = self.leaseholder_for(request.key)
            if self.is_leader:
                return self._local_get(request.key, stale=False)
            # proxy to the leaseholder
            with self._lock:
                blocked = leader in self._blocked
            reply = None
            if not blocked:
                if self._wan_delay_s:
                    time.sleep(self._wan_delay_s)
                try:
                    reply = self._stubs[leader].Get(request, timeout=self._peer_timeout)
                except grpc.RpcError:
                    reply = None
            if reply is None:
                context.abort(grpc.StatusCode.UNAVAILABLE,
                              f"ReplicaUnavailableError: leaseholder {leader} unreachable")
            return reply

        # FOLLOWER_STALE: serve from local state machine
        lag = self.lag_ms()
        if request.max_staleness_ms and lag > request.max_staleness_ms:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION,
                          f"{STALE_READ_EXCEEDED}: replica lag {lag:.0f}ms > "
                          f"max_staleness_ms={request.max_staleness_ms}")
        return self._local_get(request.key, stale=not self.is_leader)

    def SetPartition(self, request, context):
        with self._lock:
            self._blocked = set(request.blocked)
        return pb.PartitionReply(ok=True)


def build_server(port=0, host="127.0.0.1", heartbeat_s=0.2, peer_timeout_s=1.0,
                 wan_delay_ms=0, max_workers=16):
    """Create and start a server. Returns (server, servicer, bound_port).
    Call servicer.configure(addr, nodes, leader) once all ports are known."""
    servicer = KVServicer(heartbeat_s, peer_timeout_s, wan_delay_ms)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    pb_grpc.add_KeyValueStoreServicer_to_server(servicer, server)
    bound = server.add_insecure_port(f"{host}:{port}")
    if not bound:
        raise RuntimeError(f"could not bind {host}:{port}")
    server.start()
    return server, servicer, bound


def main():
    p = argparse.ArgumentParser(description="KV store replica")
    p.add_argument("--port", type=int, default=50051)
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--peers", default="127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053",
                   help="comma-separated addresses of ALL nodes (including this one)")
    p.add_argument("--leader", default=None, help="leaseholder address (default: first in --peers)")
    p.add_argument("--wan-delay-ms", type=int, default=0,
                   help="simulated extra latency added to every cross-node call")
    p.add_argument("--heartbeat-ms", type=int, default=200)
    args = p.parse_args()

    nodes = args.peers.split(",")
    server, servicer, bound = build_server(args.port, args.host, args.heartbeat_ms / 1000.0,
                                           1.0, args.wan_delay_ms)
    addr = f"{args.host}:{bound}"
    servicer.configure(addr, nodes, args.leader)
    role = "leaseholder" if servicer.is_leader else "follower"
    print(f"[{addr}] {role}, nodes={nodes}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        pass
    finally:
        servicer.close()
        server.stop(0)


if __name__ == "__main__":
    main()
