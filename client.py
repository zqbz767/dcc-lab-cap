"""Client for the CAP-lab key-value store.

CLI examples (3 nodes running, see server.py):
  python3 client.py put user:1 alice --node 0 --strategy cp
  python3 client.py get user:1 --node 1 --consistency follower --max-staleness-ms 5000
"""
import argparse
import sys
import uuid

import grpc

import kvstore_pb2 as pb
import kvstore_pb2_grpc as pb_grpc

DEFAULT_NODES = "127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053"


class ReplicaUnavailableError(Exception):
    """The addressed node (or the leaseholder it proxies to) could not be reached."""


class StaleReadExceeded(Exception):
    """FOLLOWER_STALE read rejected: replica lag > max_staleness_ms."""


class KVClient:
    def __init__(self, targets, timeout=2.0):
        self.targets = list(targets)
        self.timeout = timeout
        self._channels = [grpc.insecure_channel(t) for t in self.targets]
        self._stubs = [pb_grpc.KeyValueStoreStub(c) for c in self._channels]

    # ---- basic operations ----------------------------------------------
    def put(self, key, value, node=0, strategy=pb.QUORUM_CP, timestamp=0, idempotency_key=None):
        """Returns PutReply. A failed CP write is reported as success=False
        (error_message starts with ReplicaUnavailableError). Raises
        ReplicaUnavailableError only if the node itself is unreachable."""
        req = pb.PutRequest(key=key, value=value, strategy=strategy, timestamp=timestamp,
                            idempotency_key=idempotency_key or str(uuid.uuid4()))
        try:
            return self._stubs[node].Put(req, timeout=self.timeout)
        except grpc.RpcError as err:
            raise ReplicaUnavailableError(f"{self.targets[node]}: {err.code().name}") from err

    def get(self, key, node=0, consistency=pb.LINEARIZABLE, max_staleness_ms=0):
        req = pb.GetRequest(key=key, consistency=consistency, max_staleness_ms=max_staleness_ms)
        try:
            return self._stubs[node].Get(req, timeout=self.timeout)
        except grpc.RpcError as err:
            if err.code() == grpc.StatusCode.FAILED_PRECONDITION and \
                    "STALE_READ_EXCEEDED" in (err.details() or ""):
                raise StaleReadExceeded(err.details()) from err
            raise ReplicaUnavailableError(f"{self.targets[node]}: {err.details()}") from err

    # ---- failure injection ---------------------------------------------
    def set_blocked(self, node, blocked_nodes):
        self._stubs[node].SetPartition(
            pb.PartitionRequest(blocked=[self.targets[i] for i in blocked_nodes]),
            timeout=self.timeout)

    def partition(self, groups):
        """groups = [[0, 1], [2]] -> nodes can only talk inside their own group."""
        for group in groups:
            others = [i for g in groups if g is not group for i in g]
            for i in group:
                self.set_blocked(i, others)

    def heal(self):
        for i in range(len(self.targets)):
            self.set_blocked(i, [])

    # ---- quorum helpers (used by the R + W > N experiment) ---------------
    def write_to(self, nodes, key, value, timestamp):
        """Write to exactly these nodes (AP, no help from replication when the cluster is
        fully partitioned). Used to emulate a write that was acknowledged by W replicas."""
        for n in nodes:
            self.put(key, value, node=n, strategy=pb.ALWAYS_AP, timestamp=timestamp)

    def quorum_read(self, nodes, key):
        """Read from R replicas (local state, unbounded staleness), return the newest
        (value, timestamp) by timestamp - what a Dynamo/Cassandra style coordinator does."""
        best = (None, 0)
        for n in nodes:
            r = self.get(key, node=n, consistency=pb.FOLLOWER_STALE, max_staleness_ms=0)
            if r.found and r.read_timestamp > best[1]:
                best = (r.value, r.read_timestamp)
        return best

    def close(self):
        for c in self._channels:
            c.close()


def main():
    p = argparse.ArgumentParser(description="KV store client")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("put", "get"):
        s = sub.add_parser(name)
        s.add_argument("key")
        s.add_argument("--nodes", default=DEFAULT_NODES)
        s.add_argument("--node", type=int, default=0, help="index of the node to talk to")
        if name == "put":
            s.add_argument("value")
            s.add_argument("--strategy", choices=("cp", "ap"), default="cp")
        else:
            s.add_argument("--consistency", choices=("linearizable", "follower"),
                           default="linearizable")
            s.add_argument("--max-staleness-ms", type=int, default=0)
    a = p.parse_args()
    c = KVClient(a.nodes.split(","))
    try:
        if a.cmd == "put":
            r = c.put(a.key, a.value, a.node, pb.QUORUM_CP if a.strategy == "cp" else pb.ALWAYS_AP)
            print(f"success={r.success} acks={r.acks_received} {r.error_message}")
            sys.exit(0 if r.success else 1)
        cons = pb.LINEARIZABLE if a.consistency == "linearizable" else pb.FOLLOWER_STALE
        r = c.get(a.key, a.node, cons, a.max_staleness_ms)
        print(f"value={r.value!r} ts={r.read_timestamp} is_stale={r.is_stale}" if r.found
              else "not found")
    except (ReplicaUnavailableError, StaleReadExceeded) as e:
        print(f"ERROR {type(e).__name__}: {e}")
        sys.exit(1)
    finally:
        c.close()


if __name__ == "__main__":
    main()
