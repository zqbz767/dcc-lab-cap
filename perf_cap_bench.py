"""Task 4 - PACELC benchmark: Linearizable vs Follower-Stale reads on a 3-replica localhost cluster.

  python3 perf_cap_bench.py                      # 1000 iterations per scenario
  python3 perf_cap_bench.py --wan-delay-ms 0     # pure localhost numbers

The client sits next to Replica B (a follower). LINEARIZABLE reads are proxied by B to the
leaseholder A (a remote hop); FOLLOWER_STALE reads are answered by B's local state machine.
Because everything runs on one machine, --wan-delay-ms adds a simulated round-trip cost to
every cross-node call (default 20 ms) so the PACELC latency gap is visible. Use 0 for raw numbers.
After the main table, an extra run with the leaseholder partitioned away shows the A-vs-C side.
"""
import argparse
import json
import math
import os
import socket
import statistics
import subprocess
import sys
import time

import grpc

import kvstore_pb2 as pb
from client import KVClient, ReplicaUnavailableError, StaleReadExceeded

ROOT = os.path.dirname(os.path.abspath(__file__))
B = 1


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start_cluster(wan_delay_ms):
    ports = [free_port() for _ in range(3)]
    nodes = [f"127.0.0.1:{p}" for p in ports]
    procs = []
    for p in ports:
        procs.append(subprocess.Popen(
            [sys.executable, os.path.join(ROOT, "server.py"), "--port", str(p),
             "--peers", ",".join(nodes), "--wan-delay-ms", str(wan_delay_ms)],
            cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT))
    for n in nodes:
        ch = grpc.insecure_channel(n)
        try:
            grpc.channel_ready_future(ch).result(timeout=20)
        finally:
            ch.close()
    return procs, nodes


def percentile(sorted_vals, q):
    """nearest-rank percentile"""
    if not sorted_vals:
        return float("nan")
    return sorted_vals[max(0, math.ceil(q * len(sorted_vals)) - 1)]


def run_scenario(client, consistency, iterations, max_staleness_ms=0):
    lat, ok = [], 0
    for _ in range(iterations):
        t0 = time.perf_counter()
        try:
            r = client.get("bench-key", node=B, consistency=consistency,
                           max_staleness_ms=max_staleness_ms)
            good = r.found
        except (ReplicaUnavailableError, StaleReadExceeded):
            good = False
        dt = (time.perf_counter() - t0) * 1000.0
        if good:
            ok += 1
            lat.append(dt)
    lat.sort()
    return {"iterations": iterations, "ok": ok,
            "median_ms": statistics.median(lat) if lat else None,
            "p95_ms": percentile(lat, 0.95) if lat else None,
            "availability_pct": 100.0 * ok / iterations}


def fmt(x):
    return "n/a" if x is None else f"{x:.2f}"


def table(rows):
    out = ["| Read Mode | Target Node | Median Latency (ms) | p95 Latency (ms) | Read Availability (%) |",
           "|---|---|---|---|---|"]
    for name, target, r in rows:
        out.append(f"| {name} | {target} | {fmt(r['median_ms'])} | {fmt(r['p95_ms'])} | "
                   f"{r['availability_pct']:.1f} |")
    return "\n".join(out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--iterations", type=int, default=1000)
    ap.add_argument("--wan-delay-ms", type=int, default=20)
    ap.add_argument("--partition-iterations", type=int, default=200)
    ap.add_argument("--no-partition-demo", action="store_true")
    ap.add_argument("--out", default=os.path.join(ROOT, "logs", "bench_results.json"))
    args = ap.parse_args()

    procs, nodes = start_cluster(args.wan_delay_ms)
    client = KVClient(nodes, timeout=2.0)
    try:
        assert client.put("bench-key", "value", node=0, strategy=pb.QUORUM_CP).success
        for _ in range(100):                       # wait until B has the value locally
            try:
                if client.get("bench-key", node=B, consistency=pb.FOLLOWER_STALE).found:
                    break
            except Exception:
                pass
            time.sleep(0.05)
        for _ in range(50):                        # warm-up (not measured)
            client.get("bench-key", node=B, consistency=pb.LINEARIZABLE)
            client.get("bench-key", node=B, consistency=pb.FOLLOWER_STALE, max_staleness_ms=5000)

        lin = run_scenario(client, pb.LINEARIZABLE, args.iterations)
        fol = run_scenario(client, pb.FOLLOWER_STALE, args.iterations, 5000)
        print(f"\nMain benchmark: {args.iterations} iterations per scenario, "
              f"simulated cross-node delay {args.wan_delay_ms} ms")
        print(table([("Linearizable", "Leaseholder (Remote)", lin),
                     ("Follower Stale", "Local Follower", fol)]))

        results = {"iterations": args.iterations, "wan_delay_ms": args.wan_delay_ms,
                   "linearizable": lin, "follower_stale": fol}

        if not args.no_partition_demo:
            client.partition([[0], [1, 2]])        # leaseholder cut off from B and C
            plin = run_scenario(client, pb.LINEARIZABLE, args.partition_iterations)
            pfol = run_scenario(client, pb.FOLLOWER_STALE, args.partition_iterations, 5000)
            client.heal()
            print("\nExtra: leaseholder partitioned away from the client's follower")
            print(table([("Linearizable", "Leaseholder (unreachable)", plin),
                         ("Follower Stale", "Local Follower", pfol)]))
            results["partition_iterations"] = args.partition_iterations
            results["partition_linearizable"] = plin
            results["partition_follower_stale"] = pfol

        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, "w") as f:
            json.dump(results, f, indent=1)
        print(f"\nsaved {args.out}")
    finally:
        client.close()
        for p in procs:
            p.kill()
            p.wait()


if __name__ == "__main__":
    main()
