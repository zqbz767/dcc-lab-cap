"""CAP / PACELC lab tests. Three replicas run in-process on ephemeral ports.
Partitions are injected with the SetPartition RPC (client.partition / client.heal):
DC-1 = nodes A(0), B(1); DC-2 = node C(2). Node A (index 0) is the leaseholder.
"""
import itertools
import json
import os
import sys
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import kvstore_pb2 as pb  # noqa: E402
from client import KVClient, ReplicaUnavailableError, StaleReadExceeded  # noqa: E402
from server import build_server  # noqa: E402

A, B, C = 0, 1, 2
N = 3


class Cluster:
    def __init__(self, n=N):
        started = [build_server(0, heartbeat_s=0.05, peer_timeout_s=0.3) for _ in range(n)]
        self.servers = [s for s, _, _ in started]
        self.servicers = [sv for _, sv, _ in started]
        self.targets = [f"127.0.0.1:{port}" for _, _, port in started]
        for t, sv in zip(self.targets, self.servicers):
            sv.configure(t, self.targets, leader=self.targets[0])
        self.client = KVClient(self.targets, timeout=2.0)
        self._dead = set()

    def crash(self, i):
        """Simulate a node crash: the process stops answering."""
        self._dead.add(i)
        self.servicers[i].close()
        self.servers[i].stop(0)

    def shutdown(self):
        self.client.close()
        for i, (s, sv) in enumerate(zip(self.servers, self.servicers)):
            if i not in self._dead:
                sv.close()
                s.stop(0)


@pytest.fixture
def cluster():
    c = Cluster()
    yield c
    c.shutdown()


def wait_until(fn, timeout=5.0, step=0.05):
    end = time.time() + timeout
    while time.time() < end:
        if fn():
            return True
        time.sleep(step)
    return fn()


def local(cluster, node, key):
    """Read the node's own state machine (no staleness bound)."""
    return cluster.client.get(key, node=node, consistency=pb.FOLLOWER_STALE, max_staleness_ms=0)


# ---------------------------------------------------------------- Task 3 Part A (CP)
def test_cp_write_succeeds_on_majority_partition(cluster):
    cluster.client.partition([[A, B], [C]])
    r = cluster.client.put("k", "v1", node=A, strategy=pb.QUORUM_CP)
    assert r.success is True
    assert r.acks_received == 2          # A + B, C is cut off


def test_cp_write_rejected_on_minority_partition(cluster):
    cluster.client.partition([[A, B], [C]])
    r = cluster.client.put("k", "lost", node=C, strategy=pb.QUORUM_CP)
    assert r.success is False
    assert "ReplicaUnavailableError" in r.error_message
    assert r.acks_received == 1
    assert local(cluster, C, "k").found is False   # nothing was applied on the minority side


def test_cp_write_survives_one_crash_but_not_two(cluster):
    cluster.crash(C)
    r = cluster.client.put("k", "v", node=A, strategy=pb.QUORUM_CP)
    assert r.success and r.acks_received == 2
    cluster.crash(B)
    r = cluster.client.put("k", "v2", node=A, strategy=pb.QUORUM_CP)
    assert not r.success and "ReplicaUnavailableError" in r.error_message


# ---------------------------------------------------------------- Task 3 Part B (AP)
def test_ap_write_accepted_on_minority_partition(cluster):
    cluster.client.partition([[A, B], [C]])
    r = cluster.client.put("k", "from-dc2", node=C, strategy=pb.ALWAYS_AP)
    assert r.success is True
    assert r.acks_received == 1
    assert local(cluster, C, "k").value == "from-dc2"
    assert local(cluster, A, "k").found is False   # divergence while partitioned


def test_ap_partition_recovery_lww_newer_timestamp_wins(cluster):
    cluster.client.partition([[A, B], [C]])
    cluster.client.put("k", "dc1", node=A, strategy=pb.ALWAYS_AP, timestamp=1000)
    cluster.client.put("k", "dc2", node=C, strategy=pb.ALWAYS_AP, timestamp=2000)
    assert local(cluster, A, "k").value == "dc1" and local(cluster, C, "k").value == "dc2"
    cluster.client.heal()
    assert wait_until(lambda: all(local(cluster, n, "k").value == "dc2" for n in (A, B, C)))
    assert local(cluster, A, "k").read_timestamp == 2000


def test_lww_decides_by_timestamp_not_by_arrival_order(cluster):
    cluster.client.partition([[A, B], [C]])
    cluster.client.put("k", "dc1-newer", node=A, strategy=pb.ALWAYS_AP, timestamp=9000)
    cluster.client.put("k", "dc2-older", node=C, strategy=pb.ALWAYS_AP, timestamp=100)
    cluster.client.heal()
    assert wait_until(lambda: all(local(cluster, n, "k").value == "dc1-newer" for n in (A, B, C)))


def test_cp_write_reaches_isolated_node_after_heal(cluster):
    cluster.client.partition([[A, B], [C]])
    assert cluster.client.put("k", "v", node=A, strategy=pb.QUORUM_CP).success
    assert local(cluster, C, "k").found is False
    cluster.client.heal()
    assert wait_until(lambda: local(cluster, C, "k").found)   # hinted handoff delivers it


# ---------------------------------------------------------------- Task 2 (read paths)
def test_linearizable_read_is_proxied_to_leaseholder(cluster):
    cluster.client.put("k", "v", node=A, strategy=pb.QUORUM_CP)
    r = cluster.client.get("k", node=B, consistency=pb.LINEARIZABLE)
    assert r.found and r.value == "v" and r.is_stale is False


def test_linearizable_read_fails_when_leaseholder_unreachable(cluster):
    cluster.client.put("k", "v", node=A, strategy=pb.QUORUM_CP)
    cluster.client.partition([[A], [B, C]])
    with pytest.raises(ReplicaUnavailableError):
        cluster.client.get("k", node=B, consistency=pb.LINEARIZABLE)


def test_follower_read_stays_available_without_leaseholder(cluster):
    cluster.client.put("k", "v", node=A, strategy=pb.QUORUM_CP)
    cluster.client.partition([[A], [B, C]])
    r = cluster.client.get("k", node=B, consistency=pb.FOLLOWER_STALE, max_staleness_ms=0)
    assert r.found and r.value == "v" and r.is_stale is True


def test_stale_read_exceeded_when_follower_lags(cluster):
    cluster.client.put("k", "v", node=A, strategy=pb.QUORUM_CP)
    ok = cluster.client.get("k", node=B, consistency=pb.FOLLOWER_STALE, max_staleness_ms=1000)
    assert ok.found                                   # fresh: heartbeats keep lag tiny
    cluster.client.partition([[A], [B, C]])
    time.sleep(0.5)
    with pytest.raises(StaleReadExceeded):
        cluster.client.get("k", node=B, consistency=pb.FOLLOWER_STALE, max_staleness_ms=100)
    r = cluster.client.get("k", node=B, consistency=pb.FOLLOWER_STALE, max_staleness_ms=60000)
    assert r.found                                    # looser bound is still satisfiable


# ---------------------------------------------------------------- idempotency
def test_idempotent_put_is_applied_once(cluster):
    r1 = cluster.client.put("k", "first", node=A, idempotency_key="op-1")
    r2 = cluster.client.put("k", "second", node=A, idempotency_key="op-1")
    assert r1.success and r2.success
    assert cluster.client.get("k", node=A).value == "first"


# ---------------------------------------------------------------- quorum arithmetic
def strong_by_formula(r, w, n):
    return r + w > n


def test_majority_is_n_div_2_plus_1(cluster):
    assert all(sv.majority == 2 for sv in cluster.servicers)


def record_quorum(r, w, n, formula, always_fresh, stale_example):
    os.makedirs(os.path.join(ROOT, "logs"), exist_ok=True)
    path = os.path.join(ROOT, "logs", "quorum_results.json")
    try:
        with open(path) as f:
            rows = json.load(f)
    except (OSError, ValueError):
        rows = {}
    rows[f"{r},{w}"] = {"R": r, "W": w, "N": n, "r_plus_w_gt_n": formula,
                        "all_reads_fresh": always_fresh, "stale_read_example": stale_example}
    with open(path, "w") as f:
        json.dump(rows, f, indent=1)


@pytest.mark.parametrize("w", [1, 2, 3])
@pytest.mark.parametrize("r", [1, 2, 3])
def test_quorum_intersection_experiment(cluster, r, w):
    """Every N=3 combination of (R, W). Old value "old"(ts=1) is on all nodes, then the cluster
    is fully partitioned and "new"(ts=2) is written to W nodes only. Reading from EVERY
    possible set of R nodes and taking the newest timestamp must always return "new" iff R+W>N."""
    c = cluster.client
    assert c.put("k", "old", node=A, strategy=pb.QUORUM_CP, timestamp=1).acks_received == 3
    c.partition([[A], [B], [C]])
    write_nodes = list(range(w))
    c.write_to(write_nodes, "k", "new", timestamp=2)

    stale_sets = []
    for read_nodes in itertools.combinations(range(N), r):
        value, _ = c.quorum_read(list(read_nodes), "k")
        if value != "new":
            stale_sets.append(read_nodes)

    always_fresh = not stale_sets
    record_quorum(r, w, N, strong_by_formula(r, w, N), always_fresh,
                  ("R-nodes " + str([("A", "B", "C")[i] for i in stale_sets[0]])) if stale_sets else "-")
    assert always_fresh == strong_by_formula(r, w, N)
