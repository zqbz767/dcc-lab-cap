# CSS4004 Lab: Consistency Models & the CAP Theorem

Distributed key-value store (Python + gRPC) with 3 replicas, configurable read consistency
(LINEARIZABLE / FOLLOWER_STALE), write strategies (QUORUM_CP / ALWAYS_AP), network partition
simulation and a PACELC latency benchmark.

## Run

```bash
pip install -r requirements.txt
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. kvstore.proto
python -m pytest tests -v
python perf_cap_bench.py
python make_report.py --author "Your Name"
```

On Windows use `py` instead of `python` if needed.

## Files

- `kvstore.proto` - service and message definitions
- `server.py`, `client.py` - replica and client
- `tests/test_cap.py` - 22 pytest cases (CP/AP partitions, read paths, R + W > N experiment)
- `perf_cap_bench.py` - benchmark, 1000 iterations per scenario
- `Lab_Report_CAP.pdf` - lab report
