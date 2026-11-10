#!/usr/bin/env bash
# stubs -> tests -> benchmark -> Lab_Report_CAP.pdf
set -euo pipefail
cd "$(dirname "$0")"
python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. kvstore.proto
mkdir -p logs
rm -f logs/quorum_results.json
python3 -m pytest tests -v --junitxml=logs/junit.xml
python3 perf_cap_bench.py
python3 make_report.py --author "${AUTHOR:-Student Name}"
echo "Done: Lab_Report_CAP.pdf"
