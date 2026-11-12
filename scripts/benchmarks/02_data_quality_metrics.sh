#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

PYTHONPATH=src python - <<'PY'
from pyspark.sql import SparkSession
from read_stream import validate_event_payloads

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Glamira-DQ-Benchmark")
    .getOrCreate()
)

raw_rows = [
    (
        b"k1",
        b'{"_id":"evt_001","product_id":"p-100","store_id":"s-10","time_stamp":1700000000,"user_id_db":"u-1"}',
    ),
    (
        b"k2",
        b'{"_id":"evt_002","product_id":"","store_id":"s-10","time_stamp":"bad-ts"}',
    ),
]

raw_df = spark.createDataFrame(raw_rows, ["key", "value"])
_, _, summary = validate_event_payloads(raw_df, batch_id="bench-dq-01")

print("metric=rows_in value={}".format(summary["rows_in"]))
print("metric=rows_valid value={}".format(summary["rows_valid"]))
print("metric=rows_rejected value={}".format(summary["rows_rejected"]))
print("metric=pass_rate value={:.4f}".format(summary["pass_rate"]))

assert summary["rows_in"] == 2
assert summary["rows_valid"] == 1
assert summary["rows_rejected"] == 1
assert summary["pass_rate"] == 0.5
PY
