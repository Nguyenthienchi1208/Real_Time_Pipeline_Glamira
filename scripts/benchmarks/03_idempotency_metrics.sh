#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

PYTHONPATH=src python - <<'PY'
from pyspark.sql import SparkSession
from utils.postgres_handler import dedupe_event_batch

spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("Glamira-Idempotency-Benchmark")
    .getOrCreate()
)

rows = [
    ("evt_001", "p-1", "store-1", "2024-01-01T00:00:00Z"),
    ("evt_001", "p-1", "store-1", "2024-01-01T00:00:00Z"),
    ("evt_002", "p-2", "store-2", "2024-01-01T01:00:00Z"),
]

df = spark.createDataFrame(rows, ["event_id", "product_id", "store_id", "time_stamp"])
deduped = dedupe_event_batch(df)

unique_count = deduped.select("event_id").distinct().count()
input_count = df.count()
output_count = deduped.count()
removed_duplicates = input_count - output_count

print(f"metric=input_rows value={input_count}")
print(f"metric=output_rows value={output_count}")
print(f"metric=removed_duplicates value={removed_duplicates}")
print(f"metric=unique_event_ids value={unique_count}")
print(f"metric=dedupe_ratio value={removed_duplicates / input_count if input_count else 0.0:.4f}")

assert input_count == 3
assert output_count == 2
assert removed_duplicates == 1
assert unique_count == 2
PY
