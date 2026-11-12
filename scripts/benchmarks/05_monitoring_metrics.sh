#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

PYTHONPATH=src python - <<'PY'
from utils.postgres_handler import build_monitoring_summary

summary = build_monitoring_summary(
    batch_id="batch-006",
    rows_in=120,
    rows_out=112,
    started_at="2026-11-12T00:00:00",
    finished_at="2026-11-12T00:02:00",
    last_event_at="2026-11-12T00:03:00",
    now_ts="2026-11-12T00:05:00",
)

print(f"metric=batch_id value={summary['batch_id']}")
print(f"metric=rows_in value={summary['rows_in']}")
print(f"metric=rows_out value={summary['rows_out']}")
print(f"metric=duration_seconds value={summary['duration_seconds']}")
print(f"metric=freshness_lag_minutes value={summary['freshness_lag_minutes']}")
print(f"metric=sla_ok value={str(summary['sla_ok']).lower()}")

assert summary["batch_id"] == "batch-006"
assert summary["rows_in"] == 120
assert summary["rows_out"] == 112
assert summary["duration_seconds"] == 120
assert summary["freshness_lag_minutes"] == 2
assert summary["sla_ok"] is True
PY
