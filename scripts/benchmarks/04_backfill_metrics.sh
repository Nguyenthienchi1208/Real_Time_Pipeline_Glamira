#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

PYTHONPATH=src python - <<'PY'
from main import build_run_config

cfg = build_run_config(backfill_days=3, dry_run=True)
print(f"metric=backfill_days value={cfg['backfill_days']}")
print(f"metric=dry_run value={str(cfg['dry_run']).lower()}")
print(f"metric=checkpoint_path value={cfg['checkpoint_path']}")
print(f"metric=batch_id value={cfg['batch_id']}")

assert cfg["backfill_days"] == 3
assert cfg["dry_run"] is True
assert cfg["checkpoint_path"].endswith("backfill")
assert cfg["batch_id"].startswith("backfill-")
PY
