#!/usr/bin/env bash
set -u

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

TARGETS=(src/config/settings.py dashboard/utils_db.py run_spark.sh docker-compose.yml conf/kafka_jaas.conf)
BASELINE_COMMIT="$(git --no-pager rev-list --max-parents=0 HEAD)"
PREVIOUS_COMMIT="$(git --no-pager rev-list --max-count=2 HEAD -- "${TARGETS[@]}" .env.example | tail -n 1 || true)"
if [[ -z "$PREVIOUS_COMMIT" ]]; then
  PREVIOUS_COMMIT="$BASELINE_COMMIT"
fi

count_matches() {
  local rev="$1"
  local file="$2"
  if ! git cat-file -e "$rev:$file" 2>/dev/null; then
    echo 0
    return 0
  fi

  git show "$rev:$file" 2>/dev/null | python3 - "$rev" "$file" <<'PY'
import re
import sys

content = sys.stdin.read()
pattern = re.compile(
    r'''(?ix)
    (?:(?:POSTGRES|KAFKA)_(?:PASSWORD|PWD)|password)
    \s*[:=]\s*
    (?:"|')?
    (?!\$\{)(?!\$\()  # ignore env placeholders like ${VAR:-}
    (?P<value>[A-Za-z0-9!@#$%^&*._:-]{8,})
    (?:"|')?
    '''
)
count = 0
for line in content.splitlines():
    if pattern.search(line):
        count += 1
print(count)
PY
}

printf 'BASELINE_COMMIT=%s\n' "$PREVIOUS_COMMIT"
printf 'HEAD_COMMIT=%s\n\n' "$(git rev-parse --short HEAD)"

printf '%s\n' '--- BEFORE (pre-env-var baseline) ---'
for f in "${TARGETS[@]}"; do
  count=$(count_matches "$PREVIOUS_COMMIT" "$f")
  printf '%s: %s\n' "$f" "$count"
done

printf '%s\n' ''
printf '%s\n' '--- AFTER (HEAD) ---'
for f in "${TARGETS[@]}"; do
  count=$(count_matches HEAD "$f")
  printf '%s: %s\n' "$f" "$count"
done

printf '%s\n' ''
printf '%s\n' '--- .env.example ---'
printf '%s: placeholder file allowed\n' '.env.example'
