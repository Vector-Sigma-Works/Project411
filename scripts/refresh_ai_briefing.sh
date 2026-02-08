#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# Only run through upcoming Sunday (America/New_York).
TZNAME="America/New_York"
TODAY=$(TZ="$TZNAME" date +%F)
END=$(TZ="$TZNAME" date -d 'next sunday' +%F)

if [[ "$TODAY" > "$END" ]]; then
  exit 0
fi

node scripts/run_ai_briefing.js

# Restart UI so it serves latest run.json
systemctl --user restart project411-ai-mvs.service

# Log counts
LATEST=$(ls -1 runs/ai | grep -E '^\d{4}-\d{2}-\d{2}$' | sort | tail -n 1)
RUN_JSON="runs/ai/${LATEST}/run.json"
COUNTS=$(jq -c '{ts:now|todateiso8601, briefing_count, queue_count, candidate_count, eligible_count}' "$RUN_JSON")
mkdir -p runs/ai
printf '%s\n' "$COUNTS" >> runs/ai/refresh.log
