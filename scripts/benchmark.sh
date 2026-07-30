#!/usr/bin/env bash
set -euo pipefail

connections="${CONNECTIONS:-1000}"
duration="${DURATION:-30}"
warmup="${WARMUP:-10}"
drain="${DRAIN:-15}"
rate="${MESSAGES_PER_SECOND:-10}"
result_dir="$(cd "$(dirname "$0")/.." && pwd)/benchmark-results"
mkdir -p "$result_dir"
output="$result_dir/$(date +%Y%m%d-%H%M%S).txt"

docker compose up -d --build redis chat1 chat2 chat3 chat4 proxy
for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:8080/health >/dev/null; then break; fi
  sleep 1
done

docker compose build bench
docker compose run --rm bench \
  --url ws://proxy:8080/ws \
  --connections "$connections" \
  --duration "$duration" \
  --warmup "$warmup" \
  --drain "$drain" \
  --messages-per-second "$rate" | tee "$output"
docker compose stats --no-stream
echo "Result saved to $output"
