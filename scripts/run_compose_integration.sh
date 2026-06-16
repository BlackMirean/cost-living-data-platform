#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
project="${COMPOSE_PROJECT_NAME:-cost_living_integration}"
python_bin="${PYTHON:-python}"
api_prefix="${API_PREFIX:-/api/cost-living}"
api_host_port="${API_HOST_PORT:-18000}"
elasticsearch_host_port="${ELASTICSEARCH_HOST_PORT:-19200}"
redis_host_port="${REDIS_HOST_PORT:-16379}"
api_base_url="${API_BASE_URL:-http://127.0.0.1:${api_host_port}}"

cd "$repo_root"
mkdir -p build

cleanup() {
  docker compose -p "$project" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

cleanup

export API_HOST_PORT="$api_host_port"
export ELASTICSEARCH_HOST_PORT="$elasticsearch_host_port"
export REDIS_HOST_PORT="$redis_host_port"

docker compose -p "$project" up -d --build redis elasticsearch api

export ELASTICSEARCH_URL="http://127.0.0.1:${elasticsearch_host_port}"
export ELASTICSEARCH_USERNAME=
export ELASTICSEARCH_PASSWORD=
export ELASTICSEARCH_VERIFY_CERTS=false
export REDIS_ENABLED=true
export REDIS_URL="redis://127.0.0.1:${redis_host_port}/0"
export API_RATE_LIMIT_ENABLED=true
export RAW_POSTS_INDEX=cost_living_raw_posts
export POSTS_INDEX=cost_living_posts_current
export PROCESSED_POSTS_WRITE_INDEX=cost_living_processed_posts_write
export POSTS_CURRENT_ALIAS=cost_living_posts_current
export INDICATORS_INDEX=cost_living_indicators
export MONTHLY_METRICS_INDEX=cost_living_monthly_topic_metrics

"$python_bin" scripts/wait_for_elasticsearch.py
"$python_bin" scripts/seed_integration_data.py --reset
"$python_bin" scripts/requeue_pending_nlp.py --reason compose_integration

queue_depth="$(docker compose -p "$project" exec -T redis redis-cli llen cost_living_pipeline:queue:nlp)"
if [ "${queue_depth:-0}" -lt 1 ]; then
  echo "Expected at least one Redis NLP queue message, got $queue_depth" >&2
  exit 1
fi

worker_started="$(date +%s)"
worker_output="$(docker compose -p "$project" --profile workers run --rm nlp-worker \
  python -m backend.processing.queue_worker --once)"
worker_elapsed="$(( $(date +%s) - worker_started ))"
echo "$worker_output"
WORKER_OUTPUT="$worker_output" WORKER_ELAPSED="$worker_elapsed" "$python_bin" - <<'PY'
import json
import os
import sys
from pathlib import Path

lines = [line for line in os.environ["WORKER_OUTPUT"].splitlines() if line.strip()]
payload = json.loads(lines[-1])
payload["elapsed_seconds"] = int(os.environ["WORKER_ELAPSED"])
Path("build/compose-integration-worker.json").write_text(
    json.dumps(payload, indent=2, sort_keys=True),
    encoding="utf-8",
)
if int(payload.get("processed_messages") or 0) < 1:
    print("Expected the NLP worker to process at least one queue message.", file=sys.stderr)
    sys.exit(1)
PY

"$python_bin" - <<'PY'
from pathlib import Path
import sys

sys.path.insert(0, str(Path.cwd()))
from backend.common.config import settings
from backend.common.es_client import get_es_client

client = get_es_client()
count = client.count(index=settings.posts_current_alias, request_timeout=60).get("count", 0)
print({"processed_documents": int(count), "read_alias": settings.posts_current_alias})
if int(count) < 1:
    raise SystemExit("Expected at least one processed document after worker drain.")
PY

for _ in $(seq 1 60); do
  if curl -fsS "$api_base_url$api_prefix/health" >/dev/null; then
    break
  fi
  sleep 2
done

"$python_bin" scripts/smoke_cost_living_platform_api.py \
  --base-url "$api_base_url" \
  --prefix "$api_prefix" \
  --timeout 120

"$python_bin" scripts/openapi_contract_check.py \
  --base-url "$api_base_url" \
  --prefix "$api_prefix" \
  --timeout 120

"$python_bin" scripts/stress_cost_living_platform_api.py \
  --base-url "$api_base_url" \
  --prefix "$api_prefix" \
  --rounds 2 \
  --workers 2 \
  --timeout 120 \
  --output-json build/compose-integration-stress.json
