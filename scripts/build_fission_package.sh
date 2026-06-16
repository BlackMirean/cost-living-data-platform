#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
output="${1:-$repo_root/build/cost-living-platform.zip}"

mkdir -p "$(dirname "$output")"
rm -f "$output"

cd "$repo_root"
zip -qr "$output" \
  backend \
  scripts \
  database \
  requirements.txt \
  requirements-fission.txt \
  build.sh \
  -x "*/__pycache__/*" \
  -x "*.pyc" \
  -x ".pytest_cache/*" \
  -x "docs/generated/*" \
  -x "data/backfill_state/*"

echo "$output"
