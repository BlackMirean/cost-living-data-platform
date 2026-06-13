.PHONY: install wait api test inspect-raw import-stream-dry rebuild-unified-raw sync-recent

install:
	. .venv/bin/activate && python -m pip install -r requirements.txt

wait:
	. .venv/bin/activate && python scripts/wait_for_elasticsearch.py

api:
	. .venv/bin/activate && uvicorn backend.api.main:app --reload --host 0.0.0.0 --port 8000

test:
	. .venv/bin/activate && pytest

inspect-raw:
	. .venv/bin/activate && . scripts/load_cloud_env.sh && python scripts/inspect_es_indices.py --indices cost_living_raw_posts --sample-size 3

import-stream-dry:
	. .venv/bin/activate && . scripts/load_cloud_env.sh && python scripts/import_raw_streams.py --limit-per-index 2 --sample-size 2

rebuild-unified-raw:
	. .venv/bin/activate && . scripts/load_cloud_env.sh && python scripts/import_raw_streams.py --write --reset-target --limit-per-index 0 --scan-size 5000 --bulk-size 2500

sync-recent:
	. .venv/bin/activate && . scripts/load_cloud_env.sh && python scripts/import_raw_streams.py --write --lookback-hours 2 --limit-per-index 0 --scan-size 1000 --bulk-size 1000
