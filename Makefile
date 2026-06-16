PYTHON ?= $(shell if [ -x .venv/bin/python ]; then echo .venv/bin/python; else echo python; fi)
API_HOST ?= 0.0.0.0
API_PORT ?= 8000
API_BASE_URL ?= http://127.0.0.1:8000
API_PREFIX ?= /api/cost-living

GDELT_GKG_BACKFILL_START ?= 2026-05-01
GDELT_GKG_BACKFILL_END ?= 2026-05-02
GDELT_GKG_BACKFILL_MAX_ARCHIVES ?= 4

.PHONY: install wait api test public-check ci smoke stress contract integration gdelt-backfill-dry-run gdelt-backfill inspect-raw import-stream-dry rebuild-unified-raw sync-recent apply-ilm requeue-nlp cloud-drift fission-package cloud-deploy

install:
	$(PYTHON) -m pip install -r requirements.txt

wait:
	$(PYTHON) scripts/wait_for_elasticsearch.py

api:
	$(PYTHON) -m uvicorn backend.api.main:app --reload --host $(API_HOST) --port $(API_PORT)

test:
	$(PYTHON) -m pytest -q

public-check:
	$(PYTHON) scripts/verify_public_release.py

ci: test public-check

smoke:
	$(PYTHON) scripts/smoke_cost_living_platform_api.py --base-url $(API_BASE_URL) --prefix $(API_PREFIX)

stress:
	$(PYTHON) scripts/stress_cost_living_platform_api.py --base-url $(API_BASE_URL) --prefix $(API_PREFIX) --rounds 10 --workers 3

contract:
	$(PYTHON) scripts/openapi_contract_check.py --base-url $(API_BASE_URL) --prefix $(API_PREFIX)

integration:
	PYTHON=$(PYTHON) scripts/run_compose_integration.sh

gdelt-backfill-dry-run:
	$(PYTHON) -m backend.harvesters.gdelt_backfill --start-date $(GDELT_GKG_BACKFILL_START) --end-date $(GDELT_GKG_BACKFILL_END) --max-archives $(GDELT_GKG_BACKFILL_MAX_ARCHIVES) --dry-run

gdelt-backfill:
	$(PYTHON) -m backend.harvesters.gdelt_backfill --start-date $(GDELT_GKG_BACKFILL_START) --end-date $(GDELT_GKG_BACKFILL_END) --max-archives $(GDELT_GKG_BACKFILL_MAX_ARCHIVES)

inspect-raw:
	. scripts/load_cloud_env.sh && $(PYTHON) scripts/inspect_es_indices.py --indices cost_living_raw_posts --sample-size 3

import-stream-dry:
	. scripts/load_cloud_env.sh && $(PYTHON) scripts/import_raw_streams.py --limit-per-index 2 --sample-size 2

rebuild-unified-raw:
	. scripts/load_cloud_env.sh && $(PYTHON) scripts/import_raw_streams.py --write --reset-target --limit-per-index 0 --scan-size 5000 --bulk-size 2500

sync-recent:
	. scripts/load_cloud_env.sh && $(PYTHON) scripts/import_raw_streams.py --write --lookback-hours 2 --limit-per-index 0 --scan-size 1000 --bulk-size 1000

apply-ilm:
	. scripts/load_cloud_env.sh && $(PYTHON) scripts/apply_elasticsearch_lifecycle.py

requeue-nlp:
	. scripts/load_cloud_env.sh && $(PYTHON) scripts/requeue_pending_nlp.py

cloud-drift:
	. scripts/load_cloud_env.sh >/dev/null && $(PYTHON) scripts/check_cloud_drift.py

fission-package:
	scripts/build_fission_package.sh

cloud-deploy:
	. scripts/load_cloud_env.sh >/dev/null && PYTHON=$(PYTHON) scripts/deploy_cloud_runtime.sh
