# Operations and Testing

This document records how to run and validate the public version of the cost-of-living data platform.

## Local Setup

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
cp .env.example .env
```

Run tests:

```bash
make ci
```

Run the API:

```bash
make api
```

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

## Docker Compose

```bash
docker compose up --build
```

Health check:

```bash
curl -s http://127.0.0.1:8000/api/cost-living/health
```

Optional Kibana:

```bash
docker compose --profile tools up kibana
```

Docker Compose also starts Redis for runtime queue diagnostics. Fission jobs use the same Redis settings when `REDIS_ENABLED=true` in the cloud ConfigMaps.

## Elasticsearch Inspection

With local Docker Compose:

```bash
python scripts/inspect_es_indices.py --sample-size 1
```

With a Kubernetes Elasticsearch service, port-forward it and set credentials:

```bash
kubectl -n elastic port-forward svc/elasticsearch-es-http 9201:9200
export ELASTICSEARCH_URL=https://localhost:9201
export ELASTICSEARCH_USERNAME=elastic
export ELASTICSEARCH_PASSWORD=<password>
export ELASTICSEARCH_VERIFY_CERTS=false
python scripts/inspect_es_indices.py --json --sample-size 0
```

## API Validation

Smoke test:

```bash
python scripts/smoke_cost_living_platform_api.py \
  --base-url http://127.0.0.1:8000 \
  --prefix /api/cost-living \
  --timeout 120
```

Bounded stress test:

```bash
python scripts/stress_cost_living_platform_api.py \
  --base-url http://127.0.0.1:8000 \
  --prefix /api/cost-living \
  --rounds 10 \
  --workers 3 \
  --timeout 120
```

## Kubernetes API

Build and push the API image:

```bash
docker build -f deployment/docker/api.Dockerfile \
  -t ghcr.io/your-username/cost-living-platform-api:latest .
docker push ghcr.io/your-username/cost-living-platform-api:latest
```

Update `deployment/kubernetes/api-deployment.yaml` with your image, then apply:

```bash
kubectl apply -f deployment/kubernetes/namespace.yaml
kubectl apply -f deployment/kubernetes/secrets.example.yaml
kubectl apply -f deployment/kubernetes/configmap.yaml
kubectl apply -f deployment/kubernetes/api-deployment.yaml
kubectl apply -f deployment/kubernetes/api-service.yaml
```

Port-forward:

```bash
kubectl -n cost-living port-forward svc/cost-living-platform-api 8010:80
```

Check:

```bash
curl -s http://127.0.0.1:8010/api/cost-living/health
curl -s http://127.0.0.1:8010/api/cost-living/pipeline/runtime
curl -s http://127.0.0.1:8010/api/cost-living/pipeline/events?limit=20
```

Use `api-hpa.yaml` only after metrics-server is available:

```bash
kubectl apply -f deployment/kubernetes/api-hpa.yaml
```

## Fission Pipeline

Fission is used for scheduled ingestion and processing jobs only.

Deployment order:

1. Create real secrets from `deployment/fission/platform-secrets.example.yaml`.
2. Apply `deployment/fission/platform-environment.yaml`.
3. Build the source package.
4. Create or update the Fission pipeline package.
5. Apply `platform-configmap.yaml`.
6. Apply `platform-pipeline-functions.yaml`.
7. Manually test functions.
8. Apply `platform-pipeline-timers.yaml`.

Detailed commands are in [deployment/fission/package_commands.md](../../deployment/fission/package_commands.md).

## Optional Redis Runtime Queue

Deploy Redis before enabling the runtime queue:

```bash
kubectl apply -f deployment/redis/redis.yaml
```

Set `REDIS_ENABLED=true` in both API and Fission ConfigMaps. Redis is used for scheduled job locks and lifecycle events; Elasticsearch still stores document state.

## GDELT Archive Ingestion

The production GDELT path uses the public GKG archive list:

```text
http://data.gdeltproject.org/gdeltv2/masterfilelist.txt
```

The incremental Fission function and historical backfill both use `backend/harvesters/gdelt_archive.py`. The processor downloads `.gkg.csv.zip` files, verifies md5 checksums, extracts CSV rows, filters for Australian cost-of-living records and writes to `cost_living_gdelt_raw_stream` with an explicit mapping.

Backfill dry run:

```bash
python -m backend.harvesters.gdelt_backfill \
  --start-date 2026-05-01 \
  --end-date 2026-05-02 \
  --max-archives 4 \
  --dry-run
```

Backfill execution:

```bash
python -m backend.harvesters.gdelt_backfill \
  --start-date 2026-05-01 \
  --end-date 2026-05-02 \
  --max-archives 4
```

The checkpoint path is controlled by `GDELT_GKG_BACKFILL_CHECKPOINT_PATH`. Re-running the same command skips completed archives unless `--no-resume` is used.

## Source Plugins

Source metadata is kept in:

```text
backend/platforms/plugins.py
```

`backend/common/source_registry.py` exposes the plugin catalog to the API, integrator and NLP worker.

Checklist for adding a source:

1. Add or update the harvester.
2. Write raw records into a source-specific raw stream index.
3. Register the source in `backend/platforms/plugins.py`.
4. Add normalisation logic in `scripts/import_raw_streams.py` if needed.
5. Add deployment manifests.
6. Run the raw integrator, NLP worker, API smoke test and notebook validation.

## Notebooks

Use these environment variables before opening the notebooks:

```bash
export API_BASE_URL=http://127.0.0.1:8000
export API_PREFIX=/api/cost-living
export API_TIMEOUT_SECONDS=120
export API_PREFLIGHT_TIMEOUT_SECONDS=60
```

For a Kubernetes port-forward, use `API_BASE_URL=http://127.0.0.1:8010`.

## Troubleshooting

| Symptom | Likely cause | Check |
| --- | --- | --- |
| API returns empty rows | Elasticsearch has no processed data | `/pipeline/status`, index inspection |
| API returns 422 | Invalid query parameter | Response `detail` field |
| API returns 500 | Elasticsearch query or mapping error | API logs and `/health` |
| No new raw documents | Upstream API limit or no matching records | Harvester logs and raw stream indices |
| NLP backlog grows | Worker not running or stale processing locks | Raw index `analysis_status` distribution |

## Validation Snapshot

The repository-level test suite currently covers harvesters, GDELT archive processing and backfill resume behaviour, NLP processing, analytics queries, source plugins, Redis runtime queue logic and API route wiring.

```text
53 pytest tests passing
```
