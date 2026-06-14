# Operations and Testing

Runbook for operating and validating the public cost-of-living data platform.

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

Docker Compose also starts Redis for runtime diagnostics and API caching. Fission jobs use the same Redis settings when `REDIS_ENABLED=true` in the cloud ConfigMaps.

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

Fission is used for scheduled ingestion, raw integration and CPI jobs only. NLP processing is queue-based and runs in Kubernetes workers.

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

## Optional Redis Runtime Services

Deploy Redis before enabling runtime coordination:

```bash
kubectl apply -f deployment/redis/redis.yaml
```

The provided Kubernetes and Fission ConfigMaps enable Redis. Redis is used for scheduled job locks, lifecycle events, shared API response caching and the NLP work queue; Elasticsearch still stores document state.

## KEDA NLP Worker

The raw integrator pushes work items to `cost_living_pipeline:queue:nlp` after it writes unified raw documents. KEDA watches this Redis list and scales `cost-living-platform-nlp-worker` from zero. Failed messages retry up to `NLP_QUEUE_MAX_ATTEMPTS`; exhausted or malformed messages move to `cost_living_pipeline:queue:nlp:dead-letter`.

```bash
kubectl apply -f deployment/kubernetes/nlp-worker-deployment.yaml
kubectl -n cost-living get scaledobject cost-living-platform-nlp-worker
kubectl -n cost-living get pods -l app=cost-living-platform-nlp-worker
```

Operational checks:

```text
/api/cost-living/pipeline/queues
/api/cost-living/pipeline/events?limit=20
```

The queue status response includes active queue depth and dead-letter depth. A non-zero dead-letter depth should be investigated before treating a run as complete.

## Elasticsearch Lifecycle

Processed documents use an ILM-managed write alias:

```text
cost_living_processed_posts_write -> cost_living_processed_posts-000001
cost_living_posts_current -> processed backing indices
```

Apply or repair the lifecycle policy, template and aliases:

```bash
python scripts/apply_elasticsearch_lifecycle.py
```

The script migrates an existing concrete `cost_living_processed_posts` index into the first backing index before creating aliases.

## Observability and Limits

The API exposes Prometheus metrics, request-id propagation, rate limit status and cache status:

```text
/api/cost-living/metrics
/api/cost-living/rate-limit/status
/api/cost-living/cache/status
```

Every response includes `X-Request-ID`. Clients can provide their own `X-Request-ID`; otherwise the API generates one and includes it in structured request logs.

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
| NLP backlog grows | KEDA worker not running or stale processing locks | `/pipeline/queues`, worker pods, raw index `analysis_status` distribution |

## Validation Snapshot

The repository-level test suite currently covers harvesters, GDELT archive processing and backfill resume behaviour, NLP processing, analytics queries, source plugins, Redis runtime queue logic, API cache behaviour, rate limiting, work queues, Elasticsearch lifecycle templates and API route wiring.

```text
57 pytest tests passing
```
