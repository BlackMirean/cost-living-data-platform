# Fission Pipeline Deployment

This directory contains Fission manifests for scheduled ingestion and processing jobs.

The public architecture uses Fission for the data pipeline only. The analytics API is served by FastAPI through Docker or Kubernetes.

## Manifests

| File | Purpose |
| --- | --- |
| `platform-environment.yaml` | Dedicated Python environment |
| `platform-configmap.yaml` | Pipeline configuration and index names |
| `platform-secrets.example.yaml` | Secret template without real credentials |
| `platform-pipeline-functions.yaml` | Harvesters, raw integrator, NLP processor and CPI harvester |
| `platform-pipeline-timers.yaml` | Scheduled triggers |

## Functions

| Function | Trigger | Purpose |
| --- | --- | --- |
| `cost-living-platform-bluesky-harvester` | timer | Harvest Bluesky raw records |
| `cost-living-platform-mastodon-au-harvester` | timer | Harvest Mastodon AU records |
| `cost-living-platform-mastodon-social-harvester` | timer | Harvest Mastodon social records |
| `cost-living-platform-aus-social-harvester` | timer | Harvest Aus Social records |
| `cost-living-platform-gdelt-harvester` | timer | Harvest incremental GDELT GKG archive records |
| `cost-living-platform-raw-integrator` | timer | Integrate platform raw streams |
| `cost-living-platform-nlp-processor` | timer | Process pending raw documents |
| `cost-living-platform-official-indicators` | timer | Harvest ABS CPI observations |

## Indices

```text
cost_living_bluesky_raw_stream
cost_living_mastodon_raw_stream
cost_living_gdelt_raw_stream
cost_living_raw_posts
cost_living_processed_posts
cost_living_posts_current
cost_living_indicators
cost_living_monthly_topic_metrics
```

## Deployment Order

1. Create a real `cost-living-platform-secrets` Secret from `platform-secrets.example.yaml`.
2. Apply `platform-environment.yaml`.
3. Build the source package.
4. Create or update `cost-living-platform-pipeline-pkg`.
5. Apply `platform-configmap.yaml`.
6. Apply `platform-pipeline-functions.yaml`.
7. Test functions manually.
8. Apply `platform-pipeline-timers.yaml`.

Detailed commands are in [package_commands.md](package_commands.md).

## Optional Redis Runtime Queue

The Fission handlers use Redis when `REDIS_ENABLED=true`. Redis provides job locks and recent lifecycle events; it does not replace Elasticsearch document storage or NLP processing state.

Deploy Redis first:

```bash
kubectl apply -f deployment/redis/redis.yaml
```

Then set:

```yaml
REDIS_ENABLED: "true"
REDIS_URL: "redis://redis.redis.svc.cluster.local:6379/0"
```

When Redis is disabled or unreachable, jobs fail open and continue without distributed locks.

## GDELT Archive Pipeline

The GDELT function uses `backend/harvesters/streams/gdelt_gkg.py` for incremental scheduling and `backend/harvesters/gdelt_archive.py` for archive processing. Historical backfill uses `backend/harvesters/gdelt_backfill.py` and the same archive processor.

Main settings:

```yaml
GDELT_GKG_MASTERFILELIST_URL: "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
GDELT_GKG_INCREMENTAL_BATCH_SIZE: "2"
GDELT_GKG_INCREMENTAL_MAX_RUNTIME_SECONDS: "180"
GDELT_GKG_BACKFILL_CHECKPOINT_PATH: "data/backfill_state/gdelt_gkg_backfill.json"
```

## Notes

- Do not commit real credentials.
- Keep API serving outside Fission to avoid duplicate API deployments.
- Keep timer schedules staggered so harvesters, integration and NLP processing do not all start at once.
- Source plugins live in `backend/platforms/plugins.py` and are exposed through `backend/common/source_registry.py`.
