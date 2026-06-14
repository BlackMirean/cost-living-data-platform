# Redis Runtime Services

This directory contains a lightweight Redis deployment for optional pipeline runtime coordination and API response caching.

Redis is used for runtime concerns that should not be stored in Elasticsearch:

- distributed locks for scheduled jobs;
- recent pipeline lifecycle events;
- API diagnostics through `/api/cost-living/pipeline/runtime` and `/api/cost-living/pipeline/events`;
- shared short-lived API response caching for repeated analytics reads, exposed through `/api/cost-living/cache/status`.

Elasticsearch remains the source of truth for raw documents, processed documents, status fields and analytics.

## Deploy

```bash
kubectl apply -f deployment/redis/redis.yaml
```

Then enable Redis in the API and Fission ConfigMaps:

```yaml
REDIS_ENABLED: "true"
REDIS_URL: "redis://redis.redis.svc.cluster.local:6379/0"
```

If Redis is unavailable, the pipeline fails open: scheduled jobs still run, but lock and event features are disabled until Redis is reachable again.
