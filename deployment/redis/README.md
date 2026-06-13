# Redis Runtime Queue

This directory contains a lightweight Redis deployment for optional pipeline runtime coordination.

Redis is used only for job-level runtime concerns:

- distributed locks for scheduled jobs;
- recent pipeline lifecycle events;
- API diagnostics through `/api/cost-living/pipeline/runtime`.

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
