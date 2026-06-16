# Redis Runtime Services

This directory contains a lightweight Redis deployment for pipeline runtime coordination, queueing and API response caching.

Redis is used for runtime concerns that should not be stored in Elasticsearch:

- distributed locks for scheduled jobs;
- recent pipeline lifecycle events;
- API diagnostics through `/api/cost-living/pipeline/runtime` and `/api/cost-living/pipeline/events`;
- shared short-lived API response caching for repeated analytics reads, exposed through `/api/cost-living/cache/status`;
- the NLP work queue and dead-letter queue exposed through `/api/cost-living/pipeline/queues`.

Elasticsearch remains the source of truth for raw documents, processed documents, status fields and analytics.

The Kubernetes manifest enables append-only persistence on a `redis-data` PersistentVolumeClaim. Redis is still runtime state, not the analytical source of truth; Elasticsearch owns document state and API analytics.

## Deploy

```bash
kubectl apply -f deployment/redis/redis.yaml
kubectl -n redis rollout status deployment/redis
kubectl -n redis get pvc redis-data
```

Then enable Redis in the API and Fission ConfigMaps:

```yaml
REDIS_ENABLED: "true"
REDIS_URL: "redis://redis.redis.svc.cluster.local:6379/0"
```

If Redis is unavailable, queue-backed NLP processing and API cache sharing are degraded. Scheduled jobs keep Elasticsearch as the source of truth, and queue messages can be rebuilt from raw processing state with `make requeue-nlp`.
