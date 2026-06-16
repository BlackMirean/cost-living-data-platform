# Kubernetes API Deployment

This directory contains manifests for serving the FastAPI analytics API and the queue-based NLP worker.

Fission handles scheduled ingestion and raw integration. Kubernetes serves the API and runs the KEDA-scaled NLP worker. The API and worker consume the same Fission source package URL that is produced during deployment, so the cluster does not require a separately published application image.

## Manifests

| Manifest | Purpose |
| --- | --- |
| `namespace.yaml` | API namespace |
| `configmap.yaml` | Index names and API settings |
| `secrets.example.yaml` | Elasticsearch credential template |
| `api-deployment.yaml` | FastAPI Deployment |
| `api-service.yaml` | API Service |
| `api-hpa.yaml` | API horizontal pod autoscaler |
| `nlp-worker-deployment.yaml` | KEDA-scaled NLP worker deployment and ScaledObject |

The API reports Redis runtime status, recent pipeline events, NLP queue depth, API cache status and rate-limit status when Redis is enabled through `REDIS_ENABLED=true`.

## Runtime Package

Build the source package:

```bash
make fission-package
```

The deploy script publishes this package through Fission, patches `FISSION_PACKAGE_URL` into `cost-living-platform-api-config`, then rolls the API and worker Deployments. Docker image builds remain useful for local and CI verification through `deployment/docker/api.Dockerfile`, but they are not the canonical cloud runtime.

## Deploy

Create real Secrets from `secrets.example.yaml` and `../fission/platform-secrets.example.yaml`, then deploy the complete runtime:

```bash
make cloud-deploy
```

Validate live resources against the repository manifests:

```bash
make cloud-drift
```

Redis runtime services are part of the recommended deployment and use persistent storage. The provided API and Fission ConfigMaps enable Redis. Set `REDIS_ENABLED=false` only when deploying without Redis and without the KEDA NLP worker.

Optional HPA:

```bash
kubectl apply -f deployment/kubernetes/api-hpa.yaml
```

## Access

```bash
kubectl -n cost-living port-forward svc/cost-living-platform-api 8010:80
```

```text
http://127.0.0.1:8010/api/cost-living/health
http://127.0.0.1:8010/api/cost-living/pipeline/events?limit=20
http://127.0.0.1:8010/api/cost-living/pipeline/queues
http://127.0.0.1:8010/api/cost-living/metrics
http://127.0.0.1:8010/docs
```

Validation:

```bash
API_BASE_URL=http://127.0.0.1:8010 make smoke
API_BASE_URL=http://127.0.0.1:8010 make contract
API_BASE_URL=http://127.0.0.1:8010 make stress
```

Notebook environment:

```bash
export API_BASE_URL=http://127.0.0.1:8010
export API_PREFIX=/api/cost-living
export API_TIMEOUT_SECONDS=120
export API_PREFLIGHT_TIMEOUT_SECONDS=60
```
