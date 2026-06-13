# Kubernetes API Deployment

This directory contains manifests for serving the FastAPI analytics API.

Fission handles scheduled ingestion and processing. Kubernetes serves the API.

## Manifests

| Manifest | Purpose |
| --- | --- |
| `namespace.yaml` | API namespace |
| `configmap.yaml` | Index names and API settings |
| `secrets.example.yaml` | Elasticsearch credential template |
| `api-deployment.yaml` | FastAPI Deployment |
| `api-service.yaml` | API Service |
| `api-hpa.yaml` | Optional horizontal pod autoscaler |

## Image

Build and push the API image:

```bash
docker build -f deployment/docker/api.Dockerfile \
  -t ghcr.io/your-username/cost-living-platform-api:latest .
docker push ghcr.io/your-username/cost-living-platform-api:latest
```

Update `api-deployment.yaml` with the actual image name before deploying.

## Deploy

Create a real Secret from `secrets.example.yaml`, then apply:

```bash
kubectl apply -f deployment/kubernetes/namespace.yaml
kubectl apply -f deployment/kubernetes/secrets.example.yaml
kubectl apply -f deployment/kubernetes/configmap.yaml
kubectl apply -f deployment/kubernetes/api-deployment.yaml
kubectl apply -f deployment/kubernetes/api-service.yaml
```

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
http://127.0.0.1:8010/docs
```

Notebook environment:

```bash
export API_BASE_URL=http://127.0.0.1:8010
export API_PREFIX=/api/cost-living
export API_TIMEOUT_SECONDS=120
export API_PREFLIGHT_TIMEOUT_SECONDS=60
```
