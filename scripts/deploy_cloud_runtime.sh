#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
package_name="cost-living-platform-pipeline-pkg"
environment_name="cost-living-platform-python39"
package_zip="${PACKAGE_ZIP:-$repo_root/build/cost-living-platform.zip}"
python_bin="${PYTHON:-python}"

cd "$repo_root"

require_command() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "$name is required for cloud deployment" >&2
    exit 1
  fi
}

require_secret() {
  local namespace="$1"
  local name="$2"
  kubectl -n "$namespace" get secret "$name" >/dev/null
}

require_command kubectl
require_command fission
require_command zip
require_command unzip
require_command "$python_bin"

echo "Checking cluster access"
kubectl get namespace default >/dev/null

echo "Applying namespaces"
kubectl apply -f deployment/kubernetes/namespace.yaml

echo "Checking required secrets"
require_secret default cost-living-platform-secrets
require_secret cost-living cost-living-platform-api-secrets

echo "Building source package"
scripts/build_fission_package.sh "$package_zip" >/dev/null

echo "Applying base runtime resources"
kubectl apply -f deployment/redis/redis.yaml
kubectl apply -f deployment/fission/platform-environment.yaml
kubectl apply -f deployment/fission/platform-configmap.yaml
kubectl -n redis rollout status deployment/redis --timeout=180s

echo "Publishing Fission package"
if kubectl -n default get package "$package_name" >/dev/null 2>&1; then
  fission package update \
    --name "$package_name" \
    --env "$environment_name" \
    --src "$package_zip" \
    --buildcmd "./build.sh" \
    --force
else
  fission package create \
    --name "$package_name" \
    --env "$environment_name" \
    --src "$package_zip" \
    --buildcmd "./build.sh"
fi

echo "Waiting for package build"
for _ in $(seq 1 60); do
  status="$(kubectl -n default get package "$package_name" -o jsonpath='{.status.buildstatus}' 2>/dev/null || true)"
  case "$status" in
    succeeded)
      break
      ;;
    failed)
      kubectl -n default get package "$package_name" -o yaml
      exit 1
      ;;
  esac
  sleep 5
done

status="$(kubectl -n default get package "$package_name" -o jsonpath='{.status.buildstatus}')"
if [ "$status" != "succeeded" ]; then
  kubectl -n default get package "$package_name" -o yaml
  exit 1
fi

package_url="$(kubectl -n default get package "$package_name" -o jsonpath='{.spec.deployment.url}')"
if [ -z "$package_url" ]; then
  echo "Package URL was empty after successful build" >&2
  exit 1
fi

echo "Applying Fission pipeline"
kubectl apply -f deployment/fission/platform-pipeline-functions.yaml
kubectl apply -f deployment/fission/platform-pipeline-timers.yaml

echo "Applying Kubernetes API and worker runtime"
kubectl apply -f deployment/kubernetes/configmap.yaml
kubectl -n cost-living patch configmap cost-living-platform-api-config \
  --type merge \
  -p "{\"data\":{\"FISSION_PACKAGE_URL\":\"$package_url\"}}"
kubectl apply -f deployment/kubernetes/api-deployment.yaml
kubectl apply -f deployment/kubernetes/api-service.yaml
kubectl apply -f deployment/kubernetes/nlp-worker-deployment.yaml
kubectl apply -f deployment/kubernetes/api-hpa.yaml

echo "Waiting for API rollout"
kubectl -n cost-living rollout status deployment/cost-living-platform-api --timeout=180s

echo "Checking live resource drift"
"$python_bin" scripts/check_cloud_drift.py

echo "Deployment complete"
