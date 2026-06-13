#!/usr/bin/env bash

# Source this file from the repo root before running cloud-backed maintenance scripts:
#   source scripts/load_cloud_env.sh

export KUBECONFIG="${KUBECONFIG:-$PWD/config.yaml}"

if ! command -v kubectl >/dev/null 2>&1; then
  echo "kubectl is required to load the Elasticsearch password." >&2
  return 1 2>/dev/null || exit 1
fi

export ES_PASSWORD
ES_PASSWORD="$(kubectl -n elastic get secret elasticsearch-es-elastic-user -o go-template='{{.data.elastic | base64decode}}')"

export ELASTICSEARCH_URL="${ELASTICSEARCH_URL:-https://localhost:9201}"
export ELASTICSEARCH_USERNAME="${ELASTICSEARCH_USERNAME:-elastic}"
export ELASTICSEARCH_PASSWORD="$ES_PASSWORD"
export ELASTICSEARCH_VERIFY_CERTS="${ELASTICSEARCH_VERIFY_CERTS:-false}"

export RAW_POSTS_INDEX="${RAW_POSTS_INDEX:-cost_living_raw_posts}"
export POSTS_INDEX="${POSTS_INDEX:-cost_living_posts_current}"
export PROCESSED_POSTS_WRITE_INDEX="${PROCESSED_POSTS_WRITE_INDEX:-cost_living_processed_posts}"
export POSTS_CURRENT_ALIAS="${POSTS_CURRENT_ALIAS:-cost_living_posts_current}"
export INDICATORS_INDEX="${INDICATORS_INDEX:-cost_living_indicators}"
export MONTHLY_METRICS_INDEX="${MONTHLY_METRICS_INDEX:-cost_living_monthly_topic_metrics}"

echo "Loaded cloud Elasticsearch env. Unified raw index: $RAW_POSTS_INDEX. Stream credentials come from .env or Kubernetes Secret."
