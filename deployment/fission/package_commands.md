# Fission Package Commands

These commands are the manual form of the scheduled pipeline deployment. The recommended deployment entrypoint is:

```bash
make cloud-deploy
```

Use the commands below when debugging one layer at a time.

## Build Source Package

```bash
chmod +x build.sh
scripts/build_fission_package.sh
```

Do not commit generated zip files.

## Environment and Secrets

```bash
kubectl apply -f deployment/fission/platform-environment.yaml
kubectl apply -f deployment/fission/platform-secrets.example.yaml
```

Create real Secret values before using the manifest in a real cluster.

## Package

Create:

```bash
fission package create \
  --name cost-living-platform-pipeline-pkg \
  --env cost-living-platform-python39 \
  --src cost-living-data-platform.zip \
  --buildcmd "./build.sh"
```

Update:

```bash
fission package update \
  --name cost-living-platform-pipeline-pkg \
  --env cost-living-platform-python39 \
  --src cost-living-data-platform.zip \
  --buildcmd "./build.sh" \
  --force
```

Check:

```bash
fission package list
```

## Config and Functions

```bash
kubectl apply -f deployment/fission/platform-configmap.yaml
kubectl apply -f deployment/fission/platform-pipeline-functions.yaml
```

Manual checks:

```bash
fission function test --name cost-living-platform-bluesky-harvester --timeout 5m
fission function test --name cost-living-platform-mastodon-au-harvester --timeout 5m
fission function test --name cost-living-platform-mastodon-social-harvester --timeout 5m
fission function test --name cost-living-platform-gdelt-harvester --timeout 8m
fission function test --name cost-living-platform-raw-integrator --timeout 10m
fission function test --name cost-living-platform-official-indicators --timeout 5m
```

## Timers

Enable timers after manual tests pass:

```bash
kubectl apply -f deployment/fission/platform-pipeline-timers.yaml
```

Inspect:

```bash
fission timer list
kubectl get timetrigger -n default
```

## KEDA NLP Worker

Deploy the queue-based NLP worker after Redis and the API ConfigMap exist:

```bash
kubectl apply -f deployment/kubernetes/nlp-worker-deployment.yaml
kubectl -n cost-living get scaledobject,pods -l app=cost-living-platform-nlp-worker
```

Raw integration writes work items to:

```text
cost_living_pipeline:queue:nlp
```

Worker messages retry up to `NLP_QUEUE_MAX_ATTEMPTS`. Exhausted or malformed messages are isolated in:

```text
cost_living_pipeline:queue:nlp:dead-letter
```

## Elasticsearch Checks

```bash
python scripts/inspect_es_indices.py --json --sample-size 0
python scripts/apply_elasticsearch_lifecycle.py
```

The raw integrator should read only these source streams:

```text
cost_living_bluesky_raw_stream
cost_living_mastodon_raw_stream
cost_living_gdelt_raw_stream
```

## GDELT Historical Backfill

The backfill command uses the same GDELT GKG archive processor as the incremental Fission harvester:

```bash
python -m backend.harvesters.gdelt_backfill \
  --start-date 2026-05-01 \
  --end-date 2026-05-02 \
  --max-archives 4 \
  --dry-run
```

Remove `--dry-run` to process archives. The checkpoint path is controlled by `GDELT_GKG_BACKFILL_CHECKPOINT_PATH`.
