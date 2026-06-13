# Fission Package Commands

These commands deploy the scheduled pipeline functions.

## Build Source Package

```bash
chmod +x build.sh
zip -r cost-living-data-platform.zip \
  backend scripts database requirements.txt requirements-fission.txt build.sh \
  -x "**/__pycache__/*" "*.pyc"
```

Do not commit generated zip files.

## Environment and Secrets

```bash
kubectl apply -f deployment/fission/platform-environment.yaml
kubectl apply -f deployment/fission/platform-secrets.example.yaml
```

Replace placeholder values in the Secret before using it in a real cluster.

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
fission function test --name cost-living-platform-nlp-processor --timeout 20m
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

## Elasticsearch Checks

```bash
python scripts/inspect_es_indices.py --json --sample-size 0
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
