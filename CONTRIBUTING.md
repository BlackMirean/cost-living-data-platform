# Contributing

This repository is maintained as a public engineering portfolio project. Keep contributions focused on the cost-of-living data platform and preserve the public release hygiene.

## Development Setup

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Run the default checks before opening a pull request:

```bash
make ci
```

## Repository Hygiene

- Do not commit credentials, kubeconfigs, local `.env` files or harvested private data.
- Keep generated caches, local Elasticsearch exports and cloud backfill state out of Git.
- Keep documentation in English and keep API, data contract and deployment docs aligned.
- Prefer small, reviewable changes with tests for API, analytics and processing behavior.
- Use stable index names from `.env.example` and deployment manifests.

## Data and Ethics

Only use public data sources and document collection terms clearly. Treat social and media data as noisy observational signals, not representative survey data or causal evidence.
