# Performance Baseline

Measured on 2026-06-17 03:38 AEST against the Kubernetes API through local port-forwarding.

## Cloud API Burst

Command:

```bash
python scripts/stress_cost_living_platform_api.py \
  --base-url http://127.0.0.1:18081 \
  --prefix /api/cost-living \
  --rounds 10 \
  --workers 3 \
  --timeout 120 \
  --output-json build/cloud-stress.json
```

| Metric | Value |
| --- | ---: |
| Requests | 60 |
| Success | 60 |
| Failures | 0 |
| HTTP 200 | 60 |
| HTTP 429 | 0 |
| Average latency | 92 ms |
| p95 latency | 203 ms |
| Max latency | 1.163 s |
| Elapsed wall time | 1.863 s |

Slowest p95 endpoints in this run:

| Endpoint | p95 |
| --- | ---: |
| `/pipeline/status` | 1.163 s |
| `/categories/counts` | 1.015 s |
| `/categories/sentiment` | 674 ms |

The burst stayed below the configured rate limit of 600 requests per 60-second window, so no 429 responses were expected.

## Docker Compose Integration

Command:

```bash
make integration
```

The integration stack uses dedicated local ports: API `18000`, Elasticsearch `19200`, Redis `16379`.

| Check | Result |
| --- | ---: |
| Seeded raw documents | 2 |
| Worker queue messages processed | 1 |
| Processed documents written | 2 |
| Worker drain wall time | 1 s |
| Worker runtime event duration | 176.9 ms |
| Smoke endpoints | 25 / 25 |
| OpenAPI contract checks | 12 / 12 |
| Stress requests | 12 / 12 |
| Stress p95 latency | 15 ms |

## Release Gates

Use these thresholds as a practical baseline for public changes:

| Gate | Threshold |
| --- | --- |
| Unit tests | `make test` passes |
| Public hygiene | `make public-check` passes |
| Compose integration | worker processes at least one queue message and writes processed docs |
| API contract | 12 / 12 OpenAPI checks pass |
| Cloud drift | `make cloud-drift` passes |
| Cloud smoke | 25 / 25 endpoints pass |
| Cloud stress | 0 failures, 0 unexpected 429s, p95 under 1.5 s |

