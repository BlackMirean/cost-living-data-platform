# Frontend API Contract

The frontend and notebooks call the FastAPI service only. They do not connect to Elasticsearch directly.

Public prefix:

```text
/api/cost-living
```

The application also accepts the internal `/api/...` paths so local tests can call either form.

## Local Access

Run the API locally:

```bash
uvicorn backend.api.main:app --reload --port 8000
```

Base URL:

```text
http://127.0.0.1:8000
```

For Kubernetes, port-forward the API service:

```bash
kubectl -n cost-living port-forward svc/cost-living-platform-api 8010:80
```

## Common Parameters

| Parameter | Values | Default | Meaning |
| --- | --- | --- | --- |
| `source_group` | `all`, `social`, `media` | `all` | `social = bluesky + mastodon`; `media = gdelt` |
| `platform` | `gdelt`, `mastodon`, `bluesky` | empty | Platform filter; comma-separated values are supported |
| `topic` | topic keys | empty | Topic filter; comma-separated values are supported |
| `start` | ISO date or datetime | empty | Start time over `created_at` |
| `end` | ISO date or datetime | empty | End time over `created_at` |
| `period` | `day`, `month` | `month` | Time aggregation level |
| `quality` | `all`, `clean` | `all` | `clean` excludes default noisy flags |
| `exclude_quality_flags` | comma-separated flags | empty | Exclude selected quality flags |

If both `source_group` and `platform` are provided, the backend applies their intersection.

## Endpoints

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | API and Elasticsearch health |
| `GET /pipeline/status` | Raw, processed, discarded, failed and CPI counts |
| `GET /stats/overview` | High-level document and sentiment summary |
| `GET /trends/documents` | Document counts over time |
| `GET /categories/counts` | Topic/category counts |
| `GET /categories/sentiment` | Sentiment distribution by topic |
| `GET /categories/share` | Topic share over time |
| `GET /data-quality/summary` | Quality flags and processing outcomes |
| `GET /data-quality/comparison` | Quality comparison by source |
| `GET /media/coverage` | GDELT media coverage trends |
| `GET /platforms/categories` | Platform-topic matrix |
| `GET /trends/categories` | Topic trends by period |
| `GET /trends/sentiment` | Sentiment trends by period |
| `GET /official/comparison` | ABS CPI and discussion comparison |
| `GET /categories/yoy-change` | Year-over-year topic movement |
| `GET /categories/volatility` | Topic volatility |
| `GET /categories/keywords` | Frequent matched keywords |
| `GET /logs/errors` | Recent NLP processing errors |

Prefix these paths with `/api/cost-living` in deployed and notebook clients.

## Client Pattern

```ts
const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ?? "http://127.0.0.1:8000";
const API_PREFIX =
  import.meta.env.VITE_API_PREFIX ?? "/api/cost-living";

export async function apiGet<T>(
  path: string,
  params: Record<string, string | number | undefined> = {},
): Promise<T> {
  const requestPath = path.startsWith("/api/")
    ? path
    : `${API_PREFIX}${path.startsWith("/") ? path : `/${path}`}`;
  const url = new URL(requestPath, API_BASE_URL);

  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") {
      url.searchParams.set(key, String(value));
    }
  }

  const response = await fetch(url);
  const data = await response.json();

  if (!response.ok) {
    throw new Error(data.detail ?? `API request failed: ${response.status}`);
  }

  return data as T;
}
```

## Response Conventions

- Empty result sets use `rows: []` and are not API errors.
- Ratio fields such as `percentage`, `negative_ratio` and `duplicate_ratio` are decimals from `0` to `1`.
- Monthly trend endpoints return `YYYY-MM`; daily endpoints return `YYYY-MM-DD`.
- GDELT rows should be labelled as media coverage, not public sentiment.
