# Data Contracts

This document defines the Elasticsearch fields used by the optimized `cost-living-platform-*` stack. The Jupyter frontend does not query Elasticsearch directly. It calls the REST API, and the API maps these fields into chart-ready JSON.

## 1. Platform Raw Streams

Platform harvesters write source-specific records into these raw stream indices:

```text
cost_living_bluesky_raw_stream
cost_living_mastodon_raw_stream
cost_living_gdelt_raw_stream
```

The raw integrator reads only these configured platform stream indices. This keeps ingestion inputs explicit and prevents accidental reads from unrelated Elasticsearch indices.

GDELT raw stream records are produced from public GKG archive files listed in `masterfilelist.txt`. Both incremental harvesting and historical backfill use the same archive processor and explicit mapping in:

```text
database/mappings/gdelt_raw_stream.json
```

If records are restored from a backup or backfill, their `source_index` values are kept unchanged for provenance. Do not rewrite source lineage to make restored data look like live harvest output.

Source metadata is centralized in static platform plugins:

```text
backend/platforms/plugins.py
```

The source registry in `backend/common/source_registry.py` exposes plugin metadata to the raw integrator, NLP worker and API. Each plugin defines the source name, source group, raw stream index, optimized stream index, label, Fission handler list and schedule list.

| Source | Source group | Raw stream index |
| --- | --- | --- |
| `bluesky` | `social` | `cost_living_bluesky_raw_stream` |
| `mastodon` | `social` | `cost_living_mastodon_raw_stream` |
| `gdelt` | `media` | `cost_living_gdelt_raw_stream` |

This is a Level 1 extension point. Source metadata lives in one place and only covers platforms currently deployed by this project.

## 2. Unified Raw Data

Index:

```text
cost_living_raw_posts
```

This index stores normalized raw records and NLP processing state.

| Field | Type | Notes |
| --- | --- | --- |
| `id` | keyword | Unified raw document id |
| `platform` | keyword | `bluesky`, `mastodon` or `gdelt` |
| `source_index` | keyword | Upstream raw stream index |
| `source_es_id` | keyword | Upstream Elasticsearch document id |
| `source_id` | keyword | Original platform id |
| `stage` | keyword | Usually `raw` |
| `category` | keyword | Harvest-stage category |
| `text` | text | Raw text |
| `created_at` | date | Source publication time |
| `collected_at` | date | Upstream collection time |
| `harvested_at` | date | Time written to unified raw index |
| `url` | keyword | Source URL |
| `like_count` | long | Likes if available |
| `reply_count` | long | Replies if available |
| `repost_count` | long | Reposts if available |
| `quote_count` | long | Quotes if available |
| `has_engagement_metrics` | boolean | Whether engagement fields are present |
| `analysis_status` | keyword | `pending`, `processing`, `processed`, `discarded` or `error` |
| `analysis_started_at` | date | NLP claim time |
| `analysis_processed_at` | date | NLP completion time |
| `payload` | object, disabled | Original payload, stored for traceability |

Raw document ids use:

```text
sha1(source_index | source_es_id)
```

The same upstream record writes to the same `_id`. Re-running a harvester or integrator should not create duplicates.

## 3. Processed Data

Index:

```text
cost_living_processed_posts
```

Stable read alias:

```text
cost_living_posts_current
```

The API reads through the alias. This lets the processed index be rebuilt without changing notebook code.

| Field | Type | Notes |
| --- | --- | --- |
| `raw_id` | keyword | Corresponding raw document id |
| `canonical_id` | keyword | Stable id used for duplicate analysis |
| `source_index` | keyword | Upstream index |
| `source_es_id` | keyword | Upstream Elasticsearch document id |
| `platform` | keyword | `bluesky`, `mastodon` or `gdelt` |
| `source_group` | keyword | `social`, `media` or `unknown` |
| `created_at` | date | Source publication time |
| `harvested_at` | date | Raw ingestion time |
| `processed_at` | date | NLP processing time |
| `text` | text | Cleaned text |
| `raw_text` | text | Original text |
| `category` | keyword | Harvest-stage category |
| `harvest_category` | keyword | Explicit copy of harvest-stage category |
| `topic` | keyword | Cost-of-living topic |
| `topic_source` | keyword | `text_keywords`, `harvest_category_fallback` or `default` |
| `matched_keywords` | keyword[] | Cost-of-living keywords matched in text |
| `sentiment_label` | keyword | `negative`, `neutral` or `positive` |
| `sentiment_score` | float | VADER compound score |
| `model_name` | keyword | Currently `vader` |
| `model_version` | keyword | Currently `cost_living_topic_sentiment_2026_06` |
| `processor_version` | keyword | NLP worker contract version |
| `processing_status` | keyword | `processed`, `discarded` or `error` |
| `relevance_score` | integer | Lightweight relevance score |
| `quality_flags` | keyword[] | Quality flags such as `metadata_heavy` |
| `url` | keyword | Source URL |

API chart aggregations include only documents with:

```text
processing_status = processed
sentiment_score exists
```

Discarded and error records are kept for diagnostics. They are not included in chart aggregations.

## 4. Topics

Current topic keys:

```text
housing
groceries
energy
fuel
transport
eating_out
healthcare
home_goods
education
inflation
wages
debt
```

The NLP processor first infers topics from cleaned text. If it only has a broad harvest-stage category, it falls back to that category and records the source in `topic_source`.

The API maps these fields for frontend use:

```text
cost_category = topic
category_label = display label
```

Some endpoints also return canonical duplicate metrics:

```text
unique_document_count = cardinality(canonical_id)
duplicate_ratio = (document_count - unique_document_count) / document_count
```

## 5. Official Indicators

Index:

```text
cost_living_indicators
```

This index stores ABS CPI observations.

```text
source = ABS Data API
dataflow = ABS:CPI(2.0.0)
```

| Field | Type | Notes |
| --- | --- | --- |
| `id` | keyword | Indicator document id |
| `source` | keyword | `abs_data_api` |
| `dataflow` | keyword | `ABS:CPI(2.0.0)` |
| `indicator` | keyword | Currently `monthly_cpi` |
| `series_key` | keyword | ABS series key |
| `measure_code` | keyword | ABS measure code |
| `measure` | keyword | Measure name, such as `Index numbers` |
| `item_code` | keyword | CPI item code |
| `item_name` | keyword | CPI item name |
| `adjustment_code` | keyword | Adjustment code |
| `adjustment` | keyword | Adjustment name |
| `region_code` | keyword | Currently `50` |
| `region` | keyword | Currently `Australia` |
| `frequency_code` | keyword | `M` |
| `frequency` | keyword | `Monthly` |
| `period` | keyword | `YYYY-MM` |
| `period_start` | date | First day of the month |
| `value` | float | Indicator value |
| `harvested_at` | date | Ingestion time |
| `raw_row` | object, disabled | Original ABS row |

## 6. Monthly Rollup

Index:

```text
cost_living_monthly_topic_metrics
```

This is an optional acceleration layer. It does not replace the processed index.

| Field | Type | Notes |
| --- | --- | --- |
| `metric_id` | keyword | Rollup document id |
| `period` | keyword | `YYYY-MM` |
| `period_start` | date | First day of the month |
| `source_group` | keyword | `all`, `social` or `media` |
| `quality` | keyword | `all` or `clean` |
| `cost_category` | keyword | Topic key |
| `category_label` | keyword | Display label |
| `document_count` | long | Monthly document count |
| `unique_document_count` | long | Canonical deduplicated count |
| `duplicate_ratio` | float | Duplicate ratio |
| `negative_count` | long | Negative document count |
| `negative_ratio` | float | Negative ratio |
| `avg_sentiment` | float | Average sentiment score |
| `generated_at` | date | Rollup generation time |

## 7. Topic and CPI Alignment

The API aligns processed text and CPI observations by month:

```text
processed.created_at -> YYYY-MM
indicator.period -> YYYY-MM
topic -> CPI item candidates
```

Main mapping:

| Topic | CPI item |
| --- | --- |
| `housing` | `Rents` |
| `groceries` | `Food and non-alcoholic beverages` |
| `energy` | `Electricity` |
| `fuel` | `Automotive fuel` |
| `transport` | `Transport` |
| `healthcare` | `Medical and hospital services` or `Health` |

This comparison is contextual. It is not causal modelling.

## 8. Fields That Are Not Stable Elasticsearch Contracts

The frontend should not depend directly on these API-only or derived fields:

```text
cost_category
category_label
is_complaint
keywords
city
```

`cost_category` and `category_label` are API response fields. They are not raw Elasticsearch fields.
