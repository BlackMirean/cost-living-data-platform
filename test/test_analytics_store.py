from backend.common import analytics_store


POSTS = [
    {
        "raw_id": "p1",
        "canonical_id": "canonical-1",
        "platform": "gdelt",
        "source_group": "media",
        "topic": "housing",
        "topic_source": "text_keywords",
        "text": "Rent keeps rising",
        "created_at": "2025-09-15T00:00:00+00:00",
        "harvested_at": "2025-09-16T00:00:00+00:00",
        "processed_at": "2025-09-16T01:00:00+00:00",
        "sentiment_score": -0.4,
        "sentiment_label": "negative",
        "processing_status": "processed",
        "quality_flags": ["metadata_heavy"],
    },
    {
        "raw_id": "p2",
        "canonical_id": "canonical-1",
        "platform": "bluesky",
        "source_group": "social",
        "topic": "housing",
        "topic_source": "text_keywords",
        "text": "Mortgage stress is everywhere",
        "created_at": "2025-09-20T00:00:00+00:00",
        "harvested_at": "2025-09-20T01:00:00+00:00",
        "processed_at": "2025-09-20T02:00:00+00:00",
        "sentiment_score": -0.3,
        "sentiment_label": "negative",
        "processing_status": "processed",
        "quality_flags": [],
    },
    {
        "raw_id": "p3",
        "canonical_id": "canonical-3",
        "platform": "mastodon",
        "source_group": "social",
        "topic": "groceries",
        "topic_source": "harvest_category_fallback",
        "text": "Bread prices are okay today",
        "created_at": "2025-10-10T00:00:00+00:00",
        "harvested_at": "2025-10-10T01:00:00+00:00",
        "processed_at": "2025-10-10T02:00:00+00:00",
        "sentiment_score": 0.2,
        "sentiment_label": "positive",
        "processing_status": "processed",
        "quality_flags": [],
    },
    {
        "raw_id": "p4",
        "platform": "mastodon",
        "topic": "housing",
        "text": "Sale tickets available now",
        "created_at": "2025-09-21T00:00:00+00:00",
        "sentiment_score": None,
        "sentiment_label": None,
        "processing_status": "discarded",
    },
]

RAW_POSTS = [
    {
        "id": "r1",
        "source_index": "gdelt_gkg_raw_full",
        "analysis_status": "processed",
        "harvested_at": "2025-09-16T00:00:00Z",
    },
    {
        "id": "r2",
        "source_index": "mastodon_raw_stream",
        "analysis_status": "pending",
        "harvested_at": "2025-09-17T00:00:00Z",
    },
    {
        "id": "r3",
        "source_index": "mastodon_raw_stream",
        "analysis_status": "discarded",
        "harvested_at": "2025-09-18T00:00:00Z",
    },
    {
        "id": "r4",
        "source_index": "gdelt_gkg_raw_full",
        "analysis_status": "processing",
        "analysis_started_at": "2025-09-19T00:00:00Z",
        "harvested_at": "2025-09-19T00:00:00Z",
    },
]

INDICATORS = [
    {
        "id": "i1",
        "indicator": "monthly_cpi",
        "measure": "Percentage change from previous year",
        "item_name": "Rents",
        "period": "2025-09",
        "period_start": "2025-09-01T00:00:00+00:00",
        "value": 6.2,
    },
    {
        "id": "i2",
        "indicator": "monthly_cpi",
        "measure": "Index numbers",
        "item_name": "Rents",
        "period": "2025-09",
        "period_start": "2025-09-01T00:00:00+00:00",
        "value": 120.4,
    },
]


def use_mock_memory_store(monkeypatch):
    monkeypatch.setattr(analytics_store, "use_local_store", lambda: True)
    monkeypatch.setattr(analytics_store, "load_local_posts", lambda: POSTS)
    monkeypatch.setattr(analytics_store, "load_local_raw_posts", lambda: RAW_POSTS)
    monkeypatch.setattr(analytics_store, "load_local_indicators", lambda: INDICATORS)


def test_pipeline_status_memory(monkeypatch):
    use_mock_memory_store(monkeypatch)
    result = analytics_store.pipeline_status()
    assert result["raw_documents"] == 4
    assert result["processed_documents"] == 3
    assert result["unprocessed_documents"] == 1
    assert result["processing_documents"] == 1
    assert result["stale_processing_documents"] == 1
    assert result["discarded_documents"] == 1
    assert result["pending_by_source"]["mastodon_raw_stream"] == 1
    assert result["processing_by_source"]["gdelt_gkg_raw_full"] == 1


def test_category_counts_and_sentiment_memory(monkeypatch):
    use_mock_memory_store(monkeypatch)
    counts = analytics_store.category_counts()
    assert counts["total_complaints"] == 3
    housing = next(row for row in counts["rows"] if row["cost_category"] == "housing")
    assert housing["category_label"] == "Housing / Rent"
    assert housing["complaint_count"] == 2
    assert housing["unique_document_count"] == 1
    assert housing["duplicate_ratio"] == 0.5

    sentiment = analytics_store.category_sentiment()
    housing_sentiment = next(row for row in sentiment["rows"] if row["cost_category"] == "housing")
    assert housing_sentiment["negative_ratio"] == 1.0
    assert housing_sentiment["avg_sentiment"] == -0.35


def test_quality_filter_excludes_flagged_documents_memory(monkeypatch):
    use_mock_memory_store(monkeypatch)
    counts = analytics_store.category_counts(quality="clean")

    assert counts["total_complaints"] == 2
    housing = next(row for row in counts["rows"] if row["cost_category"] == "housing")
    assert housing["complaint_count"] == 1

    counts = analytics_store.category_counts(exclude_quality_flags="metadata_heavy")
    assert counts["total_complaints"] == 2


def test_data_quality_summary_and_comparison_memory(monkeypatch):
    use_mock_memory_store(monkeypatch)

    summary = analytics_store.data_quality_summary()
    assert summary["total_documents"] == 3
    assert summary["clean_documents"] == 2
    assert summary["duplicates"]["unique_document_count"] == 2
    assert summary["duplicates"]["duplicate_ratio"] == 0.3333
    assert {"flag": "metadata_heavy", "document_count": 1} in summary["quality_flags"]

    comparison = analytics_store.quality_comparison()
    housing = next(row for row in comparison["rows"] if row["cost_category"] == "housing")
    assert housing["all_document_count"] == 2
    assert housing["clean_document_count"] == 1
    assert housing["excluded_document_count"] == 1


def test_media_coverage_memory(monkeypatch):
    use_mock_memory_store(monkeypatch)

    result = analytics_store.media_coverage(period="month", quality="all")
    housing = next(row for row in result["rows"] if row["cost_category"] == "housing")
    assert result["source_group"] == "media"
    assert housing["period"] == "2025-09"
    assert housing["coverage_count"] == 1


def test_trends_and_official_comparison_memory(monkeypatch):
    use_mock_memory_store(monkeypatch)
    trend = analytics_store.trends_categories(period="month")
    assert any(
        row["period"] == "2025-09"
        and row["cost_category"] == "housing"
        and row["complaint_count"] == 2
        for row in trend["rows"]
    )

    comparison = analytics_store.official_comparison(topic="housing")
    assert comparison["coverage"]["overlap_period_max"] == "2025-09"
    assert comparison["rows"][0]["official_indicator"] == "Rents"
    assert comparison["rows"][0]["official_value"] == 6.2
    assert comparison["rows"][0]["official_index_value"] == 120.4


def test_platform_categories_memory(monkeypatch):
    use_mock_memory_store(monkeypatch)
    result = analytics_store.platform_categories()
    bluesky_housing = next(
        row
        for row in result["rows"]
        if row["platform"] == "bluesky" and row["cost_category"] == "housing"
    )
    assert bluesky_housing["percentage_within_platform"] == 1.0


def test_error_logs_short_circuits_when_no_errors(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.search_calls = []

        def search(self, *, index, body):
            self.search_calls.append({"index": index, "body": body})
            return {
                "aggregations": {
                    "by_status": {
                        "buckets": [
                            {"key": "processed", "doc_count": 10},
                            {"key": "pending", "doc_count": 1},
                        ]
                    }
                }
            }

    fake_client = FakeClient()
    monkeypatch.setattr(analytics_store, "use_local_store", lambda: False)
    monkeypatch.setattr(analytics_store, "get_es_client", lambda: fake_client)
    monkeypatch.setattr(analytics_store, "_term_field", lambda field, index_name=None: field)

    result = analytics_store.error_logs(size=20)

    assert result["rows"] == []
    assert result["summary"]["errors"] == 0
    assert len(fake_client.search_calls) == 1
