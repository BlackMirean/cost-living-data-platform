from backend.processing import nlp_worker


def test_clean_text_removes_social_markup():
    text = "Rent is TOO expensive! https://example.com #housing @someone"

    assert nlp_worker.clean_text(text) == "rent is too expensive!"


def test_build_contract_doc_keeps_lineage_and_sentiment_alias():
    raw = {
        "id": "raw-1",
        "source_index": "mastodon_raw_stream",
        "source_es_id": "mastodon:1",
        "platform": "mastodon",
        "category": "housing",
        "text": "Rent keeps rising",
        "created_at": "2026-05-14T00:00:00Z",
        "harvested_at": "2026-05-14T00:01:00Z",
        "like_count": 1,
    }

    doc = nlp_worker.build_contract_doc(
        raw=raw,
        raw_id="raw-1",
        cleaned_text="rent keeps rising",
        relevance_score=2,
        sentiment_score=-0.25,
        label="negative",
        processing_status="processed",
    )

    assert doc["raw_id"] == "raw-1"
    assert doc["source_index"] == "mastodon_raw_stream"
    assert doc["topic"] == "housing"
    assert doc["harvest_category"] == "housing"
    assert doc["source_group"] == "social"
    assert doc["topic_source"] == "text_keywords"
    assert "rent" in doc["matched_keywords"]
    assert doc["canonical_id"].startswith("canonical-")
    assert doc["sentiment_score"] == -0.25
    assert doc["processing_status"] == "processed"
    assert doc["model_version"] == "v2"


def test_process_hit_discards_low_relevance_ad():
    hit = {
        "_id": "raw-es-id",
        "_source": {
            "id": "raw-2",
            "platform": "bluesky",
            "category": "groceries",
            "text": "Sale",
            "created_at": "2026-05-14T00:00:00Z",
            "harvested_at": "2026-05-14T00:01:00Z",
        },
    }

    raw_es_id, status, doc = nlp_worker.process_hit(hit)

    assert raw_es_id == "raw-es-id"
    assert status == "discarded"
    assert doc["processing_status"] == "discarded"
    assert doc["sentiment_score"] is None


def test_topic_falls_back_to_harvest_category_when_text_is_generic():
    raw = {
        "id": "raw-3",
        "platform": "gdelt",
        "category": "groceries",
        "text": "household pressure keeps building",
        "url": "https://example.com/a",
    }

    doc = nlp_worker.build_contract_doc(
        raw=raw,
        raw_id="raw-3",
        cleaned_text="household pressure keeps building",
        relevance_score=0,
        sentiment_score=0.0,
        label="neutral",
        processing_status="processed",
    )

    assert doc["topic"] == "groceries"
    assert doc["topic_source"] == "harvest_category_fallback"
    assert doc["source_group"] == "media"


def test_quality_flags_mark_metadata_heavy_gdelt():
    raw = {
        "id": "raw-4",
        "platform": "gdelt",
        "category": "groceries",
        "text": "example.com tax_econ_price;wb_123;wb_456;tax_a;tax_b;tax_c;tax_d",
        "url": "https://example.com/a",
    }

    doc = nlp_worker.build_contract_doc(
        raw=raw,
        raw_id="raw-4",
        cleaned_text="example.com tax_econ_price wb metadata",
        relevance_score=-1,
        sentiment_score=0.0,
        label="neutral",
        processing_status="processed",
    )

    assert "metadata_heavy" in doc["quality_flags"]
    assert "low_relevance" in doc["quality_flags"]


def test_pending_query_retries_stale_processing_documents():
    query = nlp_worker.pending_query(stale_minutes=30)
    should = query["query"]["bool"]["should"]

    assert any(
        clause.get("bool", {})
        .get("filter", [{}, {}])[1]
        .get("range", {})
        .get("analysis_started_at", {})
        .get("lt")
        == "now-30m"
        for clause in should
        if "bool" in clause
    )


def test_process_batch_claims_bounded_docs_and_flushes(monkeypatch):
    class FakeIndices:
        def refresh(self, index):
            pass

    class FakeClient:
        indices = FakeIndices()

    hit = {
        "_id": "raw-es-id",
        "_source": {
            "id": "raw-5",
            "platform": "mastodon",
            "category": "housing",
            "text": "Rent keeps rising in Melbourne",
            "created_at": "2026-05-14T00:00:00Z",
            "harvested_at": "2026-05-14T00:01:00Z",
        },
    }
    captured = {"max_docs": None, "bulk_action_counts": []}

    def fake_claim(client, raw_index, *, batch_size, max_docs):
        captured["max_docs"] = max_docs
        return [hit]

    def fake_bulk(client, actions, raise_on_error=False):
        captured["bulk_action_counts"].append(len(actions))
        return len(actions), []

    monkeypatch.setattr(nlp_worker, "ensure_posts_index", lambda client, **kwargs: None)
    monkeypatch.setattr(nlp_worker, "claim_pending_docs", fake_claim)
    monkeypatch.setattr(nlp_worker.helpers, "bulk", fake_bulk)

    result = nlp_worker.process_batch(
        client=FakeClient(),
        raw_index="raw",
        processed_index="processed",
        batch_size=10,
        max_docs=25,
        bulk_size=1,
    )

    assert captured["max_docs"] == 25
    assert result["claimed"] == 1
    assert result["processed"] == 1
    assert result["total_written"] == 1
    assert result["raw_status_updated"] == 1
    assert captured["bulk_action_counts"] == [1, 1]


def test_claim_pending_docs_returns_only_atomically_updated_hits(monkeypatch):
    class FakeIndices:
        def __init__(self):
            self.refreshed = []

        def refresh(self, index):
            self.refreshed.append(index)

    class FakeClient:
        def __init__(self):
            self.indices = FakeIndices()

    hits = [
        {"_id": "raw-1", "_source": {"analysis_status": "pending"}},
        {"_id": "raw-2", "_source": {"analysis_status": "pending"}},
    ]
    captured = {"actions": []}

    def fake_fetch(client, raw_index, batch_size, max_docs):
        assert batch_size == 10
        assert max_docs == 20
        return hits

    def fake_streaming_bulk(client, actions, raise_on_error=False):
        captured["actions"] = list(actions)
        yield True, {"update": {"_id": "raw-1", "result": "updated"}}
        yield True, {"update": {"_id": "raw-2", "result": "noop"}}

    monkeypatch.setattr(nlp_worker, "fetch_pending_docs", fake_fetch)
    monkeypatch.setattr(nlp_worker.helpers, "streaming_bulk", fake_streaming_bulk)

    client = FakeClient()
    claimed = nlp_worker.claim_pending_docs(
        client,
        "raw-index",
        batch_size=10,
        max_docs=20,
    )

    assert [hit["_id"] for hit in claimed] == ["raw-1"]
    assert captured["actions"][0]["script"]["params"]["expected_status"] == "pending"
    assert client.indices.refreshed == ["raw-index"]
