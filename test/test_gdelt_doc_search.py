from backend.harvesters.gdelt_doc_search import (
    build_gdelt_query,
    is_gdelt_article_in_scope,
    parse_gdelt_date,
    quote_gdelt_term,
    raw_gdelt_article,
    search_gdelt_articles,
)


def test_quote_gdelt_term_quotes_phrases():
    assert quote_gdelt_term("cost of living") == '"cost of living"'
    assert quote_gdelt_term("CPI") == "CPI"


def test_build_gdelt_query_is_australia_focused():
    query = build_gdelt_query("rent increase")
    assert '"rent increase"' in query
    assert "sourcecountry:australia" in query
    assert "sourcelang:english" in query


def test_parse_gdelt_date_compact_timestamp():
    assert parse_gdelt_date("20260506123000") == "2026-05-06T12:30:00+00:00"


def test_raw_gdelt_article():
    article = {
        "url": "https://example.com/story",
        "title": "Australian renters face mortgage stress and rising prices",
        "seendate": "20260506123000",
        "domain": "example.com",
        "language": "English",
        "sourcecountry": "AU",
    }
    doc = raw_gdelt_article(article, query="rent increase")
    assert doc["platform"] == "gdelt"
    assert doc["source"] == "gdelt_doc_api"
    assert doc["created_at"] == "2026-05-06T12:30:00+00:00"
    assert doc["analysis_status"] == "pending"
    assert doc["text"] == "Australian renters face mortgage stress and rising prices"
    assert doc["payload"] == article


def test_is_gdelt_article_in_scope_requires_australian_english_source():
    assert is_gdelt_article_in_scope({"language": "English", "sourcecountry": "Australia"})
    assert not is_gdelt_article_in_scope({"language": "Chinese", "sourcecountry": "Australia"})
    assert not is_gdelt_article_in_scope({"language": "English", "sourcecountry": "United States"})


def test_search_gdelt_articles_uses_datetime_window(monkeypatch):
    captured = {}

    class FakeResponse:
        status_code = 200
        headers = {"content-type": "application/json"}
        text = ""

        def raise_for_status(self):
            return None

        def json(self):
            return {"articles": [{"title": "Australian renters face rising costs"}]}

    def fake_get(url, params, timeout):
        captured["url"] = url
        captured["params"] = params
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr("backend.harvesters.gdelt_doc_search.requests.get", fake_get)

    articles = search_gdelt_articles(
        query="rent increase",
        limit=999,
        start_datetime="20260501000000",
        end_datetime="20260508000000",
        retries=0,
    )

    assert len(articles) == 1
    assert captured["params"]["startdatetime"] == "20260501000000"
    assert captured["params"]["enddatetime"] == "20260508000000"
    assert "timespan" not in captured["params"]
    assert captured["params"]["maxrecords"] == 250
