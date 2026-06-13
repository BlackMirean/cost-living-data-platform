from backend.analytics.topics import (
    classify_cost_of_living_topic,
    has_australia_context,
    is_relevant_cost_of_living_text,
    matched_keywords,
)


def test_classify_cost_of_living_topic():
    assert classify_cost_of_living_topic("My rent is too high") == "housing"
    assert classify_cost_of_living_topic("Electricity bills are painful") == "energy"
    assert classify_cost_of_living_topic("HECS indexation is painful") == "debt"
    assert classify_cost_of_living_topic("Bulk billing gap fees keep rising") == "healthcare"


def test_matched_keywords():
    assert "groceries" in matched_keywords("Groceries are more expensive")


def test_relevance_filter_rejects_known_noise():
    assert not is_relevant_cost_of_living_text("Rent free in my head", topic_hint="housing")


def test_relevance_filter_keeps_australian_cost_story():
    text = "Australian renters face rising prices and mortgage stress in Melbourne"
    assert is_relevant_cost_of_living_text(text, topic_hint="housing")


def test_relevance_filter_falls_back_to_text_topic_when_query_hint_misses():
    text = "Flexible work in demand as employees try to dodge rising fuel costs"
    assert is_relevant_cost_of_living_text(text, topic_hint="groceries")


def test_relevance_filter_rejects_foreign_fuel_context_without_australia():
    assert not is_relevant_cost_of_living_text(
        "Europe has maybe six weeks of jet fuel left",
        topic_hint="fuel",
    )


def test_relevance_filter_rejects_budget_travel_false_positive():
    assert not is_relevant_cost_of_living_text(
        "Best budget RV and motorhome destinations in Australia",
        topic_hint="housing",
        trust_topic_hint=True,
    )


def test_relevance_filter_rejects_customer_awards_false_positive():
    assert not is_relevant_cost_of_living_text(
        "Aldi, Bunnings and Chemist Warehouse win top customer awards",
        topic_hint=None,
        trust_topic_hint=True,
    )


def test_relevance_filter_does_not_match_fuel_inside_fuels():
    assert not is_relevant_cost_of_living_text(
        "Iran war fuels good news for Ampol and business shares",
        topic_hint=None,
        trust_topic_hint=True,
    )


def test_has_australia_context_detects_local_signals():
    assert has_australia_context("Perth households face higher grocery prices")
    assert has_australia_context("Woolworths shoppers are angry about prices")
    assert not has_australia_context("US consumer sentiment falls on inflation fears")
    assert not has_australia_context("Inflation Reduction Act reaction gets worse")
