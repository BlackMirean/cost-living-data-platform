from backend.common import source_registry


def test_source_registry_groups_supported_sources():
    assert source_registry.source_group_for_platform("bluesky") == "social"
    assert source_registry.source_group_for_platform("mastodon") == "social"
    assert source_registry.source_group_for_platform("gdelt") == "media"
    assert source_registry.source_group_for_platform("unknown") == "unknown"


def test_source_registry_configured_indices_use_settings_object():
    class DummySettings:
        bluesky_stream_raw_index = "bluesky-index"
        mastodon_stream_raw_index = "mastodon-index"
        gdelt_gkg_raw_index = "gdelt-index"

    assert source_registry.configured_source_indices(DummySettings) == {
        "bluesky": "bluesky-index",
        "mastodon": "mastodon-index",
        "gdelt": "gdelt-index",
    }


def test_source_registry_platform_indices_are_optimized_indices():
    assert source_registry.platform_source_indices() == {
        "bluesky": "cost_living_bluesky_raw_stream",
        "mastodon": "cost_living_mastodon_raw_stream",
        "gdelt": "cost_living_gdelt_raw_stream",
    }


def test_source_registry_group_mapping_matches_dashboard_contract():
    assert source_registry.source_group_platforms() == {
        "social": ["bluesky", "mastodon"],
        "media": ["gdelt"],
    }
    assert source_registry.social_platforms() == {"bluesky", "mastodon"}
