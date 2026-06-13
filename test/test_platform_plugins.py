from backend.common import source_registry
from backend.platforms.plugins import PLATFORM_PLUGINS, get_platform_plugin


def test_current_platform_plugins_are_explicit():
    assert [plugin.name for plugin in PLATFORM_PLUGINS] == ["bluesky", "mastodon", "gdelt"]
    assert get_platform_plugin("bluesky").source_group == "social"
    assert len(get_platform_plugin("mastodon").fission_handlers) == 3
    assert get_platform_plugin("gdelt").source_group == "media"
    assert get_platform_plugin("unknown") is None


def test_platform_plugin_metadata_reports_index_isolation():
    class DummySettings:
        bluesky_stream_raw_index = "cost_living_bluesky_raw_stream"
        mastodon_stream_raw_index = "cost_living_mastodon_raw_stream"
        gdelt_gkg_raw_index = "wrong-index"

    metadata = source_registry.platform_plugin_metadata(DummySettings)

    assert metadata["count"] == 3
    assert metadata["groups"] == {"social": ["bluesky", "mastodon"], "media": ["gdelt"]}
    assert metadata["index_mismatches"] == {
        "gdelt": {
            "expected": "cost_living_gdelt_raw_stream",
            "configured": "wrong-index",
        }
    }
    gdelt = next(plugin for plugin in metadata["plugins"] if plugin["name"] == "gdelt")
    assert gdelt["index_isolation_ok"] is False
