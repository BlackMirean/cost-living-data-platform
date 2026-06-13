"""Source registry for the cost-of-living ingestion pipeline.

This module keeps source metadata in one place. It is intentionally small:
adding a source still requires a harvester and deployment resources, but the
raw integrator, NLP worker and API source grouping can share this registry.
"""

from __future__ import annotations

from typing import Any

from backend.common.config import settings
from backend.platforms.base import PlatformPlugin
from backend.platforms.plugins import PLATFORM_PLUGINS


SourceDefinition = PlatformPlugin


SOURCE_REGISTRY: dict[str, SourceDefinition] = {
    plugin.name: plugin for plugin in PLATFORM_PLUGINS
}


def source_names() -> list[str]:
    return list(SOURCE_REGISTRY)


def source_choices_text() -> str:
    return ", ".join(source_names())


def get_source(name: str) -> SourceDefinition | None:
    return SOURCE_REGISTRY.get(str(name or "").casefold())


def source_group_for_platform(platform: Any) -> str:
    source = get_source(str(platform or "").casefold())
    return source.source_group if source else "unknown"


def source_group_platforms() -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {}
    for source in SOURCE_REGISTRY.values():
        groups.setdefault(source.source_group, []).append(source.name)
    return groups


def social_platforms() -> set[str]:
    return {
        source.name
        for source in SOURCE_REGISTRY.values()
        if source.has_engagement_metrics
    }


def source_labels() -> dict[str, str]:
    return {name: source.source_label for name, source in SOURCE_REGISTRY.items()}


def platform_source_indices() -> dict[str, str]:
    return {name: source.platform_raw_index for name, source in SOURCE_REGISTRY.items()}


def configured_source_indices(settings_obj: Any = settings) -> dict[str, str]:
    return {
        name: getattr(settings_obj, source.raw_index_setting)
        for name, source in SOURCE_REGISTRY.items()
    }


def platform_index_mismatches(settings_obj: Any = settings) -> dict[str, dict[str, str | None]]:
    configured = configured_source_indices(settings_obj)
    return {
        name: {
            "expected": source.platform_raw_index,
            "configured": configured.get(name),
        }
        for name, source in SOURCE_REGISTRY.items()
        if configured.get(name) != source.platform_raw_index
    }


def platform_plugin_metadata(settings_obj: Any = settings) -> dict[str, Any]:
    plugins = [plugin.public_metadata(settings_obj) for plugin in SOURCE_REGISTRY.values()]
    return {
        "plugins": plugins,
        "count": len(plugins),
        "groups": source_group_platforms(),
        "index_mismatches": platform_index_mismatches(settings_obj),
    }
