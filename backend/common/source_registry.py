"""Source registry for the cost-of-living ingestion pipeline.

This module keeps source metadata in one place. It is intentionally small:
adding a source still requires a harvester and deployment resources, but the
raw integrator, NLP worker and API source grouping can share this registry.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from backend.common.config import settings


@dataclass(frozen=True)
class SourceDefinition:
    """Static metadata for one supported data source."""

    name: str
    source_group: str
    raw_index_setting: str
    platform_raw_index: str
    source_label: str
    has_engagement_metrics: bool = False


SOURCE_REGISTRY: dict[str, SourceDefinition] = {
    "bluesky": SourceDefinition(
        name="bluesky",
        source_group="social",
        raw_index_setting="bluesky_stream_raw_index",
        platform_raw_index="cost_living_bluesky_raw_stream",
        source_label="bluesky_raw_stream",
        has_engagement_metrics=True,
    ),
    "mastodon": SourceDefinition(
        name="mastodon",
        source_group="social",
        raw_index_setting="mastodon_stream_raw_index",
        platform_raw_index="cost_living_mastodon_raw_stream",
        source_label="mastodon_raw_stream",
        has_engagement_metrics=True,
    ),
    "gdelt": SourceDefinition(
        name="gdelt",
        source_group="media",
        raw_index_setting="gdelt_gkg_raw_index",
        platform_raw_index="cost_living_gdelt_raw_stream",
        source_label="gdelt_gkg_raw",
        has_engagement_metrics=False,
    ),
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
