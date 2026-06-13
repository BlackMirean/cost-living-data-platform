"""Current platform plugins used by the public pipeline."""

from __future__ import annotations

from backend.platforms.base import PlatformPlugin


PLATFORM_PLUGINS: tuple[PlatformPlugin, ...] = (
    PlatformPlugin(
        name="bluesky",
        source_group="social",
        raw_index_setting="bluesky_stream_raw_index",
        platform_raw_index="cost_living_bluesky_raw_stream",
        source_label="bluesky_raw_stream",
        fission_handlers=("backend.fission_handlers.cost_living_platform_harvest_bluesky",),
        schedules=("0,15,30,45 * * * *",),
        description="Public Bluesky search harvester with Australian context filtering.",
        has_engagement_metrics=True,
    ),
    PlatformPlugin(
        name="mastodon",
        source_group="social",
        raw_index_setting="mastodon_stream_raw_index",
        platform_raw_index="cost_living_mastodon_raw_stream",
        source_label="mastodon_raw_stream",
        fission_handlers=(
            "backend.fission_handlers.cost_living_platform_harvest_mastodon_au",
            "backend.fission_handlers.cost_living_platform_harvest_mastodon_social",
            "backend.fission_handlers.cost_living_platform_harvest_aus_social",
        ),
        schedules=(
            "1,16,31,46 * * * *",
            "2,17,32,47 * * * *",
            "3,18,33,48 * * * *",
        ),
        description="Public Mastodon harvesters for mastodon.au, mastodon.social and aus.social.",
        has_engagement_metrics=True,
    ),
    PlatformPlugin(
        name="gdelt",
        source_group="media",
        raw_index_setting="gdelt_gkg_raw_index",
        platform_raw_index="cost_living_gdelt_raw_stream",
        source_label="gdelt_gkg_raw",
        fission_handlers=("backend.fission_handlers.cost_living_platform_harvest_gdelt",),
        schedules=("4,19,34,49 * * * *",),
        description="GDELT GKG media-attention harvester for cost-of-living coverage.",
        has_engagement_metrics=False,
    ),
)


def get_platform_plugin(name: str) -> PlatformPlugin | None:
    key = str(name or "").casefold()
    return next((plugin for plugin in PLATFORM_PLUGINS if plugin.name == key), None)
