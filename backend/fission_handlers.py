"""Fission entry points for scheduled data jobs."""

from __future__ import annotations

from backend.harvesters.abs_cpi_harvester import harvest_abs_cpi
from backend.harvesters.streams.bluesky import harvest_bluesky_stream as run_bluesky_stream
from backend.harvesters.streams.gdelt_gkg import harvest_gdelt_gkg_stream as run_gdelt_gkg_stream
from backend.harvesters.streams.mastodon import (
    harvest_aus_social_stream as run_aus_social_stream,
    harvest_mastodon_au_stream as run_mastodon_au_stream,
    harvest_mastodon_social_stream as run_mastodon_social_stream,
)
from backend.harvesters.unified_raw_sync import sync_platform_raw_posts, sync_unified_raw_posts
from backend.processing.nlp_worker import process_batch as process_nlp_batch
from backend.common.runtime_queue import run_pipeline_job


def harvest_cpi() -> dict:
    count = harvest_abs_cpi(reset=False)
    return {"source": "abs_cpi", "indexed": count}


def process_raw_posts() -> dict:
    counts = process_nlp_batch()
    return {"source": "nlp_worker", **counts}


def harvest_bluesky_stream() -> dict:
    return run_bluesky_stream()


def harvest_mastodon_au_stream() -> dict:
    return run_mastodon_au_stream()


def harvest_mastodon_social_stream() -> dict:
    return run_mastodon_social_stream()


def harvest_aus_social_stream() -> dict:
    return run_aus_social_stream()


def harvest_gdelt_gkg_stream() -> dict:
    return run_gdelt_gkg_stream()


def sync_raw_streams() -> dict:
    return sync_unified_raw_posts()


def cost_living_platform_harvest_bluesky() -> dict:
    from backend.harvesters.streams import bluesky

    job_name = "cost-living-platform-bluesky-harvester"
    bluesky.FUNCTION_NAME = job_name
    return run_pipeline_job(job_name, bluesky.main)


def cost_living_platform_harvest_mastodon_au() -> dict:
    from backend.harvesters.streams import mastodon

    job_name = "cost-living-platform-mastodon-au-harvester"
    mastodon.configure_stream("mastodon_au")
    mastodon.FUNCTION_NAME = job_name
    return run_pipeline_job(job_name, mastodon.main)


def cost_living_platform_harvest_mastodon_social() -> dict:
    from backend.harvesters.streams import mastodon

    job_name = "cost-living-platform-mastodon-social-harvester"
    mastodon.configure_stream("mastodon_social")
    mastodon.FUNCTION_NAME = job_name
    return run_pipeline_job(job_name, mastodon.main)


def cost_living_platform_harvest_aus_social() -> dict:
    from backend.harvesters.streams import mastodon

    job_name = "cost-living-platform-aus-social-harvester"
    mastodon.configure_stream("aus_social")
    mastodon.FUNCTION_NAME = job_name
    return run_pipeline_job(job_name, mastodon.main)


def cost_living_platform_harvest_gdelt() -> dict:
    from backend.harvesters.streams import gdelt_gkg

    job_name = "cost-living-platform-gdelt-harvester"
    gdelt_gkg.FUNCTION_NAME = job_name
    return run_pipeline_job(job_name, gdelt_gkg.main)


def cost_living_platform_sync_raw() -> dict:
    job_name = "cost-living-platform-raw-integrator"
    return run_pipeline_job(
        job_name,
        lambda: {"source": "cost_living_platform_raw_integrator", **sync_platform_raw_posts()},
    )


def cost_living_platform_process_nlp() -> dict:
    job_name = "cost-living-platform-nlp-processor"
    return run_pipeline_job(
        job_name,
        lambda: {"source": "cost_living_platform_nlp_processor", **process_nlp_batch()},
    )


def cost_living_platform_harvest_cpi() -> dict:
    job_name = "cost-living-platform-official-indicators"
    return run_pipeline_job(
        job_name,
        lambda: {"source": "cost_living_platform_official_indicators", "indexed": harvest_abs_cpi(reset=False)},
    )
