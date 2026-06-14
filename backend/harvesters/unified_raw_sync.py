"""Fission-friendly wrapper for syncing upstream raw indexes into unified raw posts."""

from __future__ import annotations

from argparse import Namespace
from typing import Any

from backend.common.config import settings
from backend.common.source_registry import (
    configured_source_indices as registry_configured_source_indices,
    platform_index_mismatches,
    platform_source_indices,
    source_names,
)
from scripts.import_raw_streams import run_import


PLATFORM_SOURCE_INDICES = platform_source_indices()


def configured_source_indices() -> dict[str, str]:
    return registry_configured_source_indices()


def validate_platform_source_indices() -> None:
    mismatches = platform_index_mismatches()
    if mismatches:
        raise RuntimeError(
            "Platform raw integrator is not isolated to the cost-living stream indices: "
            + str(mismatches)
        )


def _run_raw_sync() -> dict[str, Any]:
    """Incrementally copy recent upstream raw documents into the unified raw index."""

    validate_platform_source_indices()

    args = Namespace(
        sources=source_names(),
        target_index=settings.raw_posts_index,
        limit_per_index=0,
        start_date=None,
        end_date=None,
        lookback_hours=settings.unified_raw_sync_lookback_hours,
        strict_filter=False,
        scan_size=settings.unified_raw_sync_scan_size,
        bulk_size=settings.unified_raw_sync_bulk_size,
        sample_size=3,
        max_text_length=settings.unified_raw_sync_max_text_length,
        reset_target=False,
        write=True,
    )
    result = run_import(args)
    result["isolation_mode"] = "platform_only"
    result["configured_source_indices"] = configured_source_indices()
    result["required_platform_source_indices"] = PLATFORM_SOURCE_INDICES
    return result


def sync_platform_raw_posts() -> dict[str, Any]:
    return _run_raw_sync()
