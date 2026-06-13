"""Typed metadata for source platform plugins."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PlatformPlugin:
    """Metadata required to wire one source into the ingestion pipeline."""

    name: str
    source_group: str
    raw_index_setting: str
    platform_raw_index: str
    source_label: str
    fission_handlers: tuple[str, ...]
    schedules: tuple[str, ...]
    description: str
    has_engagement_metrics: bool = False

    def configured_raw_index(self, settings_obj: Any) -> str:
        return getattr(settings_obj, self.raw_index_setting)

    def public_metadata(self, settings_obj: Any) -> dict[str, Any]:
        configured = self.configured_raw_index(settings_obj)
        return {
            "name": self.name,
            "source_group": self.source_group,
            "configured_raw_index": configured,
            "platform_raw_index": self.platform_raw_index,
            "source_label": self.source_label,
            "fission_handlers": list(self.fission_handlers),
            "schedules": list(self.schedules),
            "description": self.description,
            "has_engagement_metrics": self.has_engagement_metrics,
            "index_isolation_ok": configured == self.platform_raw_index,
        }
