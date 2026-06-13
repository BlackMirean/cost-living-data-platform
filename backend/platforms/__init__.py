"""Platform plugin catalog for supported data sources."""

from backend.platforms.plugins import PLATFORM_PLUGINS, get_platform_plugin

__all__ = ["PLATFORM_PLUGINS", "get_platform_plugin"]
