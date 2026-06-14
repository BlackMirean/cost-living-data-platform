"""Application settings for the cost-of-living data platform."""

import os
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


def _load_fission_mounted_settings() -> None:
    """Expose Fission-mounted ConfigMap and Secret files as environment variables."""
    explicit_env = set(os.environ)
    for base_dir in (Path("/configs"), Path("/secrets")):
        if not base_dir.is_dir():
            continue
        for file_path in sorted(path for path in base_dir.rglob("*") if path.is_file()):
            key = file_path.name
            if not key or key in explicit_env:
                continue
            try:
                os.environ[key] = file_path.read_text(encoding="utf-8").strip()
            except OSError:
                continue


_load_fission_mounted_settings()


class Settings(BaseSettings):
    """Environment-backed settings used by harvesters and the API."""

    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_username: str = ""
    elasticsearch_password: str = ""
    elasticsearch_verify_certs: bool = True
    posts_index: str = "cost_living_posts_current"
    processed_posts_write_index: str = "cost_living_processed_posts_write"
    curated_posts_index: str = "cost_living_processed_posts_write"
    posts_current_alias: str = "cost_living_posts_current"
    raw_posts_index: str = "cost_living_raw_posts"
    indicators_index: str = "cost_living_indicators"
    monthly_metrics_index: str = "cost_living_monthly_topic_metrics"
    api_title: str = "Cost-of-Living Data Platform API"
    cost_of_living_queries: str = (
        "cost of living Australia,rental crisis Australia,rent increase Australia,"
        "mortgage stress Australia,housing affordability Australia,electricity bills Australia,"
        "energy bills Australia,petrol prices Australia,grocery prices Australia,"
        "supermarket prices Australia,food prices Australia,inflation Australia,"
        "interest rates Australia,real wages Australia,auspol cost of living,"
        "Melbourne rent,Sydney rent,Perth rent"
    )

    abs_data_api_base_url: str = "https://data.api.abs.gov.au/rest"
    abs_cpi_dataflow: str = "ABS,CPI,2.0.0"
    abs_cpi_data_key: str = "1+3..10.50.M"
    abs_cpi_last_n_observations: int = 60
    bluesky_stream_base_url: str = "https://bsky.social"
    bluesky_stream_handle: str = ""
    bluesky_stream_app_password: str = ""
    bluesky_stream_raw_index: str = "cost_living_bluesky_raw_stream"

    mastodon_stream_raw_index: str = "cost_living_mastodon_raw_stream"
    mastodon_au_base_url: str = "https://mastodon.au"
    mastodon_au_access_token: str = ""
    mastodon_social_base_url: str = "https://mastodon.social"
    mastodon_social_access_token: str = ""
    aus_social_base_url: str = "https://aus.social"
    aus_social_access_token: str = ""

    social_harvest_state_index: str = "cost_living_harvest_state"
    social_stream_initial_lookback_days: int = 1095
    social_stream_first_run_max_pages: int = 0
    social_stream_incremental_max_pages: int = 0
    gdelt_gkg_masterfilelist_url: str = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
    gdelt_gkg_masterfilelist_timeout_seconds: float = 180.0
    gdelt_gkg_raw_index: str = "cost_living_gdelt_raw_stream"
    gdelt_gkg_initial_lookback_hours: float = 3.0
    gdelt_gkg_incremental_batch_size: int = 2
    gdelt_gkg_incremental_max_runtime_seconds: float = 180.0
    gdelt_gkg_download_timeout_seconds: float = 180.0
    gdelt_gkg_bulk_size: int = 500
    gdelt_gkg_backfill_months: int = 3
    gdelt_gkg_backfill_max_archives: int = 0
    gdelt_gkg_backfill_request_delay_seconds: float = 2.0
    gdelt_gkg_backfill_checkpoint_path: str = "data/backfill_state/gdelt_gkg_backfill.json"
    unified_raw_sync_lookback_hours: float = 2.0
    unified_raw_sync_scan_size: int = 1000
    unified_raw_sync_bulk_size: int = 1000
    unified_raw_sync_max_text_length: int = 10000

    nlp_batch_size: int = 250
    nlp_max_docs_per_run: int = 1000
    nlp_bulk_size: int = 500
    nlp_processing_stale_minutes: int = 120
    nlp_queue_name: str = "nlp"
    nlp_queue_max_messages_per_sync: int = 50
    nlp_queue_worker_block_timeout_seconds: int = 10
    nlp_queue_worker_idle_exit_seconds: int = 60
    nlp_queue_worker_max_messages: int = 0
    nlp_queue_max_attempts: int = 3
    nlp_queue_dead_letter_name: str = "nlp:dead-letter"
    api_cache_ttl_seconds: float = 30.0
    api_rate_limit_enabled: bool = True
    api_rate_limit_requests_per_minute: int = 600
    api_rate_limit_window_seconds: int = 60
    otel_enabled: bool = True
    otel_service_name: str = "cost-living-platform-api"
    otel_exporter_otlp_endpoint: str = ""
    otel_console_exporter_enabled: bool = False
    redis_enabled: bool = False
    redis_url: str = "redis://localhost:6379/0"
    redis_queue_prefix: str = "cost_living_pipeline"
    redis_lock_ttl_seconds: int = 900
    redis_event_ttl_seconds: int = 86400
    redis_event_queue_max_length: int = 500

    @property
    def cost_of_living_query_list(self) -> list[str]:
        return [item.strip() for item in self.cost_of_living_queries.split(",") if item.strip()]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
