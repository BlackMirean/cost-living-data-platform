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
    processed_posts_write_index: str = "cost_living_processed_posts"
    curated_posts_index: str = "cost_living_processed_posts"
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
    gdelt_gkg_queue_index: str = "cost_living_gdelt_gkg_file_queue"
    gdelt_gkg_raw_index: str = "cost_living_gdelt_raw_stream"
    gdelt_gkg_initial_lookback_hours: float = 3.0
    unified_raw_sync_lookback_hours: float = 2.0
    unified_raw_sync_scan_size: int = 1000
    unified_raw_sync_bulk_size: int = 1000
    unified_raw_sync_max_text_length: int = 10000
    unified_raw_sync_require_platform_indices: bool = False

    gdelt_doc_api_url: str = "https://api.gdeltproject.org/api/v2/doc/doc"
    gdelt_queries: str = (
        "cost of living,rental crisis,rent increase,mortgage stress,"
        "housing affordability,electricity prices,energy bills,petrol prices,"
        "fuel prices,grocery prices,food prices,supermarket prices,inflation,"
        "real wages,interest rates"
    )
    gdelt_source_country: str = "australia"
    gdelt_source_language: str = "english"
    gdelt_timespan: str = "3months"
    gdelt_limit: int = 15
    gdelt_request_delay_seconds: float = 6.0
    gdelt_request_timeout_seconds: float = 20.0
    gdelt_retries: int = 2
    gdelt_max_runtime_seconds: float = 240.0
    gdelt_backfill_months: int = 3
    gdelt_backfill_window_days: int = 1
    gdelt_backfill_limit: int = 250
    gdelt_backfill_request_delay_seconds: float = 20.0
    gdelt_backfill_request_timeout_seconds: float = 30.0
    gdelt_backfill_retries: int = 3
    gdelt_backfill_max_windows: int = 0
    gdelt_backfill_max_requests: int = 0
    gdelt_backfill_state_path: str = "data/backfill_state/gdelt_backfill.json"
    nlp_batch_size: int = 250
    nlp_max_docs_per_run: int = 1000
    nlp_bulk_size: int = 500
    nlp_processing_stale_minutes: int = 120
    api_cache_ttl_seconds: float = 30.0

    @property
    def cost_of_living_query_list(self) -> list[str]:
        return [item.strip() for item in self.cost_of_living_queries.split(",") if item.strip()]

    @property
    def gdelt_query_list(self) -> list[str]:
        return [item.strip() for item in self.gdelt_queries.split(",") if item.strip()]

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


settings = Settings()
