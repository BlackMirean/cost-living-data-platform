"""ABS Monthly CPI harvester for the cost-of-living scenario."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
from datetime import datetime, timezone
from typing import Any

import requests

from backend.common.config import settings
from backend.common.document_store import index_indicators, use_local_store


UTC = timezone.utc


CSV_ACCEPT = "application/vnd.sdmx.data+csv;labels=both"


def labelled_value_parts(value: str) -> tuple[str, str]:
    """Split ABS labelled CSV values like '10001: All groups CPI'."""

    if ": " not in value:
        return value.strip(), value.strip()
    code, label = value.split(": ", 1)
    return code.strip(), label.strip().strip('"')


def find_column(row: dict[str, str], prefix: str) -> str:
    """Find an ABS CSV column where labels may be appended after a colon."""

    for key in row:
        if key == prefix or key.startswith(f"{prefix}:"):
            return key
    raise KeyError(f"Column '{prefix}' was not found in ABS CPI response")


def optional_column(row: dict[str, str], prefix: str) -> str | None:
    """Find an optional ABS CSV column where labels may be appended."""

    for key in row:
        if key == prefix or key.startswith(f"{prefix}:"):
            return key
    return None


def period_to_start(period: str) -> str:
    """Convert an ABS period such as 2025-09 into an ISO date."""

    if len(period) == 7 and period[4] == "-":
        return f"{period}-01T00:00:00+00:00"
    if "-Q" in period:
        year, quarter = period.split("-Q", 1)
        month = {"1": "01", "2": "04", "3": "07", "4": "10"}[quarter]
        return f"{year}-{month}-01T00:00:00+00:00"
    return f"{period}-01-01T00:00:00+00:00"


def fetch_cpi_csv(last_n_observations: int | None = None, data_key: str | None = None) -> str:
    """Fetch labelled monthly CPI CSV from the ABS Data API."""

    base_url = settings.abs_data_api_base_url.rstrip("/")
    dataflow = settings.abs_cpi_dataflow
    key = data_key or settings.abs_cpi_data_key
    last_n = last_n_observations or settings.abs_cpi_last_n_observations
    url = f"{base_url}/data/{dataflow}/{key}"
    response = requests.get(
        url,
        params={
            "lastNObservations": last_n,
            "detail": "dataonly",
            "dimensionAtObservation": "AllDimensions",
        },
        headers={
            "Accept": CSV_ACCEPT,
            "User-Agent": "cost-of-living-data-platform/0.1",
        },
        timeout=60,
    )
    response.raise_for_status()
    return response.text


def parse_cpi_csv(csv_text: str) -> list[dict[str, Any]]:
    """Parse ABS CPI CSV into normalised official indicator documents."""

    harvested_at = datetime.now(tz=UTC).replace(microsecond=0).isoformat()
    reader = csv.DictReader(io.StringIO(csv_text))
    docs: list[dict[str, Any]] = []
    for row in reader:
        measure_col = find_column(row, "MEASURE")
        index_col = find_column(row, "INDEX")
        region_col = find_column(row, "REGION")
        frequency_col = find_column(row, "FREQ")
        period_col = find_column(row, "TIME_PERIOD")
        adjustment_col = optional_column(row, "TSEST")
        unit_col = optional_column(row, "UNIT_MEASURE")
        value = row.get("OBS_VALUE")
        if value in {None, ""}:
            continue

        measure_code, measure_name = labelled_value_parts(row[measure_col])
        item_code, item_name = labelled_value_parts(row[index_col])
        region_code, region_name = labelled_value_parts(row[region_col])
        frequency_code, frequency_name = labelled_value_parts(row[frequency_col])
        adjustment_code, adjustment_name = (
            labelled_value_parts(row[adjustment_col]) if adjustment_col else ("", "")
        )
        unit_code, unit_name = labelled_value_parts(row[unit_col]) if unit_col and row[unit_col] else ("", "")
        period = row[period_col]
        dataflow = row.get("DATAFLOW", "")
        series_key = ".".join(
            part for part in [measure_code, item_code, adjustment_code, region_code, frequency_code] if part
        )
        doc_id = hashlib.sha1(
            (
                f"abs-cpi|{dataflow}|{measure_code}|{item_code}|"
                f"{adjustment_code}|{region_code}|{frequency_code}|{period}"
            ).encode("utf-8")
        ).hexdigest()

        docs.append(
            {
                "id": f"abs-cpi-{doc_id}",
                "source": "abs_data_api",
                "dataflow": dataflow,
                "indicator": "monthly_cpi",
                "series_key": series_key,
                "measure_code": measure_code,
                "measure": measure_name,
                "item_code": item_code,
                "item_name": item_name,
                "adjustment_code": adjustment_code,
                "adjustment": adjustment_name,
                "region_code": region_code,
                "region": region_name,
                "frequency_code": frequency_code,
                "frequency": frequency_name or frequency_code,
                "period": period,
                "period_start": period_to_start(period),
                "value": float(value),
                "unit_code": unit_code,
                "unit": unit_name or unit_code,
                "created_at": period_to_start(period),
                "harvested_at": harvested_at,
                "notes": f"ABS CPI measure {measure_code}, region {region_code}",
                "raw_row": dict(row),
            }
        )
    return docs


def harvest_abs_cpi(reset: bool = False, last_n_observations: int | None = None) -> int:
    docs = parse_cpi_csv(fetch_cpi_csv(last_n_observations=last_n_observations))
    return index_indicators(docs, reset=reset)


def main() -> None:
    parser = argparse.ArgumentParser(description="Harvest ABS monthly CPI data.")
    parser.add_argument("--reset", action="store_true", help="Delete and recreate indicator data first.")
    parser.add_argument("--last-n", type=int, default=None, help="Number of recent observations per series.")
    args = parser.parse_args()

    count = harvest_abs_cpi(reset=args.reset, last_n_observations=args.last_n)
    target = "local memory store" if use_local_store() else f"index '{settings.indicators_index}'"
    print(f"Indexed {count} ABS CPI indicator documents into {target}.")


if __name__ == "__main__":
    main()
