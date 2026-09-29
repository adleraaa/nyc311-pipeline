"""Socrata (SODA 2.x) client for the NYC 311 dataset.

Incremental extraction uses keyset pagination on the pair
(:updated_at, unique_key) instead of $offset. The source publishes updates in
daily batches where tens of thousands of rows share one :updated_at value, so a
watermark on :updated_at alone cannot tell where a page ended; the unique_key
tiebreaker makes every position in the ordered stream unambiguous.
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DATASET_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"

# Only the columns the models use; the full row has ~45 columns and most are
# address details we never read. Fewer columns = smaller, politer requests.
SOURCE_COLUMNS = [
    ":id",
    ":created_at",
    ":updated_at",
    "unique_key",
    "created_date",
    "closed_date",
    "agency",
    "agency_name",
    "complaint_type",
    "descriptor",
    "status",
    "resolution_action_updated_date",
    "borough",
    "incident_zip",
    "open_data_channel_type",
    "latitude",
    "longitude",
]


@dataclass(frozen=True)
class Watermark:
    """Position in the (:updated_at, unique_key) ordered stream."""

    updated_at: str  # ISO-8601 UTC as returned by Socrata, e.g. 2026-09-28T01:33:33.096Z
    unique_key: str


def build_where(window_start: str, after: Watermark | None) -> str:
    """SoQL $where clause: rows created inside the window, strictly after the watermark."""
    clauses = [f"created_date >= '{window_start}'"]
    if after is not None:
        # SoQL literals are single-quoted; both values come from the API itself
        # (timestamps and numeric keys), so they never contain quotes.
        clauses.append(
            f"(:updated_at > '{after.updated_at}' OR "
            f"(:updated_at = '{after.updated_at}' AND unique_key > '{after.unique_key}'))"
        )
    return " AND ".join(clauses)


def make_session(app_token: str | None = None) -> requests.Session:
    """HTTP session with retries on throttling and transient server errors."""
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    token = app_token or os.environ.get("SOCRATA_APP_TOKEN")
    if token:
        # Optional: an app token raises the anonymous rate limit.
        session.headers["X-App-Token"] = token
    return session


def fetch_pages(
    session: requests.Session,
    window_start: str,
    after: Watermark | None,
    page_size: int = 20_000,
    url: str = DATASET_URL,
    timeout: float = 180.0,
) -> Iterator[tuple[list[dict], Watermark]]:
    """Yield (rows, watermark_of_last_row) pages until the stream is exhausted.

    The caller persists the returned watermark after each page is loaded, so an
    interrupted run resumes from the last committed page.
    """
    cursor = after
    while True:
        params = {
            "$select": ",".join(SOURCE_COLUMNS),
            "$where": build_where(window_start, cursor),
            "$order": ":updated_at, unique_key",
            "$limit": str(page_size),
        }
        response = session.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        rows = response.json()
        if not rows:
            return
        last = rows[-1]
        cursor = Watermark(updated_at=last[":updated_at"], unique_key=last["unique_key"])
        yield rows, cursor
        if len(rows) < page_size:
            return
