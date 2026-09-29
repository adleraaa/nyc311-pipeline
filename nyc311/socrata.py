"""Socrata (SODA 2.x) client for the NYC 311 dataset.

Incremental extraction uses keyset pagination on the pair
(:updated_at, unique_key) instead of $offset. The source publishes updates in
daily batches where tens of thousands of rows share one :updated_at value, so a
watermark on :updated_at alone cannot tell where a page ended; the unique_key
tiebreaker makes every position in the ordered stream unambiguous.

Reads are served by replicas. While the daily update rolls out (observed on
2026-09-29: batch stamped 01:33 UTC, first served around 04:05 UTC), most
replicas still hold the previous version and say so in the
X-SODA2-Data-Out-Of-Date response header. `get_fresh` refuses those answers.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Iterator
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DATASET_URL = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
STALE_HEADER = "X-SODA2-Data-Out-Of-Date"
STALE_ATTEMPTS = 6
STALE_WAIT_SECONDS = 10.0

log = logging.getLogger(__name__)

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


class StaleReplicaError(RuntimeError):
    """Every attempt was answered by a replica that is behind the source of truth."""


def get_fresh(
    session: requests.Session, url: str, params: dict, timeout: float
) -> requests.Response:
    """GET that only accepts answers from an up-to-date replica.

    A keyset query answered by a stale replica returns nothing past the
    watermark, which is indistinguishable from "no new data" unless the header
    is checked. Retrying after a short wait usually reaches a fresh replica.
    """
    for attempt in range(1, STALE_ATTEMPTS + 1):
        response = session.get(url, params=params, timeout=timeout)
        response.raise_for_status()
        if response.headers.get(STALE_HEADER, "").lower() != "true":
            return response
        log.warning(
            "stale replica (truth last modified %s), attempt %d of %d",
            response.headers.get("X-SODA2-Truth-Last-Modified"),
            attempt,
            STALE_ATTEMPTS,
        )
        if attempt < STALE_ATTEMPTS:
            time.sleep(STALE_WAIT_SECONDS)
    raise StaleReplicaError(f"{STALE_ATTEMPTS} responses in a row came from a stale replica")


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
    interrupted run resumes from the last committed page. Raises
    StaleReplicaError if the API keeps answering from an outdated replica.
    """
    cursor = after
    while True:
        params = {
            "$select": ",".join(SOURCE_COLUMNS),
            "$where": build_where(window_start, cursor),
            "$order": ":updated_at, unique_key",
            "$limit": str(page_size),
        }
        rows = get_fresh(session, url, params, timeout).json()
        if not rows:
            return
        last = rows[-1]
        cursor = Watermark(updated_at=last[":updated_at"], unique_key=last["unique_key"])
        yield rows, cursor
        if len(rows) < page_size:
            return
