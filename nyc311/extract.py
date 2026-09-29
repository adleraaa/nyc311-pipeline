"""Extract-load orchestration: API (or a fixture file) -> landing Parquet -> raw table.

Each page is committed in its own transaction together with the new watermark,
so a crash mid-run loses at most the page in flight and the next run resumes
exactly where the last committed page ended.
"""

from __future__ import annotations

import gzip
import json
import logging
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import requests

from nyc311 import warehouse as wh
from nyc311.socrata import StaleReplicaError, Watermark, fetch_pages

log = logging.getLogger(__name__)


def _format_wm(wm: Watermark | None) -> str | None:
    return None if wm is None else f"{wm.updated_at}|{wm.unique_key}"


def _load_pages(
    con: duckdb.DuckDBPyConnection,
    pages: Iterable[tuple[list[dict], Watermark]],
    stats: wh.RunStats,
    *,
    landing_dir: Path,
    covered_from: str | None,
    loaded_at: datetime,
) -> Watermark | None:
    """Land and upsert each page; updates `stats` in place and returns the last watermark.

    With covered_from=None the extractor state is left untouched (fixture loads).
    """
    last_wm = None
    for rows, wm in pages:
        stats.pages += 1
        table = wh.rows_to_table(rows)
        wh.land_page(table, landing_dir, stats.run_id, stats.pages)

        keys = table.column("unique_key").to_pylist()
        # Keys delivered more than once in this run (within a page or across
        # pages, e.g. a row updated while we were paging). Reported by QA.
        for key in keys:
            if key in stats.seen_keys:
                stats.rows_duplicate_in_run += 1
            stats.seen_keys.add(key)

        con.begin()
        try:
            inserted, updated = wh.upsert_page(
                con, wh.dedupe_latest(table), stats.run_id, loaded_at
            )
            if covered_from is not None:
                wh.write_state(con, wm, covered_from)
            con.commit()
        except Exception:
            con.rollback()
            raise
        stats.rows_fetched += len(rows)
        stats.rows_inserted += inserted
        stats.rows_updated += updated
        last_wm = wm
        log.info("page %d: %d rows (%d new, %d updated)", stats.pages, len(rows), inserted, updated)
    return last_wm


def _start_run(
    con: duckdb.DuckDBPyConnection,
    run_id: str,
    *,
    source: str,
    mode: str,
    started_at: datetime,
    source_as_of: datetime,
    window_start: str,
    wm_before: Watermark | None,
) -> None:
    """Write the audit row before loading anything, so a crashed run still leaves a trace."""
    con.execute(
        """
        INSERT INTO raw.extract_runs (run_id, source, mode, started_at, source_as_of,
                                      window_start, watermark_before, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'running')
        """,
        [
            run_id,
            source,
            mode,
            started_at.replace(tzinfo=None),
            source_as_of.replace(tzinfo=None),
            window_start,
            _format_wm(wm_before),
        ],
    )


def _finish_run(
    con: duckdb.DuckDBPyConnection,
    stats: wh.RunStats,
    *,
    status: str,
    wm_after: Watermark | None,
) -> None:
    con.execute(
        """
        UPDATE raw.extract_runs SET
            status = ?, finished_at = ?, watermark_after = ?, pages = ?, rows_fetched = ?,
            rows_duplicate_in_run = ?, rows_inserted = ?, rows_updated = ?, rows_pruned = ?
        WHERE run_id = ?
        """,
        [
            status,
            datetime.now(UTC).replace(tzinfo=None),
            _format_wm(wm_after),
            stats.pages,
            stats.rows_fetched,
            stats.rows_duplicate_in_run,
            stats.rows_inserted,
            stats.rows_updated,
            stats.rows_pruned,
            stats.run_id,
        ],
    )


def run_extract(
    con: duckdb.DuckDBPyConnection,
    session: requests.Session,
    *,
    data_dir: Path,
    window_days: int,
    page_size: int,
    now: datetime | None = None,
    landing_keep_days: int = 7,
) -> wh.RunStats:
    """Incremental API extract. Falls back to a full window backfill when there is
    no state, or when the requested window starts earlier than the rows the
    warehouse holds (covered_from)."""
    now = now or datetime.now(UTC)
    window_start = wh.window_start_for(now, window_days)
    watermark, covered_from = wh.read_state(con)

    if watermark is None or covered_from is None or window_start < covered_from:
        mode, after = "backfill", None
    else:
        mode, after = "incremental", watermark
    stats = wh.RunStats(run_id=wh.new_run_id(now, "api"), mode=mode)
    log.info(
        "run %s: %s from %s, window_start=%s", stats.run_id, mode, _format_wm(after), window_start
    )
    _start_run(
        con,
        stats.run_id,
        source="api",
        mode=mode,
        started_at=now,
        source_as_of=now,
        window_start=window_start,
        wm_before=after,
    )
    status = "success"
    try:
        # While pages load, state says "covered from window_start": a backfill
        # starts there, and an incremental run already covers at least that much.
        try:
            _load_pages(
                con,
                fetch_pages(session, window_start, after, page_size=page_size),
                stats,
                landing_dir=data_dir / "landing",
                covered_from=window_start,
                loaded_at=now.replace(tzinfo=None),
            )
        except StaleReplicaError as exc:
            # Not a failure: the pages loaded so far are committed, and the
            # next run resumes from their watermark once replicas catch up.
            log.warning("stopping early: %s", exc)
            status = "stale_source"
        last_wm = wh.read_state(con)[0] if stats.pages else None
        con.begin()
        try:
            stats.rows_pruned = wh.prune_window(con, window_start)
            # After the prune the warehouse holds exactly the rows created since
            # window_start, so covered_from moves forward with it in the same
            # transaction. Otherwise a later, wider window would look covered
            # and the pruned rows would never be fetched again.
            wh.write_state(con, last_wm or after, window_start)
            con.commit()
        except Exception:
            con.rollback()
            raise
        wh.prune_landing(data_dir / "landing", landing_keep_days, now)
    except BaseException:
        _finish_run(con, stats, status="failed", wm_after=wh.read_state(con)[0])
        raise
    stats.status = status
    _finish_run(con, stats, status=status, wm_after=last_wm or after)
    return stats


def read_fixture(path: Path) -> tuple[datetime, list[dict]]:
    """A fixture is {"fetched_at": iso, "rows": [...]} as written by scripts/make_fixture.py."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8") as fh:
        payload = json.load(fh)
    fetched_at = datetime.fromisoformat(payload["fetched_at"].replace("Z", "+00:00"))
    return fetched_at, payload["rows"]


def load_fixture(
    con: duckdb.DuckDBPyConnection, path: Path, *, data_dir: Path, page_size: int = 1000
) -> wh.RunStats:
    """Load a committed fixture through the same landing/upsert path as the API.

    The fixture's fetch time is recorded as source_as_of, so freshness and
    volume checks evaluate the data relative to when it was captured and give
    the same answer in CI on any day.

    A fixture is a sample, so it must never become the API extractor's resume
    point: the watermark state is not written, and a later `extract` on the
    same file starts with a full backfill. Loading a fixture into a warehouse
    that already holds API data is refused, because the sample's older row
    versions would overwrite newer ones.
    """
    n_api_runs = con.execute(
        "SELECT count(*) FROM raw.extract_runs WHERE source = 'api'"
    ).fetchone()[0]
    if n_api_runs:
        raise ValueError(
            f"this warehouse already holds {n_api_runs} API runs; "
            "load the fixture into a separate --db"
        )
    fetched_at, rows = read_fixture(path)
    rows = sorted(rows, key=lambda r: (r[":updated_at"], r["unique_key"]))

    def pages():
        for i in range(0, len(rows), page_size):
            chunk = rows[i : i + page_size]
            yield chunk, Watermark(chunk[-1][":updated_at"], chunk[-1]["unique_key"])

    started = datetime.now(UTC)
    stats = wh.RunStats(run_id=wh.new_run_id(started, "fixture"), mode="fixture")
    window_start = min(r["created_date"] for r in rows)[:10] + "T00:00:00"
    _start_run(
        con,
        stats.run_id,
        source="fixture",
        mode="fixture",
        started_at=started,
        source_as_of=fetched_at,
        window_start=window_start,
        wm_before=None,
    )
    last_wm = _load_pages(
        con,
        pages(),
        stats,
        landing_dir=data_dir / "landing",
        covered_from=None,
        loaded_at=started.replace(tzinfo=None),
    )
    _finish_run(con, stats, status="success", wm_after=last_wm)
    return stats
