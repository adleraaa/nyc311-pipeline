"""Raw layer: Parquet landing files plus an idempotent upsert into DuckDB.

Layout
  data/landing/<run_id>/page_00001.parquet   rows exactly as the API returned them
  data/warehouse.duckdb                       raw.service_requests (one row per unique_key)
                                              raw.extract_state   (the watermark, one row)
                                              raw.extract_runs    (one row per run, for audit/QA)

The raw table keeps every column as VARCHAR; typing and cleaning happen in dbt
staging so a bad value never makes a load fail.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from nyc311.socrata import SOURCE_COLUMNS, Watermark

# API field name -> raw column name. System fields (":x") get a "_sys_" prefix
# because a leading colon is not a valid unquoted SQL identifier.
RAW_COLUMNS = {
    name: ("_sys_" + name[1:] if name.startswith(":") else name) for name in SOURCE_COLUMNS
}
DATA_COLUMNS = list(RAW_COLUMNS.values())
ARROW_SCHEMA = pa.schema([(col, pa.string()) for col in DATA_COLUMNS])

DDL = f"""
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.service_requests (
    {
    ", ".join(
        f"{col} VARCHAR" + (" PRIMARY KEY" if col == "unique_key" else "") for col in DATA_COLUMNS
    )
},
    _first_loaded_at TIMESTAMP NOT NULL,
    _last_loaded_at TIMESTAMP NOT NULL,
    _last_run_id VARCHAR NOT NULL,
    _load_count INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS raw.extract_state (
    id INTEGER PRIMARY KEY,
    watermark_updated_at VARCHAR,
    watermark_unique_key VARCHAR,
    covered_from VARCHAR NOT NULL
);
CREATE TABLE IF NOT EXISTS raw.extract_runs (
    run_id VARCHAR PRIMARY KEY,
    source VARCHAR NOT NULL,
    mode VARCHAR NOT NULL,
    started_at TIMESTAMP NOT NULL,
    finished_at TIMESTAMP,
    source_as_of TIMESTAMP NOT NULL,
    window_start VARCHAR NOT NULL,
    watermark_before VARCHAR,
    watermark_after VARCHAR,
    pages INTEGER NOT NULL DEFAULT 0,
    rows_fetched INTEGER NOT NULL DEFAULT 0,
    rows_duplicate_in_run INTEGER NOT NULL DEFAULT 0,
    rows_inserted INTEGER NOT NULL DEFAULT 0,
    rows_updated INTEGER NOT NULL DEFAULT 0,
    rows_pruned INTEGER NOT NULL DEFAULT 0
);
"""


@dataclass
class RunStats:
    run_id: str
    pages: int = 0
    rows_fetched: int = 0
    rows_duplicate_in_run: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_pruned: int = 0
    seen_keys: set[str] = field(default_factory=set, repr=False)


def connect(db_path: Path | str) -> duckdb.DuckDBPyConnection:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    con.execute(DDL)
    return con


def window_start_for(now: datetime, window_days: int) -> str:
    """Midnight `window_days` ago, formatted like the source's floating timestamps.

    created_date in the source is New York local time without an offset; we use
    the UTC calendar date here, which can shift the boundary by a few hours. That
    is fine for a rolling window and avoids a timezone database dependency.
    """
    start = (now - timedelta(days=window_days)).date()
    return f"{start.isoformat()}T00:00:00"


def read_state(con: duckdb.DuckDBPyConnection) -> tuple[Watermark | None, str | None]:
    row = con.execute(
        "SELECT watermark_updated_at, watermark_unique_key, covered_from "
        "FROM raw.extract_state WHERE id = 1"
    ).fetchone()
    if row is None:
        return None, None
    watermark = Watermark(row[0], row[1]) if row[0] is not None else None
    return watermark, row[2]


def write_state(
    con: duckdb.DuckDBPyConnection, watermark: Watermark | None, covered_from: str
) -> None:
    con.execute(
        "INSERT OR REPLACE INTO raw.extract_state VALUES (1, ?, ?, ?)",
        [
            watermark.updated_at if watermark else None,
            watermark.unique_key if watermark else None,
            covered_from,
        ],
    )


def rows_to_table(rows: list[dict]) -> pa.Table:
    """API rows (dicts with optional keys) -> all-string Arrow table in raw column order."""
    columns = {
        raw: [None if (v := row.get(api)) is None else str(v) for row in rows]
        for api, raw in RAW_COLUMNS.items()
    }
    return pa.table(columns, schema=ARROW_SCHEMA)


def dedupe_latest(table: pa.Table) -> pa.Table:
    """Keep one row per unique_key (the one with the greatest _sys_updated_at).

    DuckDB refuses an ON CONFLICT DO UPDATE that touches the same key twice in
    one statement, and a replayed page can legitimately contain repeats.
    """
    if table.num_rows == len(set(table.column("unique_key").to_pylist())):
        return table
    con = duckdb.connect()
    con.register("page", table)
    return con.execute(
        "SELECT * FROM page QUALIFY row_number() OVER "
        "(PARTITION BY unique_key ORDER BY _sys_updated_at DESC NULLS LAST) = 1"
    ).to_arrow_table()


def land_page(table: pa.Table, landing_dir: Path, run_id: str, page_no: int) -> Path:
    """Write the untouched page to Parquet.

    Same run + page number = same file name, so a rerun overwrites instead of duplicating.
    """
    out_dir = landing_dir / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"page_{page_no:05d}.parquet"
    pq.write_table(table, path)
    return path


def upsert_page(
    con: duckdb.DuckDBPyConnection, table: pa.Table, run_id: str, loaded_at: datetime
) -> tuple[int, int]:
    """Merge one deduplicated page into raw.service_requests; returns (inserted, updated)."""
    con.register("page", table)
    try:
        existing = con.execute(
            "SELECT count(*) FROM page JOIN raw.service_requests USING (unique_key)"
        ).fetchone()[0]
        set_clause = ", ".join(f"{c} = excluded.{c}" for c in DATA_COLUMNS if c != "unique_key")
        # ON CONFLICT DO UPDATE (rather than INSERT OR REPLACE) so first-load
        # time and the load counter survive, which the late-update check uses.
        con.execute(
            f"""
            INSERT INTO raw.service_requests
            SELECT {", ".join(DATA_COLUMNS)}, $ts, $ts, $run_id, 1 FROM page
            ON CONFLICT (unique_key) DO UPDATE SET {set_clause},
                _last_loaded_at = excluded._last_loaded_at,
                _last_run_id = excluded._last_run_id,
                _load_count = raw.service_requests._load_count + 1
            """,
            {"ts": loaded_at, "run_id": run_id},
        )
    finally:
        con.unregister("page")
    return table.num_rows - existing, existing


def prune_window(con: duckdb.DuckDBPyConnection, window_start: str) -> int:
    """Drop rows whose created_date fell out of the rolling window."""
    return con.execute(
        "DELETE FROM raw.service_requests "
        "WHERE try_cast(created_date AS TIMESTAMP) < try_cast($w AS TIMESTAMP)",
        {"w": window_start},
    ).fetchone()[0]


def prune_landing(landing_dir: Path, keep_days: int, now: datetime) -> list[Path]:
    """Delete landing run folders older than keep_days (run ids start with a UTC timestamp)."""
    removed = []
    if not landing_dir.exists():
        return removed
    cutoff = (now - timedelta(days=keep_days)).strftime("%Y%m%dT%H%M%S")
    for run_dir in sorted(landing_dir.iterdir()):
        if run_dir.is_dir() and run_dir.name[:15] < cutoff:
            shutil.rmtree(run_dir)
            removed.append(run_dir)
    return removed


def new_run_id(now: datetime, source: str) -> str:
    return f"{now.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}-{source}"
