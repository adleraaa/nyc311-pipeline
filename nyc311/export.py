"""Export mart data to small JSON files and assemble the static dashboard."""

from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import duckdb

SITE_SRC = Path(__file__).resolve().parent.parent / "site"
TOP_COMPLAINT_TYPES = 10
MIN_REQUESTS_FOR_RESOLUTION = 30  # below this a p90 is mostly noise


def _rows(con: duckdb.DuckDBPyConnection, sql: str, params: dict | None = None) -> list[dict]:
    cur = con.execute(sql, params or {})
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]


def export_marts(con: duckdb.DuckDBPyConnection) -> dict[str, object]:
    """Query the marts into dashboard-sized datasets (hundreds of rows, not the whole table)."""
    meta = _rows(
        con,
        """
        SELECT
            (SELECT max(created_at) FROM staging.stg_service_requests)::varchar AS data_as_of,
            (SELECT min(created_day) FROM marts.fct_daily_volume)::varchar AS window_start,
            (SELECT max(source_updated_at_utc) FROM staging.stg_service_requests)::varchar
                AS source_updated_at_utc,
            (SELECT count(*) FROM staging.stg_service_requests) AS n_requests,
            (SELECT sum(n_open) FROM marts.agg_open_backlog_aging) AS n_open
        """,
    )[0]
    daily_by_borough = _rows(
        con,
        """
        SELECT created_day::varchar AS day, borough, sum(n_requests)::bigint AS n
        FROM marts.fct_daily_volume GROUP BY ALL ORDER BY day, borough
        """,
    )
    top_types = _rows(
        con,
        """
        SELECT complaint_type, sum(n_requests)::bigint AS n
        FROM marts.fct_daily_volume GROUP BY ALL ORDER BY n DESC, complaint_type
        LIMIT $k
        """,
        {"k": TOP_COMPLAINT_TYPES},
    )
    # NULL median/p90 = too many requests still open for the quantile to be
    # known (see agg_resolution_time); those agencies sort last.
    resolution_by_agency = _rows(
        con,
        """
        SELECT agency, n_requests, n_closed, round(share_open, 4) AS share_open,
               round(median_hours, 2) AS median_hours, round(p90_hours, 2) AS p90_hours
        FROM marts.agg_resolution_time
        WHERE complaint_type = '(all)' AND n_requests >= $min_n
        ORDER BY median_hours NULLS LAST, agency
        """,
        {"min_n": MIN_REQUESTS_FOR_RESOLUTION},
    )
    resolution_top_types = _rows(
        con,
        """
        SELECT agency, complaint_type, n_requests, n_closed, round(share_open, 4) AS share_open,
               round(median_hours, 2) AS median_hours, round(p90_hours, 2) AS p90_hours
        FROM marts.agg_resolution_time
        WHERE complaint_type <> '(all)' AND n_requests >= $min_n
        ORDER BY n_requests DESC LIMIT 15
        """,
        {"min_n": MIN_REQUESTS_FOR_RESOLUTION},
    )
    backlog_by_agency = _rows(
        con,
        """
        SELECT agency, age_bucket, bucket_order, sum(n_open)::bigint AS n
        FROM marts.agg_open_backlog_aging GROUP BY ALL ORDER BY agency, bucket_order
        """,
    )
    return {
        "meta": meta,
        "daily_by_borough": daily_by_borough,
        "top_complaint_types": top_types,
        "resolution_by_agency": resolution_by_agency,
        "resolution_top_types": resolution_top_types,
        "backlog_by_agency": backlog_by_agency,
    }


def build_site(
    con: duckdb.DuckDBPyConnection, out_dir: Path, quality_report: dict | None = None
) -> list[Path]:
    """Copy site/ into out_dir and write data/*.json next to it."""
    if out_dir.exists():
        shutil.rmtree(out_dir)
    shutil.copytree(SITE_SRC, out_dir, ignore=shutil.ignore_patterns("data"))
    data_dir = out_dir / "data"
    data_dir.mkdir()
    datasets = export_marts(con)
    datasets["meta"]["generated_at_utc"] = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    if quality_report is not None:
        datasets["quality"] = quality_report
    written = []
    for name, payload in datasets.items():
        path = data_dir / f"{name}.json"
        path.write_text(json.dumps(payload, default=str), encoding="utf-8")
        written.append(path)
    return written
