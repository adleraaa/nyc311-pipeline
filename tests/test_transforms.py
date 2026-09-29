"""Runs `dbt build` on a hand-made dataset whose correct answers are known."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb
import pytest
from dbt.cli.main import dbtRunner

from nyc311 import warehouse as wh
from tests.conftest import make_row

DBT_DIR = Path(__file__).resolve().parent.parent / "dbt"


def _closed(key: str, hours: int, **fields) -> dict:
    # Created 2026-09-10 00:00, closed `hours` later (hours < 24 keeps it simple).
    return make_row(
        key,
        created="2026-09-10T00:00:00.000",
        closed_date=f"2026-09-10T{hours:02d}:00:00.000",
        status="Closed",
        agency="DOT",
        complaint_type="Street Light Condition",
        **fields,
    )


ROWS = [
    # Resolution hours 1, 2, 3, 4, 10 -> median 3, p90 (linear) 4 + 0.6 * 6 = 7.6
    _closed("c1", 1),
    _closed("c2", 2),
    _closed("c3", 3),
    _closed("c4", 4),
    _closed("c5", 10),
    # Closed before it was created: must not produce a negative resolution time.
    make_row(
        "bad",
        created="2026-09-12T00:00:00.000",
        closed_date="1900-01-01T00:00:00.000",
        status="Closed",
        agency="DOT",
        complaint_type="Street Light Condition",
    ),
    # Borough variants.
    make_row("b1", borough="Kings"),
    make_row("b2", borough=" queens "),
    make_row("b3", borough="Unspecified"),
    make_row("b4", borough=None),
    # Open requests aged against snapshot_at = latest created_at = 2026-09-20 12:00.
    make_row("o1", created="2026-09-20T12:00:00.000", agency="NYPD"),  # 0 days
    make_row("o2", created="2026-09-18T00:00:00.000", agency="NYPD"),  # 2.5 days
    make_row("o3", created="2026-09-01T00:00:00.000", agency="NYPD"),  # 19.5 days
]


@pytest.fixture(scope="module")
def built_db(tmp_path_factory) -> Path:
    tmp = tmp_path_factory.mktemp("dbt")
    db_path = tmp / "warehouse.duckdb"
    con = wh.connect(db_path)
    wh.upsert_page(con, wh.rows_to_table(ROWS), "test-run", datetime(2026, 9, 21))
    con.close()

    mp = pytest.MonkeyPatch()
    mp.setenv("NYC311_DB_PATH", str(db_path))
    try:
        result = dbtRunner().invoke(
            [
                "build",
                "--project-dir",
                str(DBT_DIR),
                "--profiles-dir",
                str(DBT_DIR),
                "--target-path",
                str(tmp / "target"),
                "--log-path",
                str(tmp / "logs"),
                "--quiet",
            ]
        )
    finally:
        mp.undo()
    assert result.success, result.exception
    return db_path


def _query(db_path: Path, sql: str):
    with duckdb.connect(str(db_path)) as con:
        return con.execute(sql).fetchall()


def test_borough_normalization(built_db):
    rows = dict(
        _query(
            built_db,
            "SELECT unique_key, borough FROM staging.stg_service_requests "
            "WHERE unique_key IN ('b1', 'b2', 'b3', 'b4')",
        )
    )
    assert rows == {"b1": "BROOKLYN", "b2": "QUEENS", "b3": "UNSPECIFIED", "b4": "UNSPECIFIED"}


def test_invalid_closed_date_is_nulled_and_flagged(built_db):
    assert _query(
        built_db,
        "SELECT closed_at, has_invalid_closed_date FROM "
        "staging.stg_service_requests WHERE unique_key = 'bad'",
    ) == [(None, True)]
    assert _query(
        built_db,
        "SELECT resolution_hours FROM intermediate.int_requests_enriched WHERE unique_key = 'bad'",
    ) == [(None,)]


def test_resolution_quantiles(built_db):
    rows = _query(
        built_db,
        "SELECT complaint_type, n_closed, median_hours, p90_hours "
        "FROM marts.agg_resolution_time WHERE agency = 'DOT' ORDER BY 1",
    )
    assert [(t, n) for t, n, _, _ in rows] == [("(all)", 5), ("Street Light Condition", 5)]
    for _, _, median, p90 in rows:
        assert median == pytest.approx(3.0)
        assert p90 == pytest.approx(7.6)


def test_backlog_buckets(built_db):
    rows = _query(
        built_db,
        "SELECT age_bucket, n_open FROM marts.agg_open_backlog_aging "
        "WHERE agency = 'NYPD' ORDER BY bucket_order",
    )
    assert rows == [("<1d", 1), ("1-3d", 1), ("14-30d", 1)]


def test_daily_volume_counts_every_request(built_db):
    assert _query(built_db, "SELECT sum(n_requests) FROM marts.fct_daily_volume") == [(len(ROWS),)]
    assert _query(
        built_db,
        "SELECT n_requests FROM marts.fct_daily_volume "
        "WHERE created_day = DATE '2026-09-10' AND borough = 'BROOKLYN' "
        "AND complaint_type = 'Street Light Condition'",
    ) == [(5,)]
