from __future__ import annotations

from datetime import UTC, datetime

import pyarrow.parquet as pq

from nyc311 import warehouse as wh
from tests.conftest import make_row

T1 = datetime(2026, 9, 20, 6, 0)
T2 = datetime(2026, 9, 21, 6, 0)


def test_rows_to_table_renames_system_fields_and_fills_missing():
    table = wh.rows_to_table([make_row("1", closed_date=None), make_row("2", latitude=40.7)])
    assert "_sys_updated_at" in table.column_names
    assert table.column("closed_date").to_pylist() == [None, None]
    # Everything lands as text; typing happens in dbt staging.
    assert table.column("latitude").to_pylist() == [None, "40.7"]


def test_upsert_is_idempotent_and_tracks_loads(tmp_path):
    con = wh.connect(tmp_path / "w.duckdb")
    page = wh.rows_to_table([make_row("1"), make_row("2")])

    assert wh.upsert_page(con, page, "run-a", T1) == (2, 0)
    assert wh.upsert_page(con, page, "run-b", T2) == (0, 2)

    rows = con.execute(
        "SELECT unique_key, _first_loaded_at, _last_loaded_at, _last_run_id, _load_count "
        "FROM raw.service_requests ORDER BY unique_key"
    ).fetchall()
    assert rows == [("1", T1, T2, "run-b", 2), ("2", T1, T2, "run-b", 2)]


def test_upsert_overwrites_changed_fields(tmp_path):
    con = wh.connect(tmp_path / "w.duckdb")
    wh.upsert_page(con, wh.rows_to_table([make_row("1", status="Open")]), "a", T1)
    closed = make_row(
        "1", "2026-09-21T01:33:00.000Z", status="Closed", closed_date="2026-09-20T10:00:00.000"
    )
    wh.upsert_page(con, wh.rows_to_table([closed]), "b", T2)
    assert con.execute(
        "SELECT status, closed_date, _sys_updated_at FROM raw.service_requests"
    ).fetchall() == [("Closed", "2026-09-20T10:00:00.000", "2026-09-21T01:33:00.000Z")]


def test_dedupe_latest_keeps_newest_version():
    table = wh.rows_to_table(
        [
            make_row("1", "2026-09-20T01:00:00.000Z", status="Open"),
            make_row("1", "2026-09-21T01:00:00.000Z", status="Closed"),
            make_row("2"),
        ]
    )
    deduped = wh.dedupe_latest(table)
    by_key = dict(
        zip(
            deduped.column("unique_key").to_pylist(),
            deduped.column("status").to_pylist(),
            strict=True,
        )
    )
    assert by_key == {"1": "Closed", "2": "Open"}


def test_prune_window_drops_only_old_rows(tmp_path):
    con = wh.connect(tmp_path / "w.duckdb")
    page = wh.rows_to_table(
        [
            make_row("old", created="2026-08-01T12:00:00.000"),
            make_row("new", created="2026-09-15T12:00:00.000"),
        ]
    )
    wh.upsert_page(con, page, "a", T1)
    assert wh.prune_window(con, "2026-09-01T00:00:00") == 1
    assert con.execute("SELECT unique_key FROM raw.service_requests").fetchall() == [("new",)]


def test_state_round_trip(tmp_path):
    con = wh.connect(tmp_path / "w.duckdb")
    assert wh.read_state(con) == (None, None)
    wm = wh.Watermark("2026-09-28T01:33:33.096Z", "70555073")
    wh.write_state(con, wm, "2026-08-29T00:00:00")
    wh.write_state(con, wm, "2026-08-29T00:00:00")  # upsert, not a second row
    assert wh.read_state(con) == (wm, "2026-08-29T00:00:00")


def test_land_page_and_prune_landing(tmp_path):
    table = wh.rows_to_table([make_row("1")])
    old = wh.land_page(table, tmp_path, "20260901T060000Z-api", 1)
    new = wh.land_page(table, tmp_path, "20260927T060000Z-api", 1)
    assert pq.read_table(new).num_rows == 1

    removed = wh.prune_landing(tmp_path, keep_days=7, now=datetime(2026, 9, 28, tzinfo=UTC))
    assert removed == [old.parent]
    assert new.exists() and not old.exists()


def test_window_start_for():
    now = datetime(2026, 9, 28, 6, 30, tzinfo=UTC)
    assert wh.window_start_for(now, 30) == "2026-08-29T00:00:00"
