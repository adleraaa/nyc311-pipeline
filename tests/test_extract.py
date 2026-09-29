from __future__ import annotations

from datetime import UTC, datetime
from urllib.parse import parse_qs, urlparse

import pytest
import requests
import responses

from nyc311 import extract
from nyc311 import warehouse as wh
from nyc311.socrata import DATASET_URL, Watermark
from tests.conftest import make_row

NOW = datetime(2026, 9, 28, 6, 30, tzinfo=UTC)
TS1 = "2026-09-27T01:33:00.000Z"
TS2 = "2026-09-28T01:33:00.000Z"


def _where(call) -> str:
    return parse_qs(urlparse(call.request.url).query)["$where"][0]


def _run(con, tmp_path, **kwargs):
    return extract.run_extract(
        con,
        requests.Session(),
        data_dir=tmp_path,
        window_days=kwargs.pop("window_days", 30),
        page_size=2,
        now=kwargs.pop("now", NOW),
        **kwargs,
    )


@responses.activate
def test_backfill_then_incremental(tmp_path):
    con = wh.connect(tmp_path / "w.duckdb")
    responses.get(DATASET_URL, json=[make_row("1", TS1), make_row("2", TS1)])
    responses.get(DATASET_URL, json=[make_row("3", TS1)])

    first = _run(con, tmp_path)

    assert (first.pages, first.rows_inserted, first.rows_updated) == (2, 3, 0)
    assert "unique_key >" not in _where(responses.calls[0])  # backfill: no keyset
    assert wh.read_state(con) == (Watermark(TS1, "3"), "2026-08-29T00:00:00")
    assert len(list((tmp_path / "landing").glob("*/page_*.parquet"))) == 2

    # Next day: request 2 was closed, request 4 is new.
    responses.get(DATASET_URL, json=[make_row("2", TS2, status="Closed"), make_row("4", TS2)])
    responses.get(DATASET_URL, json=[])
    second = _run(con, tmp_path, now=datetime(2026, 9, 29, 6, 30, tzinfo=UTC))

    assert (second.rows_inserted, second.rows_updated) == (1, 1)
    assert f"(:updated_at = '{TS1}' AND unique_key > '3')" in _where(responses.calls[2])
    runs = con.execute("SELECT mode FROM raw.extract_runs ORDER BY started_at").fetchall()
    assert runs == [("backfill",), ("incremental",)]
    assert con.execute(
        "SELECT status FROM raw.service_requests WHERE unique_key = '2'"
    ).fetchone() == ("Closed",)


@responses.activate
def test_failed_page_keeps_last_committed_watermark(tmp_path):
    con = wh.connect(tmp_path / "w.duckdb")
    responses.get(DATASET_URL, json=[make_row("1", TS1), make_row("2", TS1)])
    responses.get(DATASET_URL, status=500)

    with pytest.raises(requests.HTTPError):
        _run(con, tmp_path)

    # Page 1 is committed with its watermark; a rerun resumes after key "2".
    assert con.execute("SELECT count(*) FROM raw.service_requests").fetchone() == (2,)
    assert wh.read_state(con)[0] == Watermark(TS1, "2")

    responses.get(DATASET_URL, json=[make_row("3", TS1)])
    stats = _run(con, tmp_path)
    assert stats.rows_inserted == 1
    assert "unique_key > '2'" in _where(responses.calls[-1])


@responses.activate
def test_wider_window_forces_backfill(tmp_path):
    con = wh.connect(tmp_path / "w.duckdb")
    wh.write_state(con, Watermark(TS1, "9"), covered_from="2026-09-20T00:00:00")
    responses.get(DATASET_URL, json=[])

    _run(con, tmp_path)  # 30-day window starts 2026-08-29, before covered_from

    assert "unique_key >" not in _where(responses.calls[0])
    assert con.execute("SELECT mode FROM raw.extract_runs").fetchone() == ("backfill",)


@responses.activate
def test_duplicates_across_pages_are_counted_not_double_loaded(tmp_path):
    con = wh.connect(tmp_path / "w.duckdb")
    # Row 2 is updated while we page, so it shows up again later in the stream.
    responses.get(DATASET_URL, json=[make_row("1", TS1), make_row("2", TS1)])
    responses.get(DATASET_URL, json=[make_row("2", TS2, status="Closed")])

    stats = _run(con, tmp_path)

    assert stats.rows_fetched == 3
    assert stats.rows_duplicate_in_run == 1
    assert con.execute("SELECT count(*) FROM raw.service_requests").fetchone() == (2,)


def test_load_fixture_uses_fixture_time_as_source_as_of(tmp_path, sample_fixture_path):
    con = wh.connect(tmp_path / "w.duckdb")
    fetched_at, rows = extract.read_fixture(sample_fixture_path)

    stats = extract.load_fixture(con, sample_fixture_path, data_dir=tmp_path)

    distinct = len({r["unique_key"] for r in rows})
    assert stats.rows_inserted == distinct
    assert con.execute("SELECT count(*) FROM raw.service_requests").fetchone() == (distinct,)
    as_of = con.execute("SELECT source_as_of FROM raw.extract_runs").fetchone()[0]
    assert as_of == fetched_at.replace(tzinfo=None)


@responses.activate
def test_narrow_then_wider_window_forces_backfill(tmp_path):
    # Regression: pruning must move covered_from forward, or a later wider
    # window looks covered and the pruned rows are never fetched again.
    con = wh.connect(tmp_path / "w.duckdb")
    old = make_row("a", TS1, created="2026-09-01T09:00:00.000")
    new = make_row("b", TS1, created="2026-09-25T09:00:00.000")
    responses.get(DATASET_URL, json=[old, new])
    responses.get(DATASET_URL, json=[])
    _run(con, tmp_path)  # 30-day window from 2026-08-29

    responses.get(DATASET_URL, json=[])
    narrow = _run(con, tmp_path, window_days=7)  # window from 2026-09-21
    assert narrow.rows_pruned == 1
    assert wh.read_state(con)[1] == "2026-09-21T00:00:00"

    responses.get(DATASET_URL, json=[old, new])
    responses.get(DATASET_URL, json=[])
    wide = _run(con, tmp_path)

    assert wide.mode == "backfill"
    assert "unique_key >" not in _where(responses.calls[-2])
    keys = con.execute("SELECT unique_key FROM raw.service_requests ORDER BY 1").fetchall()
    assert keys == [("a",), ("b",)]


@responses.activate
def test_extract_after_fixture_load_backfills(tmp_path, sample_fixture_path):
    # Regression: a fixture is a sample, so it must not leave a watermark the
    # API extractor would resume from.
    con = wh.connect(tmp_path / "w.duckdb")
    extract.load_fixture(con, sample_fixture_path, data_dir=tmp_path)
    assert wh.read_state(con) == (None, None)

    responses.get(DATASET_URL, json=[])
    stats = _run(con, tmp_path)

    assert stats.mode == "backfill"
    assert "unique_key >" not in _where(responses.calls[0])


@responses.activate
def test_load_fixture_refuses_warehouse_with_api_data(tmp_path, sample_fixture_path):
    con = wh.connect(tmp_path / "w.duckdb")
    responses.get(DATASET_URL, json=[])
    _run(con, tmp_path)

    with pytest.raises(ValueError, match="separate --db"):
        extract.load_fixture(con, sample_fixture_path, data_dir=tmp_path)


@responses.activate
def test_failed_run_is_recorded_with_committed_progress(tmp_path):
    con = wh.connect(tmp_path / "w.duckdb")
    responses.get(DATASET_URL, json=[make_row("1", TS1), make_row("2", TS1)])
    responses.get(DATASET_URL, status=500)

    with pytest.raises(requests.HTTPError):
        _run(con, tmp_path)

    row = con.execute(
        "SELECT status, pages, rows_inserted, watermark_after, finished_at IS NOT NULL "
        "FROM raw.extract_runs"
    ).fetchone()
    assert row == ("failed", 1, 2, f"{TS1}|2", True)


@responses.activate
def test_two_runs_in_the_same_second_get_distinct_ids(tmp_path):
    con = wh.connect(tmp_path / "w.duckdb")
    responses.get(DATASET_URL, json=[])
    responses.get(DATASET_URL, json=[])

    first, second = _run(con, tmp_path), _run(con, tmp_path)

    assert first.run_id != second.run_id
    assert con.execute("SELECT count(*) FROM raw.extract_runs").fetchone() == (2,)


@responses.activate
def test_stale_source_stops_run_without_losing_committed_pages(tmp_path):
    con = wh.connect(tmp_path / "w.duckdb")
    responses.get(DATASET_URL, json=[make_row("1", TS1), make_row("2", TS1)])
    responses.get(DATASET_URL, json=[], headers={"X-SODA2-Data-Out-Of-Date": "true"})

    stats = _run(con, tmp_path)

    assert stats.status == "stale_source"
    assert wh.read_state(con)[0] == Watermark(TS1, "2")
    assert con.execute("SELECT status, pages FROM raw.extract_runs").fetchone() == (
        "stale_source",
        1,
    )
