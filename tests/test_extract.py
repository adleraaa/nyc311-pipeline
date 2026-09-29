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
        window_days=30,
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
