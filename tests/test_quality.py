from __future__ import annotations

from datetime import date, datetime, timedelta
from urllib.parse import parse_qs, urlparse

import duckdb
import pytest
import requests
import responses

from nyc311 import quality
from nyc311 import warehouse as wh
from nyc311.socrata import DATASET_URL
from tests.conftest import make_row


def _days(counts: list[int], start: str = "2026-08-31") -> list[tuple[str, int]]:
    """(ISO day, count) for consecutive days; 2026-08-31 is a Monday."""
    first = date.fromisoformat(start)
    return [((first + timedelta(days=i)).isoformat(), c) for i, c in enumerate(counts)]


WEEK = [100, 100, 100, 100, 100, 50, 50]  # Mon-Fri busy, weekend quiet


def test_volume_weekly_cycle_is_not_an_anomaly():
    result = quality.volume_anomalies(_days(WEEK * 4))

    # Two earlier same-weekday values are needed, so week 3 is the first evaluated.
    assert result[0]["day"] == "2026-09-14"
    assert result[0]["baseline_weeks"] == 2
    assert not any(r["flagged"] for r in result)


def test_volume_drop_on_a_weekday_is_flagged():
    counts = WEEK * 3 + [60]  # Monday of week 4 at 60% of the Monday baseline
    result = quality.volume_anomalies(_days(counts))

    assert result[-1]["flagged"] is True
    assert result[-1]["ratio"] == pytest.approx(0.6)


def test_volume_baseline_uses_at_most_four_weeks():
    counts = [1000] + [100] * 34  # an old spike five weeks back
    last = quality.volume_anomalies(_days(counts))[-1]
    assert last["baseline_weeks"] == 4
    assert last["baseline_mean"] == pytest.approx(100)


def test_complete_days_respect_publication_lag():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA staging; CREATE SCHEMA marts")
    con.execute(
        "CREATE TABLE marts.fct_daily_volume AS SELECT * FROM (VALUES "
        "(DATE '2026-09-25', 10), (DATE '2026-09-26', 10), (DATE '2026-09-27', 3)) "
        "t(created_day, n_requests)"
    )
    # Every row published 10 h after creation; latest source update 2026-09-28 01:30 UTC.
    con.execute(
        "CREATE TABLE staging.stg_service_requests AS SELECT * FROM (VALUES "
        "(TIMESTAMP '2026-09-27 12:00', TIMESTAMP '2026-09-27 22:00', "
        " TIMESTAMP '2026-09-28 01:30')) "
        "t(created_at_utc, published_at_utc, source_updated_at_utc)"
    )
    days, lag = quality.complete_days(con)

    # 09-26 ended 09-27 04:00 UTC (+10 h = 14:00, before the update) -> complete;
    # 09-27 ended 09-28 04:00 UTC -> still filling in.
    assert lag == pytest.approx(10.0)
    assert days == [("2026-09-25", 10), ("2026-09-26", 10)]


@pytest.fixture
def con():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA staging; CREATE SCHEMA raw")
    con.execute("CREATE TABLE staging.stg_service_requests (source_updated_at_utc TIMESTAMP)")
    con.execute("CREATE TABLE raw.service_requests (unique_key VARCHAR)")
    return con


@pytest.mark.parametrize(
    ("updated", "expected"),
    [("2026-09-28 01:33", "pass"), ("2026-09-26 12:00", "warn"), ("2026-09-24 00:00", "fail")],
)
def test_freshness_thresholds(con, updated, expected):
    con.execute("INSERT INTO staging.stg_service_requests VALUES (?)", [updated])
    result = quality.check_freshness(con, datetime(2026, 9, 28, 6, 30))
    assert result.status == expected


def test_duplicates_warn_on_high_in_run_repeat_rate():
    assert quality.check_duplicates({"rows_fetched": 100, "rows_duplicate_in_run": 5}).status == (
        "warn"
    )
    assert quality.check_duplicates({"rows_fetched": 0, "rows_duplicate_in_run": 0}).status == (
        "pass"
    )


API_RUN = {"source": "api", "window_start": "2026-08-30T00:00:00"}


@responses.activate
@pytest.mark.parametrize(("source_n", "expected"), [(1000, "pass"), (1010, "warn")])
def test_source_count_compares_with_api(source_n, expected):
    con = wh.connect(":memory:")
    wh.upsert_page(
        con, wh.rows_to_table([make_row(str(i)) for i in range(1000)]), "r", datetime(2026, 9, 1)
    )
    responses.get(DATASET_URL, json=[{"n": str(source_n)}])

    result = quality.check_source_count(con, API_RUN, requests.Session())

    assert result.status == expected
    assert result.metrics["difference"] == 1000 - source_n
    where = parse_qs(urlparse(responses.calls[0].request.url).query)["$where"][0]
    assert where == "created_date >= '2026-08-30T00:00:00'"


@responses.activate
def test_source_count_unreachable_api_is_a_warning_not_a_crash():
    con = wh.connect(":memory:")
    responses.get(DATASET_URL, body=requests.ConnectionError("down"))
    assert quality.check_source_count(con, API_RUN, requests.Session()).status == "warn"


def test_source_count_skipped_for_fixture_runs():
    con = wh.connect(":memory:")
    result = quality.check_source_count(con, {"source": "fixture"}, requests.Session())
    assert result.status == "info"


def _staging_view(con: duckdb.DuckDBPyConnection) -> None:
    """Just the staging columns the late-arrival check reads (created treated as UTC)."""
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute(
        """
        CREATE VIEW staging.stg_service_requests AS
        SELECT unique_key,
               try_cast(created_date AS TIMESTAMP) AS created_at_utc,
               try_cast(replace(_sys_created_at, 'Z', '') AS TIMESTAMP) AS published_at_utc,
               _last_run_id, _load_count
        FROM raw.service_requests
        """
    )


def test_late_updates_count_only_rows_that_existed_before():
    con = wh.connect(":memory:")
    _staging_view(con)
    old = "2026-09-01T10:00:00.000"  # > 7 days before the second run
    recent = "2026-09-26T10:00:00.000"
    run1 = [make_row("old", created=old), make_row("recent", created=recent)]
    wh.upsert_page(con, wh.rows_to_table(run1), "run1", datetime(2026, 9, 27, 7))
    run2 = [
        make_row("old", created=old, status="Closed"),
        make_row("recent", created=recent, status="Closed"),
        make_row("brand-new-but-old", created=old),  # first load: not a late update
    ]
    wh.upsert_page(con, wh.rows_to_table(run2), "run2", datetime(2026, 9, 28, 7))
    run = {"run_id": "run2", "mode": "incremental", "source_as_of": datetime(2026, 9, 28, 7)}

    metrics = quality.check_late_arriving(con, run).metrics

    assert metrics["latest_run_updates_of_existing_rows"] == 2
    assert metrics["latest_run_updates_created_over_7d_before"] == 1


def test_late_updates_not_applicable_to_backfill():
    con = wh.connect(":memory:")
    _staging_view(con)
    wh.upsert_page(con, wh.rows_to_table([make_row("1")]), "b", datetime(2026, 9, 28))
    result = quality.check_late_arriving(
        con, {"run_id": "b", "mode": "backfill", "source_as_of": datetime(2026, 9, 28)}
    )
    assert "not applicable" in result.detail
    assert "latest_run_updates_of_existing_rows" not in result.metrics


def test_late_arriving_on_empty_staging_does_not_crash():
    con = wh.connect(":memory:")
    _staging_view(con)
    result = quality.check_late_arriving(
        con, {"run_id": "x", "mode": "incremental", "source_as_of": datetime(2026, 9, 28)}
    )
    assert result.status == "info"
    assert result.metrics == {"rows": 0}


def test_markdown_report_lists_every_check():
    report = {
        "run_id": "r",
        "source": "fixture",
        "mode": "fixture",
        "source_as_of_utc": "2026-09-28T00:00:00",
        "overall": "pass",
        "checks": [
            {"name": "freshness", "status": "pass", "detail": "ok", "metrics": {}},
            {"name": "volume_anomaly", "status": "warn", "detail": "x", "metrics": {}},
        ],
    }
    md = quality.to_markdown(report)
    assert "| freshness | pass | ok |" in md
    assert "| volume_anomaly | warn | x |" in md
