from __future__ import annotations

from datetime import datetime

import duckdb
import pytest

from nyc311 import quality


def test_volume_anomalies_flags_spike_and_skips_short_history():
    daily = [(f"2026-09-{d:02d}", 100) for d in range(1, 11)] + [("2026-09-11", 190)]
    result = quality.volume_anomalies(daily)

    # First 7 days lack history and are not evaluated.
    assert result[0]["day"] == "2026-09-08"
    assert [r["flagged"] for r in result] == [False, False, False, True]
    assert result[-1]["ratio"] == pytest.approx(1.9)


def test_volume_anomalies_uses_only_trailing_window():
    # An old spike more than 28 days back must not affect today's mean.
    daily = [("d00", 10_000)] + [(f"d{i:02d}", 100) for i in range(1, 30)] + [("d30", 100)]
    assert quality.volume_anomalies(daily)[-1]["trailing_mean"] == pytest.approx(100)


def test_volume_anomalies_drop_is_flagged():
    daily = [(f"d{i:02d}", 100) for i in range(10)] + [("d10", 40)]
    assert quality.volume_anomalies(daily)[-1]["flagged"] is True


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


def test_duplicates_fail_when_warehouse_has_repeated_keys(con):
    con.execute("INSERT INTO raw.service_requests VALUES ('1'), ('1'), ('2')")
    run = {"rows_fetched": 3, "rows_duplicate_in_run": 0}
    result = quality.check_duplicates(con, run)
    assert result.status == "fail"
    assert result.metrics["warehouse_duplicate_keys"] == 1


def test_duplicates_warn_on_high_in_run_repeat_rate(con):
    con.execute("INSERT INTO raw.service_requests VALUES ('1'), ('2')")
    result = quality.check_duplicates(con, {"rows_fetched": 100, "rows_duplicate_in_run": 5})
    assert result.status == "warn"


def test_markdown_report_lists_every_check():
    report = {
        "run_id": "r",
        "source": "fixture",
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
