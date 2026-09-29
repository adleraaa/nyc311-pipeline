"""Data-quality checks that dbt's column tests do not cover.

Runs after `dbt build` against the warehouse and writes a JSON + Markdown
report. Each check returns pass / warn / fail / info; only `fail` makes the
CLI exit with 1 (a crash in the check code exits with 2, see __main__).

All "now"-dependent checks use the latest run's source_as_of (the time the
source was observed) rather than the wall clock, so re-running the checks on
an old warehouse or a committed fixture gives the same answer.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean

import duckdb
import requests

from nyc311.socrata import DATASET_URL

FRESHNESS_WARN_HOURS = 36  # the source publishes once a day
FRESHNESS_FAIL_HOURS = 72
VOLUME_WEEKS = 4  # baseline: the same weekday in up to 4 preceding weeks
VOLUME_MIN_WEEKS = 2
VOLUME_BAND = 0.25  # flag a day outside [0.75x, 1.25x] of its baseline
DUPLICATE_WARN_RATE = 0.01
SOURCE_COUNT_WARN_RATE = 0.001
LATE_PUBLISH_HOURS = 72
LATE_UPDATE_DAYS = 7


@dataclass
class CheckResult:
    name: str
    status: str  # pass | warn | fail | info
    detail: str
    metrics: dict


def latest_run(con: duckdb.DuckDBPyConnection) -> dict:
    cur = con.execute("SELECT * FROM raw.extract_runs ORDER BY started_at DESC LIMIT 1")
    row = cur.fetchone()
    if row is None:
        raise RuntimeError("raw.extract_runs is empty: run the extract first")
    return dict(zip([d[0] for d in cur.description], row, strict=True))


def publication_lag_hours(con: duckdb.DuckDBPyConnection) -> tuple[float | None, ...]:
    """(p50, p90, share over LATE_PUBLISH_HOURS, n) of created -> first published, in hours."""
    return con.execute(
        f"""
        SELECT
            quantile_cont(h, 0.5), quantile_cont(h, 0.9),
            avg(CASE WHEN h > {LATE_PUBLISH_HOURS} THEN 1.0 ELSE 0.0 END), count(*)
        FROM (
            SELECT date_diff('second', created_at_utc, published_at_utc) / 3600.0 AS h
            FROM staging.stg_service_requests
            WHERE published_at_utc IS NOT NULL
        )
        """
    ).fetchone()


def check_freshness(con: duckdb.DuckDBPyConnection, as_of: datetime) -> CheckResult:
    max_updated = con.execute(
        "SELECT max(source_updated_at_utc) FROM staging.stg_service_requests"
    ).fetchone()[0]
    if max_updated is None:
        return CheckResult("freshness", "fail", "no rows in staging", {})
    lag_hours = (as_of - max_updated).total_seconds() / 3600
    if lag_hours > FRESHNESS_FAIL_HOURS:
        status = "fail"
    elif lag_hours > FRESHNESS_WARN_HOURS:
        status = "warn"
    else:
        status = "pass"
    return CheckResult(
        "freshness",
        status,
        f"latest source update {lag_hours:.1f} h before the run "
        f"(warn > {FRESHNESS_WARN_HOURS} h, fail > {FRESHNESS_FAIL_HOURS} h)",
        {
            "as_of_utc": as_of.isoformat(),
            "max_source_updated_at_utc": max_updated.isoformat(),
            "lag_hours": round(lag_hours, 2),
        },
    )


def volume_anomalies(daily: list[tuple[str, int]]) -> list[dict]:
    """Compare each day's count with the mean of the same weekday in up to 4 earlier weeks.

    311 volume has a strong weekly cycle (weekends differ from weekdays), so a
    same-weekday baseline lets the band be much tighter than a plain trailing
    mean. `daily` holds (ISO day, count) for complete days only; days with
    fewer than VOLUME_MIN_WEEKS earlier same-weekday counts are skipped.
    """
    counts = dict(daily)
    results = []
    for day, count in daily:
        d = date.fromisoformat(day)
        history = [
            counts[prev]
            for weeks in range(1, VOLUME_WEEKS + 1)
            if (prev := (d - timedelta(weeks=weeks)).isoformat()) in counts
        ]
        if len(history) < VOLUME_MIN_WEEKS:
            continue
        baseline = mean(history)
        ratio = count / baseline if baseline else float("inf")
        results.append(
            {
                "day": day,
                "count": count,
                "baseline_mean": round(baseline, 1),
                "baseline_weeks": len(history),
                "ratio": round(ratio, 3),
                "flagged": abs(ratio - 1) > VOLUME_BAND,
            }
        )
    return results


def complete_days(con: duckdb.DuckDBPyConnection) -> tuple[list[tuple[str, int]], float | None]:
    """Daily totals for days that should be fully published by now.

    A day counts as complete when it ended (midnight in New York) at least the
    p90 publication lag before the source's latest update; later days are still
    filling in and would look like a drop.
    """
    p90_lag = publication_lag_hours(con)[1]
    if p90_lag is None:
        return [], None
    rows = con.execute(
        """
        SELECT created_day::varchar, sum(n_requests)::bigint
        FROM marts.fct_daily_volume
        WHERE {day_end_utc} + to_seconds($lag_s)
              <= (SELECT max(source_updated_at_utc) FROM staging.stg_service_requests)
        GROUP BY 1 ORDER BY 1
        """.format(
            day_end_utc="timezone('UTC', timezone('America/New_York', "
            "(created_day + 1)::timestamp))"
        ),
        {"lag_s": round(p90_lag * 3600)},
    ).fetchall()
    return rows, p90_lag


def check_volume(con: duckdb.DuckDBPyConnection) -> CheckResult:
    days, p90_lag = complete_days(con)
    evaluated = volume_anomalies(days)
    if not evaluated:
        return CheckResult(
            "volume_anomaly", "info", "not enough complete days to evaluate", {"days": len(days)}
        )
    latest = evaluated[-1]
    flagged_days = [r["day"] for r in evaluated if r["flagged"]]
    status = "warn" if latest["flagged"] else "pass"
    return CheckResult(
        "volume_anomaly",
        status,
        f"{latest['day']}: {latest['count']} requests = {latest['ratio']:.2f}x the mean of the "
        f"same weekday in the {latest['baseline_weeks']} preceding weeks; "
        f"{len(flagged_days)} of {len(evaluated)} evaluated days outside "
        f"+/-{int(VOLUME_BAND * 100)}% (complete = ended >= {p90_lag:.1f} h, the p90 "
        "publication lag, before the latest source update)",
        {
            "latest": latest,
            "complete_days": len(days),
            "last_complete_day": days[-1][0],
            "days_evaluated": len(evaluated),
            "flagged_days": flagged_days,
            "completeness_lag_hours": round(p90_lag, 2),
        },
    )


def check_duplicates(run: dict) -> CheckResult:
    """Share of rows the latest run received more than once.

    Duplicate keys inside the warehouse are impossible (unique_key is the raw
    table's primary key), so the useful signal is how often the source
    re-delivered a key while we were paging.
    """
    fetched = run["rows_fetched"]
    rate = run["rows_duplicate_in_run"] / fetched if fetched else 0.0
    return CheckResult(
        "duplicate_rate",
        "warn" if rate > DUPLICATE_WARN_RATE else "pass",
        f"{rate:.2%} of {fetched} rows fetched in the latest run were repeats "
        f"(warn > {DUPLICATE_WARN_RATE:.0%})",
        {
            "run_rows_fetched": fetched,
            "run_duplicate_rows": run["rows_duplicate_in_run"],
            "run_duplicate_rate": rate,
        },
    )


def check_source_count(
    con: duckdb.DuckDBPyConnection, run: dict, session: requests.Session | None
) -> CheckResult:
    """Warehouse row count vs the API's own count(*) for the same window.

    Catches rows the incremental path cannot see (for example requests deleted
    upstream, which never get a newer :updated_at) and any paging gap.
    """
    if run["source"] != "api" or session is None:
        return CheckResult("source_count", "info", "not applicable (fixture run or offline)", {})
    try:
        response = session.get(
            DATASET_URL,
            params={
                "$select": "count(*) AS n",
                "$where": f"created_date >= '{run['window_start']}'",
            },
            timeout=120,
        )
        response.raise_for_status()
        source_rows = int(response.json()[0]["n"])
    except requests.RequestException as exc:
        # An unreachable API is not a data problem; report it without failing.
        return CheckResult("source_count", "warn", f"could not query the source: {exc}", {})
    warehouse_rows = con.execute("SELECT count(*) FROM raw.service_requests").fetchone()[0]
    diff = warehouse_rows - source_rows
    rate = abs(diff) / source_rows if source_rows else float(bool(diff))
    return CheckResult(
        "source_count",
        "warn" if rate > SOURCE_COUNT_WARN_RATE else "pass",
        f"warehouse {warehouse_rows} rows vs source {source_rows} (difference {diff:+d}, "
        f"warn above {SOURCE_COUNT_WARN_RATE:.1%})",
        {"warehouse_rows": warehouse_rows, "source_rows": source_rows, "difference": diff},
    )


def check_late_arriving(con: duckdb.DuckDBPyConnection, run: dict) -> CheckResult:
    """Two kinds of lateness, both informational.

    - publication lag: hours between the request being created and the row
      first appearing in the dataset (both in UTC).
    - late updates: rows the latest run changed that were already in the
      warehouse (_load_count > 1) and whose request was created more than
      7 days before the run (status changes, closures long after the fact).
      Only meaningful for incremental runs: a backfill or fixture load inserts
      every row for the first time.
    """
    p50, p90, share_late, n = publication_lag_hours(con)
    if p50 is None:
        return CheckResult("late_arriving", "info", "no published rows in staging", {"rows": 0})
    metrics = {
        "publication_lag_p50_hours": round(p50, 2),
        "publication_lag_p90_hours": round(p90, 2),
        "share_published_late": round(share_late, 4),
        "rows": n,
    }
    detail = (
        f"publication lag p50 {p50:.1f} h, p90 {p90:.1f} h; "
        f"{share_late:.1%} published > {LATE_PUBLISH_HOURS} h after creation; "
    )
    if run["mode"] != "incremental":
        detail += f"late updates not applicable to a {run['mode']} run"
    else:
        updated, updated_late = con.execute(
            f"""
            SELECT count(*),
                   count(*) FILTER (
                       WHERE created_at_utc < $as_of - INTERVAL {LATE_UPDATE_DAYS} DAY)
            FROM staging.stg_service_requests
            WHERE _last_run_id = $run_id AND _load_count > 1
            """,
            {"run_id": run["run_id"], "as_of": run["source_as_of"]},
        ).fetchone()
        metrics["latest_run_updates_of_existing_rows"] = updated
        metrics["latest_run_updates_created_over_7d_before"] = updated_late
        detail += (
            f"latest run updated {updated} existing requests, {updated_late} of them "
            f"created > {LATE_UPDATE_DAYS} days earlier"
        )
    return CheckResult("late_arriving", "info", detail, metrics)


def check_invalid_closed_dates(con: duckdb.DuckDBPyConnection) -> CheckResult:
    n_invalid, n = con.execute(
        "SELECT count(*) FILTER (WHERE has_invalid_closed_date), count(*) "
        "FROM staging.stg_service_requests"
    ).fetchone()
    return CheckResult(
        "closed_before_created",
        "info",
        f"{n_invalid} of {n} requests have closed_date < created_date "
        "(resolution time set to null)",
        {"invalid": n_invalid, "rows": n},
    )


def run_checks(con: duckdb.DuckDBPyConnection, session: requests.Session | None = None) -> dict:
    """Run every check. `session` is used only by the source-count check (API runs)."""
    run = latest_run(con)
    checks = [
        check_freshness(con, run["source_as_of"]),
        check_volume(con),
        check_duplicates(run),
        check_source_count(con, run, session),
        check_late_arriving(con, run),
        check_invalid_closed_dates(con),
    ]
    return {
        "run_id": run["run_id"],
        "source": run["source"],
        "mode": run["mode"],
        "source_as_of_utc": run["source_as_of"].isoformat(),
        "overall": "fail" if any(c.status == "fail" for c in checks) else "pass",
        "checks": [asdict(c) for c in checks],
    }


def to_markdown(report: dict) -> str:
    lines = [
        "# Data quality report",
        "",
        f"Run `{report['run_id']}` ({report['source']}, {report.get('mode', 'n/a')}), "
        f"source observed at {report['source_as_of_utc']} UTC. Overall: **{report['overall']}**.",
        "",
        "| Check | Status | Detail |",
        "|---|---|---|",
    ]
    lines += [f"| {c['name']} | {c['status']} | {c['detail']} |" for c in report["checks"]]
    return "\n".join(lines) + "\n"


def write_report(report: dict, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "quality_report.json").write_text(
        json.dumps(report, indent=2, default=str), encoding="utf-8"
    )
    (out_dir / "quality_report.md").write_text(to_markdown(report), encoding="utf-8")
