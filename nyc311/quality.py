"""Data-quality checks that dbt's column tests do not cover.

Runs after `dbt build` against the warehouse and writes a JSON + Markdown
report. Each check returns pass / warn / fail / info; only `fail` makes the
CLI exit non-zero.

All "now"-dependent checks use the latest run's source_as_of (the time the
source was observed) rather than the wall clock, so re-running the checks on
an old warehouse or a committed fixture gives the same answer.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from statistics import mean, pstdev

import duckdb

FRESHNESS_WARN_HOURS = 36  # the source publishes once a day
FRESHNESS_FAIL_HOURS = 72
VOLUME_TRAILING_DAYS = 28
VOLUME_MIN_HISTORY = 7
VOLUME_BAND = 0.5  # flag a day outside [0.5x, 1.5x] of the trailing mean
DUPLICATE_WARN_RATE = 0.01
LATE_PUBLISH_HOURS = 72


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
    """Compare each day's count with the mean of up to 28 preceding days.

    `daily` must be sorted by day and contain only complete days. Days with
    fewer than VOLUME_MIN_HISTORY days of history are skipped.
    """
    results = []
    for i, (day, count) in enumerate(daily):
        history = [c for _, c in daily[max(0, i - VOLUME_TRAILING_DAYS) : i]]
        if len(history) < VOLUME_MIN_HISTORY:
            continue
        trailing = mean(history)
        sd = pstdev(history)
        ratio = count / trailing if trailing else float("inf")
        results.append(
            {
                "day": day,
                "count": count,
                "trailing_mean": round(trailing, 1),
                "ratio": round(ratio, 3),
                "z": round((count - trailing) / sd, 2) if sd else None,
                "flagged": not (1 - VOLUME_BAND <= ratio <= 1 + VOLUME_BAND),
            }
        )
    return results


def check_volume(con: duckdb.DuckDBPyConnection) -> CheckResult:
    # The most recent created_day is usually partial (the source lags by up to a
    # day), so it is excluded as incomplete.
    rows = con.execute(
        """
        SELECT created_day::varchar, sum(n_requests)::bigint
        FROM marts.fct_daily_volume
        WHERE created_day < (SELECT max(created_day) FROM marts.fct_daily_volume)
        GROUP BY 1 ORDER BY 1
        """
    ).fetchall()
    evaluated = volume_anomalies(rows)
    if not evaluated:
        return CheckResult("volume_anomaly", "info", "not enough history to evaluate", {})
    latest = evaluated[-1]
    flagged_days = [r["day"] for r in evaluated if r["flagged"]]
    status = "warn" if latest["flagged"] else "pass"
    return CheckResult(
        "volume_anomaly",
        status,
        f"{latest['day']}: {latest['count']} requests = {latest['ratio']:.2f}x the trailing "
        f"{VOLUME_TRAILING_DAYS}-day mean; {len(flagged_days)} of {len(evaluated)} evaluated "
        f"days outside +/-{int(VOLUME_BAND * 100)}%",
        {"latest": latest, "days_evaluated": len(evaluated), "flagged_days": flagged_days},
    )


def check_duplicates(con: duckdb.DuckDBPyConnection, run: dict) -> CheckResult:
    total, distinct = con.execute(
        "SELECT count(*), count(DISTINCT unique_key) FROM raw.service_requests"
    ).fetchone()
    warehouse_dupes = total - distinct
    fetched = run["rows_fetched"]
    run_rate = run["rows_duplicate_in_run"] / fetched if fetched else 0.0
    if warehouse_dupes:
        status = "fail"
    elif run_rate > DUPLICATE_WARN_RATE:
        status = "warn"
    else:
        status = "pass"
    return CheckResult(
        "duplicate_rate",
        status,
        f"{warehouse_dupes} duplicate keys in the warehouse; "
        f"{run_rate:.2%} of rows fetched in the latest run were repeats",
        {
            "warehouse_duplicate_keys": warehouse_dupes,
            "run_rows_fetched": fetched,
            "run_duplicate_rows": run["rows_duplicate_in_run"],
            "run_duplicate_rate": run_rate,
        },
    )


def check_late_arriving(con: duckdb.DuckDBPyConnection, run: dict) -> CheckResult:
    """Two kinds of lateness, both informational.

    - publication lag: hours between created_at (NY local) and the row first
      appearing in the dataset (_sys_created_at, UTC). The 4-5 h offset between
      the two clocks is small next to the daily publishing cadence.
    - late updates: rows changed in the latest run whose request was created
      more than 7 days before the run (status changes, closures).
    """
    lag = con.execute(
        f"""
        SELECT
            quantile_cont(h, 0.5), quantile_cont(h, 0.9),
            avg(CASE WHEN h > {LATE_PUBLISH_HOURS} THEN 1.0 ELSE 0.0 END), count(*)
        FROM (
            SELECT date_diff('second', created_at, published_at_utc) / 3600.0 AS h
            FROM staging.stg_service_requests
            WHERE published_at_utc IS NOT NULL
        )
        """
    ).fetchone()
    late_updates = con.execute(
        """
        SELECT count(*) FILTER (WHERE _load_count > 1),
               count(*) FILTER (WHERE created_at < $as_of - INTERVAL 7 DAY)
        FROM staging.stg_service_requests
        WHERE _last_run_id = $run_id
        """,
        {"run_id": run["run_id"], "as_of": run["source_as_of"]},
    ).fetchone()
    p50, p90, share_late, n = lag
    return CheckResult(
        "late_arriving",
        "info",
        f"publication lag p50 {p50:.1f} h, p90 {p90:.1f} h; "
        f"{share_late:.1%} published > {LATE_PUBLISH_HOURS} h after creation; "
        f"latest run touched {late_updates[1]} requests created > 7 days earlier",
        {
            "publication_lag_p50_hours": round(p50, 2),
            "publication_lag_p90_hours": round(p90, 2),
            "share_published_late": round(share_late, 4),
            "rows": n,
            "latest_run_updates_of_existing_rows": late_updates[0],
            "latest_run_rows_created_over_7d_before": late_updates[1],
        },
    )


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


def run_checks(con: duckdb.DuckDBPyConnection) -> dict:
    run = latest_run(con)
    checks = [
        check_freshness(con, run["source_as_of"]),
        check_volume(con),
        check_duplicates(con, run),
        check_late_arriving(con, run),
        check_invalid_closed_dates(con),
    ]
    return {
        "run_id": run["run_id"],
        "source": run["source"],
        "source_as_of_utc": run["source_as_of"].isoformat(),
        "overall": "fail" if any(c.status == "fail" for c in checks) else "pass",
        "checks": [asdict(c) for c in checks],
    }


def to_markdown(report: dict) -> str:
    lines = [
        "# Data quality report",
        "",
        f"Run `{report['run_id']}` ({report['source']}), source observed at "
        f"{report['source_as_of_utc']} UTC. Overall: **{report['overall']}**.",
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
