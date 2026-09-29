"""Reconcile the warehouse with the source, key by key, for the latest run's window.

Equal row counts alone would not prove that paging skipped nothing: a missing
row could be offset by an extra one (for example a request deleted upstream,
which the incremental path never sees). So this script downloads the source's
full list of unique_keys for the window (keys only, about 2 MB per 100k rows)
and diffs it against the warehouse, and also compares counts per created day.

Run it right after an extract: rows the source published after that extract
show up as "missing".

Usage: python -m scripts.reconcile [--db data/warehouse.duckdb] [--out out/reconciliation.json]
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import requests

from nyc311.socrata import DATASET_URL, make_session

KEY_PAGE_SIZE = 50_000
SAMPLE = 10


def source_keys(session: requests.Session, window_start: str) -> set[str]:
    """All unique_keys created since window_start, keyset-paged on unique_key."""
    keys: set[str] = set()
    last = None
    while True:
        where = f"created_date >= '{window_start}'"
        if last is not None:
            where += f" AND unique_key > '{last}'"
        response = session.get(
            DATASET_URL,
            params={
                "$select": "unique_key",
                "$where": where,
                "$order": "unique_key",
                "$limit": str(KEY_PAGE_SIZE),
            },
            timeout=300,
        )
        response.raise_for_status()
        rows = response.json()
        keys.update(r["unique_key"] for r in rows)
        if len(rows) < KEY_PAGE_SIZE:
            return keys
        last = rows[-1]["unique_key"]


def source_daily_counts(session: requests.Session, window_start: str) -> dict[str, int]:
    response = session.get(
        DATASET_URL,
        params={
            "$select": "date_trunc_ymd(created_date) AS day, count(*) AS n",
            "$where": f"created_date >= '{window_start}'",
            "$group": "day",
            "$order": "day",
            "$limit": "1000",
        },
        timeout=300,
    )
    response.raise_for_status()
    return {r["day"][:10]: int(r["n"]) for r in response.json()}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=Path("data/warehouse.duckdb"))
    parser.add_argument("--out", type=Path, default=Path("out/reconciliation.json"))
    args = parser.parse_args()

    con = duckdb.connect(str(args.db), read_only=True)
    run_id, window_start = con.execute(
        "SELECT run_id, window_start FROM raw.extract_runs WHERE source = 'api' "
        "ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    warehouse_keys = {
        k for (k,) in con.execute("SELECT unique_key FROM raw.service_requests").fetchall()
    }
    warehouse_daily = dict(
        con.execute(
            "SELECT left(created_date, 10), count(*)::int FROM raw.service_requests GROUP BY 1"
        ).fetchall()
    )

    session = make_session()
    keys = source_keys(session, window_start)
    daily = source_daily_counts(session, window_start)

    missing = sorted(keys - warehouse_keys)
    extra = sorted(warehouse_keys - keys)
    day_diffs = {
        day: warehouse_daily.get(day, 0) - daily.get(day, 0)
        for day in sorted(set(daily) | set(warehouse_daily))
        if warehouse_daily.get(day, 0) != daily.get(day, 0)
    }
    result = {
        "checked_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latest_run_id": run_id,
        "window_start": window_start,
        "source_keys": len(keys),
        "warehouse_rows": len(warehouse_keys),
        "missing_in_warehouse": len(missing),
        "missing_sample": missing[:SAMPLE],
        "extra_in_warehouse": len(extra),
        "extra_sample": extra[:SAMPLE],
        "days_compared": len(set(daily) | set(warehouse_daily)),
        "days_with_count_difference": day_diffs,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
