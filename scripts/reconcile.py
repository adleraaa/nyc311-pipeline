"""Compare the warehouse row count with the source's own count(*) for the same window.

This is the end-to-end check that keyset paging neither skipped nor duplicated
rows. Writes results/reconciliation.json.

Usage: python scripts/reconcile.py [--db data/warehouse.duckdb]
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from nyc311.socrata import DATASET_URL, make_session


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, default=Path("data/warehouse.duckdb"))
    parser.add_argument("--out", type=Path, default=Path("results/reconciliation.json"))
    args = parser.parse_args()

    con = duckdb.connect(str(args.db), read_only=True)
    run_id, window_start = con.execute(
        "SELECT run_id, window_start FROM raw.extract_runs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    local_rows, local_distinct = con.execute(
        "SELECT count(*), count(DISTINCT unique_key) FROM raw.service_requests"
    ).fetchone()

    response = make_session().get(
        DATASET_URL,
        params={"$select": "count(*) AS n", "$where": f"created_date >= '{window_start}'"},
        timeout=300,
    )
    response.raise_for_status()
    source_rows = int(response.json()[0]["n"])

    result = {
        "checked_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "latest_run_id": run_id,
        "window_start": window_start,
        "source_count": source_rows,
        "warehouse_rows": local_rows,
        "warehouse_distinct_keys": local_distinct,
        "difference": local_rows - source_rows,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
