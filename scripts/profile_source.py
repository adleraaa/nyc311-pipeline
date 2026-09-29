"""Profile how the source publishes updates: rows per distinct :updated_at value.

This is the evidence behind keyset paging on (:updated_at, unique_key): if
many rows share one timestamp, a watermark on the timestamp alone cannot mark
a page boundary. Writes results/source_update_batches.json.

Usage: python scripts/profile_source.py [--days 10]
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

from nyc311.socrata import DATASET_URL, make_session


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=10)
    parser.add_argument("--out", type=Path, default=Path("results/source_update_batches.json"))
    args = parser.parse_args()

    now = datetime.now(UTC)
    since = (now - timedelta(days=args.days)).strftime("%Y-%m-%dT00:00:00")
    response = make_session().get(
        DATASET_URL,
        params={
            "$select": ":updated_at, count(*) AS n",
            "$where": f":updated_at >= '{since}'",
            "$group": ":updated_at",
            "$order": "n DESC",
            "$limit": "5000",
        },
        timeout=600,
    )
    response.raise_for_status()
    batches = [{"updated_at": r[":updated_at"], "rows": int(r["n"])} for r in response.json()]
    total = sum(b["rows"] for b in batches)
    result = {
        "checked_at_utc": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "updated_since": since,
        "distinct_updated_at_values": len(batches),
        "rows_updated": total,
        "largest_batches": batches[:10],
        "share_of_rows_in_largest_10_batches": round(
            sum(b["rows"] for b in batches[:10]) / total, 4
        )
        if total
        else None,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k != "largest_batches"}))


if __name__ == "__main__":
    main()
