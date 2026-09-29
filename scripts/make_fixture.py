"""Regenerate the committed CI fixture: a ~1% sample of the last 30 days.

The sample keeps requests whose unique_key ends in "00". Keys are assigned
sequentially, so this is close to a uniform systematic sample across days,
boroughs and agencies, which keeps every mart populated in CI.

Usage: python scripts/make_fixture.py [--out tests/fixtures/sample_311.json.gz]
"""

from __future__ import annotations

import argparse
import gzip
import json
from datetime import UTC, datetime
from pathlib import Path

from nyc311.socrata import DATASET_URL, SOURCE_COLUMNS, make_session
from nyc311.warehouse import window_start_for


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=Path("tests/fixtures/sample_311.json.gz"))
    parser.add_argument("--window-days", type=int, default=30)
    args = parser.parse_args()

    fetched_at = datetime.now(UTC)
    params = {
        "$select": ",".join(SOURCE_COLUMNS),
        "$where": f"created_date >= '{window_start_for(fetched_at, args.window_days)}' "
        "AND unique_key like '%00'",
        "$order": ":updated_at, unique_key",
        "$limit": "10000",
    }
    response = make_session().get(DATASET_URL, params=params, timeout=600)
    response.raise_for_status()
    rows = response.json()
    payload = {
        "source": DATASET_URL,
        "sample": "unique_key like '%00'",
        "fetched_at": fetched_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "rows": rows,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    # mtime=0 keeps the gzip bytes identical for identical content.
    with gzip.GzipFile(args.out, "wb", mtime=0) as fh:
        fh.write(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    print(f"wrote {len(rows)} rows to {args.out}")


if __name__ == "__main__":
    main()
