"""Command line entry point: python -m nyc311 <command>."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path

from nyc311 import export, quality
from nyc311 import extract as extract_mod
from nyc311 import warehouse as wh
from nyc311.socrata import make_session

DEFAULT_DB = Path("data/warehouse.duckdb")


def cmd_extract(args: argparse.Namespace) -> int:
    con = wh.connect(args.db)
    started = time.perf_counter()
    stats = extract_mod.run_extract(
        con,
        make_session(),
        data_dir=args.db.parent,
        window_days=args.window_days,
        page_size=args.page_size,
    )
    elapsed = time.perf_counter() - started
    summary = {
        "run_id": stats.run_id,
        "pages": stats.pages,
        "rows_fetched": stats.rows_fetched,
        "rows_inserted": stats.rows_inserted,
        "rows_updated": stats.rows_updated,
        "rows_pruned": stats.rows_pruned,
        "rows_duplicate_in_run": stats.rows_duplicate_in_run,
        "seconds": round(elapsed, 1),
    }
    print(json.dumps(summary))
    if args.stats_out:
        args.stats_out.parent.mkdir(parents=True, exist_ok=True)
        args.stats_out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return 0


def cmd_load_fixture(args: argparse.Namespace) -> int:
    con = wh.connect(args.db)
    stats = extract_mod.load_fixture(con, args.path, data_dir=args.db.parent)
    print(
        json.dumps(
            {
                "run_id": stats.run_id,
                "rows_fetched": stats.rows_fetched,
                "rows_inserted": stats.rows_inserted,
                "rows_updated": stats.rows_updated,
            }
        )
    )
    return 0


def cmd_quality(args: argparse.Namespace) -> int:
    con = wh.connect(args.db)
    report = quality.run_checks(con)
    quality.write_report(report, args.out)
    print(quality.to_markdown(report))
    return 1 if report["overall"] == "fail" else 0


def cmd_build_site(args: argparse.Namespace) -> int:
    con = wh.connect(args.db)
    report = None
    if args.quality and args.quality.exists():
        report = json.loads(args.quality.read_text(encoding="utf-8"))
    written = export.build_site(con, args.out, report)
    print(f"wrote {len(written)} data files to {args.out / 'data'}")
    return 0


def cmd_summary(args: argparse.Namespace) -> int:
    """Headline numbers for the README, taken straight from the warehouse."""
    con = wh.connect(args.db)
    cur = con.execute("SELECT * FROM raw.extract_runs ORDER BY started_at")
    cols = [d[0] for d in cur.description]
    runs = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
    datasets = export.export_marts(con)
    citywide = con.execute(
        "SELECT count(*), quantile_cont(resolution_hours, 0.5),"
        " quantile_cont(resolution_hours, 0.9)"
        " FROM intermediate.int_requests_enriched WHERE resolution_hours IS NOT NULL"
    ).fetchone()
    summary = {
        "runs": runs,
        "meta": datasets["meta"],
        "closed_with_resolution_time": citywide[0],
        "citywide_median_resolution_hours": round(citywide[1], 2),
        "citywide_p90_resolution_hours": round(citywide[2], 2),
        "top_complaint_types": datasets["top_complaint_types"][:5],
        "fastest_agencies": datasets["resolution_by_agency"][:3],
        "slowest_agencies": datasets["resolution_by_agency"][-3:],
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    print(f"wrote {args.out}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="nyc311")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="DuckDB warehouse file")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("extract", help="incremental load from the Socrata API")
    p.add_argument("--window-days", type=int, default=30)
    p.add_argument("--page-size", type=int, default=20_000)
    p.add_argument("--stats-out", type=Path, help="also write run stats JSON here")
    p.set_defaults(func=cmd_extract)

    p = sub.add_parser("load-fixture", help="load a committed fixture through the same path")
    p.add_argument("path", type=Path)
    p.set_defaults(func=cmd_load_fixture)

    p = sub.add_parser("quality", help="run custom data-quality checks (after dbt build)")
    p.add_argument("--out", type=Path, default=Path("results"))
    p.set_defaults(func=cmd_quality)

    p = sub.add_parser("build-site", help="export marts to JSON and assemble the dashboard")
    p.add_argument("--out", type=Path, default=Path("_site"))
    p.add_argument("--quality", type=Path, default=Path("results/quality_report.json"))
    p.set_defaults(func=cmd_build_site)

    p = sub.add_parser("summary", help="write headline numbers for the README")
    p.add_argument("--out", type=Path, default=Path("results/pipeline_summary.json"))
    p.set_defaults(func=cmd_summary)

    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
