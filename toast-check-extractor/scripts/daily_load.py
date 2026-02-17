"""Incremental daily loader: finds and loads new files since last ETL run.

Checks etl_load_log for the last loaded date, scans the output directory
for new files, loads them, and refreshes materialized views.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

from backfill import find_daily_files
from loader import load_daily_file
from schema import create_schema, refresh_materialized_views


def get_last_loaded_date(conn, restaurant_name: str) -> date | None:
    """Get the most recent business_date that was successfully loaded."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT MAX(l.business_date)
               FROM etl_load_log l
               JOIN restaurants r ON r.restaurant_id = l.restaurant_id
               WHERE r.name = %s AND l.status = 'complete'""",
            (restaurant_name,),
        )
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def get_loaded_files(conn, restaurant_name: str) -> set[str]:
    """Get all filenames that have been successfully loaded."""
    with conn.cursor() as cur:
        cur.execute(
            """SELECT l.source_file
               FROM etl_load_log l
               JOIN restaurants r ON r.restaurant_id = l.restaurant_id
               WHERE r.name = %s AND l.status = 'complete'""",
            (restaurant_name,),
        )
        return {row[0] for row in cur.fetchall()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Incremental daily loader")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "output",
    )
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--restaurant", default="Quality Italian")
    parser.add_argument("--refresh-views", action="store_true", default=True)
    parser.add_argument("--no-refresh-views", dest="refresh_views", action="store_false")
    parser.add_argument("--dry-run", action="store_true", help="List files to load without loading")
    args = parser.parse_args()

    if not args.database_url:
        print("DATABASE_URL is required (--database-url or env var)", file=sys.stderr)
        return 1

    import psycopg

    with psycopg.connect(args.database_url) as conn:
        create_schema(conn)

        last_date = get_last_loaded_date(conn, args.restaurant)
        loaded_files = get_loaded_files(conn, args.restaurant)

        all_files = find_daily_files(args.output_dir)
        new_files = [f for f in all_files if f.name not in loaded_files]

        if not new_files:
            print(f"No new files to load (last loaded: {last_date})")
            return 0

        print(f"Found {len(new_files)} new files to load (last loaded: {last_date})")

        if args.dry_run:
            for f in new_files:
                print(f"  Would load: {f.name}")
            return 0

        loaded = 0
        errors = 0
        for file_path in new_files:
            try:
                result = load_daily_file(conn, file_path, args.restaurant)
                loaded += 1
                print(
                    f"Loaded {result['business_date']}: "
                    f"{result['checks_loaded']} checks, {result['items_loaded']} items "
                    f"({result['duration_sec']}s)"
                )
            except Exception as exc:
                errors += 1
                print(f"ERROR {file_path.name}: {exc}", file=sys.stderr)

        print(f"\nLoaded {loaded} files, {errors} errors")

        if args.refresh_views and loaded > 0:
            print("Refreshing materialized views...")
            refresh_materialized_views(conn)
            print("Views refreshed.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
