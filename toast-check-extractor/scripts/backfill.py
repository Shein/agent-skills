"""Backfill: load all historical daily JSON files into PostgreSQL.

Walks output/YYYY-MM/YYYY-MM-DD.json files and loads each sequentially.
Supports resuming by checking etl_load_log for already-loaded files.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from loader import load_daily_file
from schema import create_schema, refresh_materialized_views


def find_daily_files(output_dir: Path) -> list[Path]:
    """Find all daily JSON files sorted chronologically."""
    files = []
    for month_dir in sorted(output_dir.iterdir()):
        if not month_dir.is_dir():
            continue
        # Match YYYY-MM-DD.json or state_YYYY-MM-DD.json
        for json_file in sorted(month_dir.glob("*.json")):
            stem = json_file.stem.replace("state_", "")
            # Validate it looks like a date
            parts = stem.split("-")
            if len(parts) == 3 and len(parts[0]) == 4:
                files.append(json_file)
    return files


def get_loaded_files(conn, restaurant_id: int) -> set[str]:
    """Get set of filenames already loaded successfully."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT source_file FROM etl_load_log WHERE restaurant_id = %s AND status = 'complete'",
            (restaurant_id,),
        )
        return {row[0] for row in cur.fetchall()}


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill all historical data")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "output",
    )
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--restaurant", default="Quality Italian")
    parser.add_argument("--skip-existing", action="store_true", default=True,
                        help="Skip files already loaded (default: True)")
    parser.add_argument("--no-skip-existing", dest="skip_existing", action="store_false")
    parser.add_argument("--refresh-views", action="store_true", default=True,
                        help="Refresh materialized views after loading (default: True)")
    parser.add_argument("--no-refresh-views", dest="refresh_views", action="store_false")
    parser.add_argument("--limit", type=int, default=0, help="Max files to load (0=all)")
    args = parser.parse_args()

    if not args.database_url:
        print("DATABASE_URL is required (--database-url or env var)", file=sys.stderr)
        return 1

    import psycopg

    files = find_daily_files(args.output_dir)
    if not files:
        print(f"No daily JSON files found in {args.output_dir}")
        return 0

    print(f"Found {len(files)} daily files")

    with psycopg.connect(args.database_url) as conn:
        # Ensure schema exists
        create_schema(conn)

        # Get already-loaded files for skip logic
        loaded_files: set[str] = set()
        if args.skip_existing:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT restaurant_id FROM restaurants WHERE name = %s",
                    (args.restaurant,),
                )
                row = cur.fetchone()
                if row:
                    loaded_files = get_loaded_files(conn, row[0])

        total = len(files)
        loaded = 0
        skipped = 0
        errors = 0
        start_time = time.monotonic()

        for i, file_path in enumerate(files):
            if args.limit and loaded >= args.limit:
                print(f"Reached limit of {args.limit} files")
                break

            if args.skip_existing and file_path.name in loaded_files:
                skipped += 1
                continue

            try:
                result = load_daily_file(conn, file_path, args.restaurant)
                loaded += 1
                elapsed = time.monotonic() - start_time
                rate = loaded / elapsed if elapsed > 0 else 0
                eta = (total - i - 1) / rate if rate > 0 else 0
                print(
                    f"[{i+1}/{total}] {result['business_date']}: "
                    f"{result['checks_loaded']} checks, {result['items_loaded']} items "
                    f"({result['duration_sec']}s) "
                    f"[{rate:.1f} files/s, ETA {eta:.0f}s]"
                )
            except Exception as exc:
                conn.rollback()
                errors += 1
                print(f"[{i+1}/{total}] ERROR {file_path.name}: {exc}", file=sys.stderr)
                # Log error in etl_load_log
                try:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT restaurant_id FROM restaurants WHERE name = %s",
                            (args.restaurant,),
                        )
                        row = cur.fetchone()
                        if row:
                            cur.execute(
                                """INSERT INTO etl_load_log (restaurant_id, business_date, source_file, status, error_message)
                                   VALUES (%s, %s, %s, 'error', %s)
                                   ON CONFLICT (restaurant_id, business_date, source_file) DO UPDATE SET
                                       status = 'error', error_message = EXCLUDED.error_message""",
                                (row[0], file_path.stem.replace("state_", ""), file_path.name, str(exc)),
                            )
                    conn.commit()
                except Exception:
                    conn.rollback()

        elapsed = time.monotonic() - start_time
        print(f"\nBackfill complete in {elapsed:.1f}s")
        print(f"  Loaded:  {loaded}")
        print(f"  Skipped: {skipped}")
        print(f"  Errors:  {errors}")

        if args.refresh_views and loaded > 0:
            print("Refreshing materialized views...")
            refresh_materialized_views(conn)
            print("Views refreshed.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
