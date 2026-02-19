#!/usr/bin/env python3
"""Re-extract and reload specific gap dates from Toast into PostgreSQL.

For each date:
1. Runs toast_extract.py to scrape Toast and write the daily JSON file.
2. Clears old ETL log + check data for that date.
3. Reloads the fresh JSON via loader.load_daily_file().
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from datetime import date
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
EXTRACT_SCRIPT = SCRIPT_DIR / "toast_extract.py"
OUTPUT_DIR = PROJECT_DIR / "output"

# All gap dates: 18 completely missing + 8 partial (only 20 checks loaded)
GAP_DATES = [
    # Completely missing from DB
    "2025-01-10", "2025-02-04", "2025-03-03", "2025-04-27",
    "2025-05-20", "2025-06-11", "2025-06-17", "2025-07-08",
    "2025-07-21", "2025-07-24", "2025-07-26", "2025-08-29",
    "2025-09-25", "2025-12-25", "2025-12-28", "2025-12-29",
    "2025-12-30", "2025-12-31",
    # Partial data (only ~20 checks in DB, should be 100+)
    "2025-02-01", "2025-04-29", "2025-05-04", "2025-05-05",
    "2025-05-08", "2025-06-18", "2025-07-25", "2025-08-26",
]


def daily_json_path(d: str) -> Path:
    month = d[:7]
    return OUTPUT_DIR / month / f"{d}.json"


def extract_date(d: str, env_file: str, user_data_dir: str, headless: bool) -> bool:
    """Run toast_extract.py for a single date. Returns True on success."""
    output_path = daily_json_path(d)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    state_file = OUTPUT_DIR / f"gap_state_{d}.json"
    menu_file = OUTPUT_DIR / f"gap_menu_{d}.json"
    progress_file = OUTPUT_DIR / f"gap_progress_{d}.json"
    error_log = OUTPUT_DIR / f"gap_errors_{d}.jsonl"

    cmd = [
        sys.executable, str(EXTRACT_SCRIPT),
        "--start-date", d,
        "--end-date", d,
        "--state-file", str(state_file),
        "--menu-summary-file", str(menu_file),
        "--progress-file", str(progress_file),
        "--error-log-file", str(error_log),
        "--env-file", env_file,
        "--user-data-dir", user_data_dir,
        "--browser-channel", "chrome",
        "--workers", "6",
        "--max-pages", "0",
        "--limit", "0",
        "--auth-block-restarts", "2",
        "--auth-block-cooldown-sec", "90",
        "--challenge-timeout-sec", "120",
        "--human-min-delay-ms", "250",
        "--human-max-delay-ms", "900",
        "--detail-start-min-interval-ms", "700",
        "--combined-output", str(output_path),
        "--skip-metadata",  # We want fresh metadata for just this date
    ]
    # Actually we DO need metadata (that's the payments table scrape)
    # Remove --skip-metadata
    cmd = [x for x in cmd if x != "--skip-metadata"]

    if headless:
        cmd.append("--headless")

    print(f"\n{'='*60}", flush=True)
    print(f"EXTRACTING: {d}", flush=True)
    print(f"Output: {output_path}", flush=True)
    print(f"{'='*60}", flush=True)

    result = subprocess.run(cmd, cwd=str(PROJECT_DIR))

    # Cleanup temp files
    for tmp in [state_file, menu_file, progress_file, error_log]:
        tmp.unlink(missing_ok=True)

    if result.returncode != 0:
        print(f"EXTRACTION FAILED for {d} (exit code {result.returncode})", file=sys.stderr, flush=True)
        return False

    # Verify the output file has checks
    if output_path.exists():
        data = json.loads(output_path.read_text(encoding="utf-8"))
        checks = data.get("checks", [])
        print(f"EXTRACTED: {d} -> {len(checks)} checks", flush=True)
        return len(checks) > 0
    else:
        print(f"NO OUTPUT FILE for {d}", file=sys.stderr, flush=True)
        return False


def clear_and_reload(d: str, database_url: str, restaurant: str) -> dict | None:
    """Clear old data for a date and reload from the fresh JSON."""
    import psycopg
    from loader import load_daily_file

    file_path = daily_json_path(d)
    if not file_path.exists():
        print(f"SKIP RELOAD: no JSON for {d}", file=sys.stderr, flush=True)
        return None

    business_date = date.fromisoformat(d)

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            # Get restaurant_id
            cur.execute("SELECT restaurant_id FROM restaurants WHERE name = %s", (restaurant,))
            row = cur.fetchone()
            if not row:
                print(f"Restaurant '{restaurant}' not found", file=sys.stderr, flush=True)
                return None
            restaurant_id = row[0]

            # Delete old checks and their children (cascading FK)
            cur.execute(
                "DELETE FROM checks WHERE restaurant_id = %s AND business_date = %s",
                (restaurant_id, business_date),
            )
            deleted = cur.rowcount

            # Delete old ETL log entry
            cur.execute(
                "DELETE FROM etl_load_log WHERE restaurant_id = %s AND business_date = %s",
                (restaurant_id, business_date),
            )

            # Delete old menu item daily summary
            cur.execute(
                "DELETE FROM menu_item_daily_summary WHERE restaurant_id = %s AND business_date = %s",
                (restaurant_id, business_date),
            )

        conn.commit()
        print(f"CLEARED: {d} (deleted {deleted} old checks)", flush=True)

        # Reload
        result = load_daily_file(conn, file_path, restaurant)
        print(
            f"LOADED: {d} -> {result['checks_loaded']} checks, "
            f"{result['items_loaded']} items ({result['duration_sec']}s)",
            flush=True,
        )
        return result


def main() -> int:
    parser = argparse.ArgumentParser(description="Re-extract and reload gap dates")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", "postgresql://localhost:5432/agm"))
    parser.add_argument("--restaurant", default="Quality Italian")
    parser.add_argument("--env-file", default=str(PROJECT_DIR / ".env"))
    parser.add_argument("--user-data-dir", default=str(PROJECT_DIR / ".toast_browser_profile"))
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--extract-only", action="store_true", help="Only extract, don't load")
    parser.add_argument("--load-only", action="store_true", help="Only load existing JSONs, don't extract")
    parser.add_argument("--dates", nargs="*", help="Specific dates to process (default: all gaps)")
    parser.add_argument("--refresh-views", action="store_true", default=True)
    args = parser.parse_args()

    dates = args.dates if args.dates else GAP_DATES
    print(f"Processing {len(dates)} gap dates", flush=True)

    start_time = time.monotonic()
    extract_ok = 0
    extract_fail = 0
    load_ok = 0
    load_fail = 0

    for i, d in enumerate(dates):
        print(f"\n[{i+1}/{len(dates)}] Processing {d}...", flush=True)

        if not args.load_only:
            success = extract_date(d, args.env_file, args.user_data_dir, args.headless)
            if success:
                extract_ok += 1
            else:
                extract_fail += 1
                if not args.extract_only:
                    print(f"Skipping load for {d} due to extraction failure", flush=True)
                    load_fail += 1
                continue

        if not args.extract_only:
            try:
                result = clear_and_reload(d, args.database_url, args.restaurant)
                if result:
                    load_ok += 1
                else:
                    load_fail += 1
            except Exception as exc:
                print(f"LOAD FAILED for {d}: {exc}", file=sys.stderr, flush=True)
                load_fail += 1

    elapsed = time.monotonic() - start_time

    # Refresh materialized views if we loaded anything
    if args.refresh_views and load_ok > 0 and not args.extract_only:
        print("\nRefreshing materialized views...", flush=True)
        import psycopg
        from schema import refresh_materialized_views
        with psycopg.connect(args.database_url) as conn:
            refresh_materialized_views(conn)
        print("Views refreshed.", flush=True)

    print(f"\n{'='*60}", flush=True)
    print(f"BACKFILL GAPS COMPLETE in {elapsed:.0f}s", flush=True)
    if not args.load_only:
        print(f"  Extracted: {extract_ok} ok, {extract_fail} failed", flush=True)
    if not args.extract_only:
        print(f"  Loaded:    {load_ok} ok, {load_fail} failed", flush=True)
    print(f"{'='*60}", flush=True)

    return 0 if extract_fail == 0 and load_fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
