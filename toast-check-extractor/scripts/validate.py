"""Post-load data quality validation checks.

Compares database contents against source JSON files to verify integrity.
Can be run after backfill or daily loads to catch issues.
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path
from typing import Any


def validate_day(
    conn: Any, file_path: Path, restaurant_name: str = "Quality Italian",
) -> dict:
    """Validate a single day's data against its source JSON file.

    Returns a dict with validation results and any issues found.
    """
    issues: list[str] = []

    # Load source data
    raw = json.loads(file_path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        source_checks = raw.get("checks") or []
    elif isinstance(raw, list):
        source_checks = raw
    else:
        return {"file": str(file_path), "status": "error", "issues": ["Unexpected JSON structure"]}

    # Count source records (only those with payment_id)
    source_payment_ids = {
        str(r.get("payment_id")).strip()
        for r in source_checks
        if isinstance(r, dict) and r.get("payment_id")
    }
    source_count = len(source_payment_ids)

    # Parse business date from filename
    stem = file_path.stem.replace("state_", "")
    try:
        business_date = date.fromisoformat(stem)
    except ValueError:
        return {"file": str(file_path), "status": "error", "issues": [f"Cannot parse date from {file_path.stem}"]}

    with conn.cursor() as cur:
        # Get restaurant_id
        cur.execute("SELECT restaurant_id FROM restaurants WHERE name = %s", (restaurant_name,))
        row = cur.fetchone()
        if not row:
            return {"file": str(file_path), "status": "error", "issues": ["Restaurant not found"]}
        restaurant_id = row[0]

        # Check count
        cur.execute(
            "SELECT COUNT(*) FROM checks WHERE restaurant_id = %s AND business_date = %s",
            (restaurant_id, business_date),
        )
        db_count = cur.fetchone()[0]
        if db_count != source_count:
            issues.append(f"Check count mismatch: source={source_count}, db={db_count}")

        # Duplicate payment_ids check
        cur.execute(
            """SELECT payment_id, COUNT(*) FROM checks
               WHERE restaurant_id = %s AND business_date = %s
               GROUP BY payment_id HAVING COUNT(*) > 1""",
            (restaurant_id, business_date),
        )
        dupes = cur.fetchall()
        if dupes:
            issues.append(f"Duplicate payment_ids: {[d[0] for d in dupes]}")

        # Payment total reconciliation
        cur.execute(
            """SELECT SUM(total) FROM checks
               WHERE restaurant_id = %s AND business_date = %s""",
            (restaurant_id, business_date),
        )
        db_total = cur.fetchone()[0] or 0

        # db_total is in cents; convert source to cents for comparison
        source_total_cents = sum(
            round(float(r.get("data", {}).get("total") or 0) * 100)
            for r in source_checks
            if isinstance(r, dict) and r.get("payment_id")
        )

        tolerance_cents = 100  # $1 tolerance for VOIDED/rounding
        diff_cents = abs(int(db_total) - source_total_cents)
        if diff_cents > tolerance_cents:
            issues.append(
                f"Total mismatch: source=${source_total_cents/100:.2f}, db=${int(db_total)/100:.2f} "
                f"(diff=${diff_cents/100:.2f})"
            )

        # Missing critical fields
        cur.execute(
            """SELECT COUNT(*) FROM checks
               WHERE restaurant_id = %s AND business_date = %s
               AND (server_name IS NULL OR time_opened IS NULL)""",
            (restaurant_id, business_date),
        )
        missing = cur.fetchone()[0]
        if missing > 0:
            issues.append(f"{missing} checks missing server_name or time_opened")

        # Items count check
        cur.execute(
            """SELECT SUM(array_length(
                   (SELECT array_agg(1) FROM check_items ci WHERE ci.check_id = c.check_id), 1
               ))
               FROM checks c
               WHERE c.restaurant_id = %s AND c.business_date = %s""",
            (restaurant_id, business_date),
        )
        # Simpler items count
        cur.execute(
            """SELECT COUNT(*) FROM check_items ci
               JOIN checks c ON c.check_id = ci.check_id
               WHERE c.restaurant_id = %s AND c.business_date = %s""",
            (restaurant_id, business_date),
        )
        db_items = cur.fetchone()[0]

        source_items = sum(
            len(r.get("data", {}).get("items") or [])
            for r in source_checks
            if isinstance(r, dict) and r.get("payment_id")
        )
        if db_items != source_items:
            issues.append(f"Items count mismatch: source={source_items}, db={db_items}")

    status = "pass" if not issues else "fail"
    return {
        "file": str(file_path),
        "business_date": business_date.isoformat(),
        "source_checks": source_count,
        "db_checks": db_count,
        "status": status,
        "issues": issues,
    }


def validate_all(
    conn: Any, output_dir: Path, restaurant_name: str = "Quality Italian",
    sample_size: int = 0,
) -> list[dict]:
    """Validate all loaded days. If sample_size > 0, validate a random sample."""
    from backfill import find_daily_files
    import random

    files = find_daily_files(output_dir)
    if sample_size > 0 and sample_size < len(files):
        files = random.sample(files, sample_size)

    results = []
    for f in files:
        result = validate_day(conn, f, restaurant_name)
        results.append(result)
        status_icon = "OK" if result["status"] == "pass" else "FAIL"
        print(f"  [{status_icon}] {result.get('business_date', f.name)}", end="")
        if result["issues"]:
            print(f" - {'; '.join(result['issues'])}")
        else:
            print()

    pass_count = sum(1 for r in results if r["status"] == "pass")
    fail_count = sum(1 for r in results if r["status"] == "fail")
    error_count = sum(1 for r in results if r["status"] == "error")
    print(f"\nValidation: {pass_count} pass, {fail_count} fail, {error_count} error out of {len(results)}")

    return results


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Validate loaded data against source JSON")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "output",
    )
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--restaurant", default="Quality Italian")
    parser.add_argument("--file", type=Path, help="Validate a single file")
    parser.add_argument("--sample", type=int, default=0, help="Validate a random sample of N days")
    args = parser.parse_args()

    if not args.database_url:
        print("DATABASE_URL is required", file=sys.stderr)
        raise SystemExit(1)

    import psycopg

    with psycopg.connect(args.database_url) as conn:
        if args.file:
            result = validate_day(conn, args.file, args.restaurant)
            print(json.dumps(result, indent=2))
        else:
            results = validate_all(conn, args.output_dir, args.restaurant, args.sample)
            fails = [r for r in results if r["status"] != "pass"]
            if fails:
                raise SystemExit(1)
