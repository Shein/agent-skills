#!/usr/bin/env python3
"""Run toast_extract.py for a date range with resume, retry, and adaptive throttling."""

from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Orchestrate toast_extract.py across a date range with resume and retry support."
    )
    parser.add_argument("--start-date", required=True, help="First date to extract (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="Last date to extract (YYYY-MM-DD)")
    parser.add_argument("--output-dir", default="output", help="Root output directory (default: output/)")
    parser.add_argument("--resume", action="store_true", help="Skip days that already have a complete output file")
    parser.add_argument("--cooldown", type=float, default=5.0, help="Base seconds to sleep between days (default: 5)")
    parser.add_argument("--max-retries", type=int, default=2, help="Max retries per failed day (default: 2)")
    parser.add_argument(
        "--adaptive-cooldown",
        action="store_true",
        help="Double cooldown after throttle signals, reset after 3 clean days",
    )
    parser.add_argument(
        "--extract-script",
        default=str(Path(__file__).resolve().parent / "toast_extract.py"),
        help="Path to toast_extract.py (auto-detected by default)",
    )
    return parser.parse_args()


def date_range(start: date, end: date):
    """Yield each date from start to end inclusive."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def output_path_for_date(output_dir: str, d: date) -> Path:
    """Return output/<YYYY-MM>/<YYYY-MM-DD>.json."""
    return Path(output_dir) / d.strftime("%Y-%m") / f"{d.isoformat()}.json"


def is_day_complete(path: Path) -> bool:
    """Check if the output file exists and contains a checks array."""
    if not path.exists():
        return False
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return isinstance(data.get("checks"), list)
    except (json.JSONDecodeError, OSError):
        return False


def run_day(d: date, args: argparse.Namespace, tmp_dir: Path) -> dict:
    """Run toast_extract.py for a single day. Returns a stats dict."""
    date_str = d.isoformat()
    out_path = output_path_for_date(args.output_dir, d)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    state_file = tmp_dir / f"state_{date_str}.json"
    menu_file = tmp_dir / f"menu_{date_str}.json"

    cmd = [
        sys.executable,
        args.extract_script,
        "--start-date", date_str,
        "--end-date", date_str,
        "--state-file", str(state_file),
        "--menu-summary-file", str(menu_file),
        "--headless",
        "--combined-output", str(out_path),
    ]

    start_time = time.monotonic()
    result = subprocess.run(cmd, capture_output=True, text=True)
    elapsed = time.monotonic() - start_time

    stdout = result.stdout or ""
    stderr = result.stderr or ""
    combined_output = stdout + "\n" + stderr

    # Parse run_complete event from stdout
    stats: dict = {
        "date": date_str,
        "exit_code": result.returncode,
        "elapsed_sec": round(elapsed, 1),
        "total": 0,
        "complete": 0,
        "incomplete": 0,
        "throttled": False,
        "error": "",
    }

    for line in stdout.splitlines():
        if '"event": "run_complete"' in line or '"event":"run_complete"' in line:
            try:
                event = json.loads(line)
                stats["total"] = event.get("total", 0)
                stats["complete"] = event.get("complete", 0)
                stats["incomplete"] = event.get("incomplete", 0)
            except json.JSONDecodeError:
                pass

    # Detect throttle/error signals
    throttle_keywords = ["throttl", "rate limit", "429", "too many", "cloudflare", "AUTH_BLOCKED"]
    for kw in throttle_keywords:
        if kw.lower() in combined_output.lower():
            stats["throttled"] = True
            break

    if result.returncode != 0:
        # Capture last few lines of stderr for error context
        err_lines = [l for l in stderr.strip().splitlines() if l.strip()]
        stats["error"] = (err_lines[-1] if err_lines else f"exit code {result.returncode}")[:200]

    return stats


def append_log(log_path: Path, record: dict) -> None:
    """Append a JSON record to the run log."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=True) + "\n")


def format_duration(seconds: float) -> str:
    """Format seconds as Xm Ys."""
    m = int(seconds) // 60
    s = int(seconds) % 60
    return f"{m}m{s}s"


def main() -> None:
    args = parse_args()
    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)

    if start > end:
        print(f"Error: start-date {start} is after end-date {end}", file=sys.stderr)
        sys.exit(1)

    log_path = Path(args.output_dir) / "run_range_log.jsonl"
    days = list(date_range(start, end))

    # Print header
    header = f"{'Date':<12} {'Total':>8} {'Complete':>8} {'Errors':>8} {'Duration':>10} {'Notes'}"
    sep = f"{'----------':<12} {'------':>8} {'--------':>8} {'------':>8} {'--------':>10} {'-----'}"
    print(header)
    print(sep)

    grand_total = 0
    grand_complete = 0
    grand_incomplete = 0
    grand_start = time.monotonic()

    cooldown = args.cooldown
    clean_streak = 0

    with tempfile.TemporaryDirectory(prefix="toast_range_") as tmp_dir:
        tmp_path = Path(tmp_dir)

        for i, d in enumerate(days):
            out_path = output_path_for_date(args.output_dir, d)

            # Resume: skip completed days
            if args.resume and is_day_complete(out_path):
                try:
                    data = json.loads(out_path.read_text(encoding="utf-8"))
                    n_checks = len(data.get("checks", []))
                except (json.JSONDecodeError, OSError):
                    n_checks = 0
                notes = "SKIPPED (resume)"
                print(f"{d.isoformat():<12} {n_checks:>8} {n_checks:>8} {0:>8} {'--':>10} {notes}")
                grand_total += n_checks
                grand_complete += n_checks
                append_log(log_path, {
                    "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.gmtime()),
                    "event": "day_skipped",
                    "date": d.isoformat(),
                    "checks": n_checks,
                })
                continue

            # Run with retries
            stats: dict = {}
            for attempt in range(1, args.max_retries + 2):  # max_retries + 1 total attempts
                stats = run_day(d, args, tmp_path)

                if stats["exit_code"] == 0:
                    break

                if attempt <= args.max_retries:
                    backoff = cooldown * (2 ** (attempt - 1)) + random.uniform(0, 2)
                    append_log(log_path, {
                        "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.gmtime()),
                        "event": "day_retry",
                        "date": d.isoformat(),
                        "attempt": attempt,
                        "backoff_sec": round(backoff, 1),
                        "error": stats.get("error", ""),
                    })
                    time.sleep(backoff)

            # Build notes
            notes_parts: list[str] = []
            if stats.get("throttled"):
                notes_parts.append("THROTTLE")
            if stats.get("error"):
                notes_parts.append(f"ERR: {stats['error'][:60]}")
            if stats.get("incomplete", 0) > 0:
                notes_parts.append(f"INCOMPLETE({stats['incomplete']})")
            notes = " ".join(notes_parts)

            duration_str = format_duration(stats["elapsed_sec"])
            print(
                f"{d.isoformat():<12} {stats['total']:>8} {stats['complete']:>8} "
                f"{stats['incomplete']:>8} {duration_str:>10} {notes}"
            )

            grand_total += stats["total"]
            grand_complete += stats["complete"]
            grand_incomplete += stats["incomplete"]

            append_log(log_path, {
                "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.gmtime()),
                "event": "day_done",
                "date": d.isoformat(),
                **stats,
            })

            # Adaptive cooldown
            if args.adaptive_cooldown:
                if stats.get("throttled"):
                    cooldown = min(cooldown * 2, 120)
                    clean_streak = 0
                else:
                    clean_streak += 1
                    if clean_streak >= 3:
                        cooldown = max(args.cooldown, cooldown / 2)
                        clean_streak = 0

            # Sleep between days (skip after last day)
            if i < len(days) - 1:
                jitter = random.uniform(0, min(3, cooldown * 0.3))
                time.sleep(cooldown + jitter)

    grand_elapsed = time.monotonic() - grand_start
    print()
    print(
        f"{'TOTALS':<12} {grand_total:>8} {grand_complete:>8} "
        f"{grand_incomplete:>8} {format_duration(grand_elapsed):>10}"
    )

    append_log(log_path, {
        "ts": time.strftime("%Y-%m-%dT%H:%M:%S%z", time.gmtime()),
        "event": "range_done",
        "start_date": args.start_date,
        "end_date": args.end_date,
        "grand_total": grand_total,
        "grand_complete": grand_complete,
        "grand_incomplete": grand_incomplete,
        "grand_elapsed_sec": round(grand_elapsed, 1),
    })


if __name__ == "__main__":
    main()
