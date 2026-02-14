#!/usr/bin/env python3
"""Orchestrate Toast extraction with date parsing, output adapters, and background runs."""

from __future__ import annotations

import argparse
import json
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


SCRIPT_PATH = Path(__file__).resolve()
EXTRACT_SCRIPT = SCRIPT_PATH.parent / "toast_extract.py"

DEFAULT_STATE_FILE = "output/toast_checks_state.json"
DEFAULT_MENU_SUMMARY_FILE = "output/toast_menu_item_summary.json"
DEFAULT_JSON_OUTPUT_FILE = "output/toast/checks.json"
DEFAULT_RUN_DIR = "output/toast_runs"


@dataclass
class RunConfig:
    start_date: str
    end_date: str
    output_format: str
    output_path: str | None
    database_url: str | None
    date_query: str | None


class ConfigError(Exception):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def parse_iso_date(value: str, field_name: str) -> date:
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ConfigError(f"Invalid {field_name}: {value}. Expected YYYY-MM-DD.") from exc
    return parsed


def resolve_date_query(query: str, *, today: date) -> tuple[date, date]:
    text = (query or "").strip().lower()
    if not text:
        raise ConfigError("Date query cannot be empty.")

    match = re.search(r"\blast\s+(\d+)\s+days?\b", text)
    if match:
        days = int(match.group(1))
        if days <= 0:
            raise ConfigError("The number of days must be greater than 0.")
        return today - timedelta(days=days), today

    if "last week" in text:
        this_week_start = today - timedelta(days=today.weekday())
        last_week_start = this_week_start - timedelta(days=7)
        return last_week_start, this_week_start

    if "yesterday" in text:
        start = today - timedelta(days=1)
        return start, today

    if "today" in text:
        return today, today

    explicit = re.search(r"from\s+(\d{4}-\d{2}-\d{2})\s+to\s+(\d{4}-\d{2}-\d{2})", text)
    if explicit:
        start = parse_iso_date(explicit.group(1), "start date")
        end = parse_iso_date(explicit.group(2), "end date")
        return start, end

    raise ConfigError(
        "Unsupported date phrase. Use --start-date/--end-date or phrases like "
        "'last week', 'last 7 days', or 'yesterday'."
    )


def prompt_for_format() -> str:
    while True:
        choice = input("Output format (`JSON` or `SQL`): ").strip().lower()
        if choice in {"json", "sql"}:
            return choice
        print("Please enter JSON or SQL.", file=sys.stderr)


def prompt_for_json_path(default_path: str) -> str:
    value = input(f"JSON output path [{default_path}]: ").strip()
    if not value:
        return default_path
    return value


def prompt_for_database_url() -> str:
    return input("Database connection URL: ").strip()


def resolve_run_config(args: argparse.Namespace, *, allow_prompt: bool) -> RunConfig:
    if args.start_date and args.end_date:
        start_date = parse_iso_date(args.start_date, "start date")
        end_date = parse_iso_date(args.end_date, "end date")
    elif args.date_query:
        today = datetime.now().astimezone().date()
        start_date, end_date = resolve_date_query(args.date_query, today=today)
    else:
        raise ConfigError("Provide --start-date and --end-date, or --date-query.")

    if start_date > end_date:
        raise ConfigError("start_date cannot be later than end_date.")

    output_format = (args.format or "").strip().lower()
    if not output_format:
        if allow_prompt and sys.stdin.isatty():
            output_format = prompt_for_format()
        else:
            raise ConfigError("Missing output format. Provide --format json|sql.")

    if output_format not in {"json", "sql"}:
        raise ConfigError("--format must be json or sql.")

    output_path = args.output_path
    database_url = args.database_url

    if output_format == "json":
        if not output_path:
            if allow_prompt and sys.stdin.isatty():
                output_path = prompt_for_json_path(DEFAULT_JSON_OUTPUT_FILE)
            else:
                output_path = DEFAULT_JSON_OUTPUT_FILE
    else:
        if not database_url and allow_prompt and sys.stdin.isatty():
            database_url = prompt_for_database_url()
        if not database_url:
            raise ConfigError("SQL output requires --database-url. Aborting.")

    return RunConfig(
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
        output_format=output_format,
        output_path=output_path,
        database_url=database_url,
        date_query=args.date_query,
    )


def load_records(state_file: Path) -> list[dict[str, Any]]:
    if not state_file.exists():
        raise ConfigError(f"State file not found: {state_file}")
    raw = json.loads(state_file.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        records = [record for record in raw if isinstance(record, dict)]
    elif isinstance(raw, dict):
        records = [record for record in raw.values() if isinstance(record, dict)]
    else:
        raise ConfigError("State file has unexpected structure.")
    records.sort(key=lambda row: str(row.get("payment_id") or ""))
    return records


def load_menu_summary(menu_summary_file: Path) -> list[dict[str, Any]]:
    if not menu_summary_file.exists():
        return []
    raw = json.loads(menu_summary_file.read_text(encoding="utf-8"))
    if isinstance(raw, list):
        return [row for row in raw if isinstance(row, dict)]
    return []


def build_export_payload(
    records: list[dict[str, Any]],
    menu_summary: list[dict[str, Any]],
    config: RunConfig,
) -> dict[str, Any]:
    total = len(records)
    completed = sum(1 for row in records if row.get("complete"))
    errored = sum(1 for row in records if row.get("last_error"))
    return {
        "generated_at": utc_now(),
        "date_range": {
            "start_date": config.start_date,
            "end_date": config.end_date,
            "date_query": config.date_query,
        },
        "stats": {
            "total": total,
            "complete": completed,
            "incomplete": total - completed,
            "errored": errored,
        },
        "checks": records,
        "menu_item_summary": menu_summary,
    }


def export_to_json(payload: dict[str, Any], output_path: Path) -> None:
    ensure_parent_dir(output_path)
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def ensure_sql_schema(conn: Any) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS toast_checks (
                payment_id TEXT PRIMARY KEY,
                complete BOOLEAN NOT NULL,
                attempts INTEGER NOT NULL,
                last_error TEXT,
                extracted_at TIMESTAMPTZ,
                parsed_url TEXT,
                check_number INTEGER,
                time_opened TEXT,
                guest_count INTEGER,
                server_name TEXT,
                table_name TEXT,
                discount NUMERIC,
                subtotal NUMERIC,
                tax NUMERIC,
                tip NUMERIC,
                gratuity NUMERIC,
                total NUMERIC,
                revenue_center TEXT,
                metadata JSONB NOT NULL,
                data JSONB,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS toast_check_items (
                payment_id TEXT NOT NULL,
                item_index INTEGER NOT NULL,
                item_name TEXT,
                quantity NUMERIC,
                unit_price NUMERIC,
                line_total NUMERIC,
                line_total_with_tax NUMERIC,
                row_data JSONB NOT NULL,
                PRIMARY KEY (payment_id, item_index)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS toast_check_payments (
                payment_id TEXT NOT NULL,
                payment_index INTEGER NOT NULL,
                payment_type TEXT,
                amount NUMERIC,
                tip NUMERIC,
                total NUMERIC,
                card_type TEXT,
                card_last_4 TEXT,
                row_data JSONB NOT NULL,
                PRIMARY KEY (payment_id, payment_index)
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS toast_menu_item_summary (
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                row_index INTEGER NOT NULL,
                row_data JSONB NOT NULL,
                extracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (start_date, end_date, row_index)
            )
            """
        )
    conn.commit()


def export_to_sql(payload: dict[str, Any], database_url: str) -> None:
    try:
        import psycopg
    except ImportError as exc:
        raise ConfigError("psycopg is required for SQL output. Install dependencies first.") from exc

    with psycopg.connect(database_url) as conn:
        ensure_sql_schema(conn)
        with conn.cursor() as cur:
            for record in payload["checks"]:
                payment_id = str(record.get("payment_id") or "").strip()
                if not payment_id:
                    continue
                data = record.get("data") or {}
                metadata = record.get("metadata") or {}
                cur.execute(
                    """
                    INSERT INTO toast_checks (
                        payment_id, complete, attempts, last_error, extracted_at, parsed_url,
                        check_number, time_opened, guest_count, server_name, table_name,
                        discount, subtotal, tax, tip, gratuity, total, revenue_center,
                        metadata, data, updated_at
                    )
                    VALUES (
                        %(payment_id)s, %(complete)s, %(attempts)s, %(last_error)s, %(extracted_at)s,
                        %(parsed_url)s, %(check_number)s, %(time_opened)s, %(guest_count)s,
                        %(server_name)s, %(table_name)s, %(discount)s, %(subtotal)s, %(tax)s,
                        %(tip)s, %(gratuity)s, %(total)s, %(revenue_center)s,
                        %(metadata)s::jsonb, %(data)s::jsonb, NOW()
                    )
                    ON CONFLICT (payment_id) DO UPDATE SET
                        complete = EXCLUDED.complete,
                        attempts = EXCLUDED.attempts,
                        last_error = EXCLUDED.last_error,
                        extracted_at = EXCLUDED.extracted_at,
                        parsed_url = EXCLUDED.parsed_url,
                        check_number = EXCLUDED.check_number,
                        time_opened = EXCLUDED.time_opened,
                        guest_count = EXCLUDED.guest_count,
                        server_name = EXCLUDED.server_name,
                        table_name = EXCLUDED.table_name,
                        discount = EXCLUDED.discount,
                        subtotal = EXCLUDED.subtotal,
                        tax = EXCLUDED.tax,
                        tip = EXCLUDED.tip,
                        gratuity = EXCLUDED.gratuity,
                        total = EXCLUDED.total,
                        revenue_center = EXCLUDED.revenue_center,
                        metadata = EXCLUDED.metadata,
                        data = EXCLUDED.data,
                        updated_at = NOW()
                    """,
                    {
                        "payment_id": payment_id,
                        "complete": bool(record.get("complete")),
                        "attempts": int(record.get("attempts") or 0),
                        "last_error": record.get("last_error"),
                        "extracted_at": record.get("extracted_at"),
                        "parsed_url": record.get("parsed_url"),
                        "check_number": data.get("check_number"),
                        "time_opened": data.get("time_opened"),
                        "guest_count": data.get("guest_count"),
                        "server_name": data.get("server"),
                        "table_name": data.get("table"),
                        "discount": data.get("discount"),
                        "subtotal": data.get("subtotal"),
                        "tax": data.get("tax"),
                        "tip": data.get("tip"),
                        "gratuity": data.get("gratuity"),
                        "total": data.get("total"),
                        "revenue_center": data.get("revenue_center"),
                        "metadata": json.dumps(metadata),
                        "data": json.dumps(data),
                    },
                )

                cur.execute("DELETE FROM toast_check_items WHERE payment_id = %s", (payment_id,))
                items = data.get("items") or []
                for item_index, item in enumerate(items):
                    cur.execute(
                        """
                        INSERT INTO toast_check_items (
                            payment_id, item_index, item_name, quantity, unit_price,
                            line_total, line_total_with_tax, row_data
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        """,
                        (
                            payment_id,
                            item_index,
                            item.get("item_name"),
                            item.get("quantity"),
                            item.get("unit_price"),
                            item.get("line_total"),
                            item.get("line_total_with_tax"),
                            json.dumps(item),
                        ),
                    )

                cur.execute("DELETE FROM toast_check_payments WHERE payment_id = %s", (payment_id,))
                payments = data.get("payments") or []
                for payment_index, payment in enumerate(payments):
                    cur.execute(
                        """
                        INSERT INTO toast_check_payments (
                            payment_id, payment_index, payment_type, amount, tip,
                            total, card_type, card_last_4, row_data
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        """,
                        (
                            payment_id,
                            payment_index,
                            payment.get("payment_type"),
                            payment.get("amount"),
                            payment.get("tip"),
                            payment.get("total"),
                            payment.get("card_type"),
                            payment.get("card_last_4"),
                            json.dumps(payment),
                        ),
                    )

            cur.execute(
                "DELETE FROM toast_menu_item_summary WHERE start_date = %s AND end_date = %s",
                (payload["date_range"]["start_date"], payload["date_range"]["end_date"]),
            )
            for row_index, row in enumerate(payload.get("menu_item_summary") or []):
                cur.execute(
                    """
                    INSERT INTO toast_menu_item_summary (start_date, end_date, row_index, row_data)
                    VALUES (%s, %s, %s, %s::jsonb)
                    ON CONFLICT (start_date, end_date, row_index) DO UPDATE SET
                        row_data = EXCLUDED.row_data,
                        extracted_at = NOW()
                    """,
                    (
                        payload["date_range"]["start_date"],
                        payload["date_range"]["end_date"],
                        row_index,
                        json.dumps(row),
                    ),
                )

        conn.commit()


def add_run_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--start-date", help="Concrete start date in YYYY-MM-DD")
    parser.add_argument("--end-date", help="Concrete end date in YYYY-MM-DD")
    parser.add_argument("--date-query", help="Natural-language range (for example: 'last week')")
    parser.add_argument("--format", choices=["json", "sql"], help="Output format")
    parser.add_argument("--output-path", help="Output file path for JSON mode")
    parser.add_argument("--database-url", help="Database URL for SQL mode")
    parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    parser.add_argument("--menu-summary-file", default=DEFAULT_MENU_SUMMARY_FILE)
    parser.add_argument("--progress-file", default="output/toast_progress.json")
    parser.add_argument("--error-log-file", default="output/toast_errors.jsonl")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--user-data-dir", default=".toast_browser_profile")
    # Default to system Chrome for stability on macOS (avoids "Chrome for Testing" crashes).
    parser.add_argument("--browser-channel", default="chrome")
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--max-pages", type=int, default=0)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--auth-block-restarts", type=int, default=2)
    parser.add_argument("--auth-block-cooldown-sec", type=int, default=90)
    parser.add_argument("--challenge-timeout-sec", type=int, default=120)
    parser.add_argument("--headless", action="store_true")
    parser.add_argument("--skip-metadata", action="store_true")
    parser.add_argument("--refresh-metadata", action="store_true")
    parser.add_argument("--metadata-only", action="store_true")
    parser.add_argument("--human-min-delay-ms", type=int, default=250)
    parser.add_argument("--human-max-delay-ms", type=int, default=900)
    parser.add_argument("--detail-start-min-interval-ms", type=int, default=700)
    parser.add_argument("--no-prompt", action="store_true")


def build_extract_cmd(args: argparse.Namespace, config: RunConfig) -> list[str]:
    cmd = [
        sys.executable,
        str(EXTRACT_SCRIPT),
        "--start-date",
        config.start_date,
        "--end-date",
        config.end_date,
        "--state-file",
        args.state_file,
        "--menu-summary-file",
        args.menu_summary_file,
        "--progress-file",
        args.progress_file,
        "--error-log-file",
        args.error_log_file,
        "--env-file",
        args.env_file,
        "--user-data-dir",
        args.user_data_dir,
        "--workers",
        str(args.workers),
        "--max-pages",
        str(args.max_pages),
        "--limit",
        str(args.limit),
        "--auth-block-restarts",
        str(args.auth_block_restarts),
        "--auth-block-cooldown-sec",
        str(args.auth_block_cooldown_sec),
        "--challenge-timeout-sec",
        str(args.challenge_timeout_sec),
        "--human-min-delay-ms",
        str(args.human_min_delay_ms),
        "--human-max-delay-ms",
        str(args.human_max_delay_ms),
        "--detail-start-min-interval-ms",
        str(args.detail_start_min_interval_ms),
    ]

    if args.browser_channel:
        cmd.extend(["--browser-channel", args.browser_channel])
    if args.headless:
        cmd.append("--headless")
    if args.skip_metadata:
        cmd.append("--skip-metadata")
    if args.refresh_metadata:
        cmd.append("--refresh-metadata")
    if args.metadata_only:
        cmd.append("--metadata-only")
    return cmd


def run_foreground(args: argparse.Namespace) -> int:
    config = resolve_run_config(args, allow_prompt=not args.no_prompt)

    extract_cmd = build_extract_cmd(args, config)
    print(json.dumps({"event": "extract_start", "cmd": extract_cmd, "ts": utc_now()}), flush=True)
    subprocess.run(extract_cmd, check=True)

    records = load_records(Path(args.state_file))
    menu_summary = load_menu_summary(Path(args.menu_summary_file))
    payload = build_export_payload(records=records, menu_summary=menu_summary, config=config)

    if config.output_format == "json":
        if not config.output_path:
            raise ConfigError("JSON output path is missing.")
        output_path = Path(config.output_path)
        export_to_json(payload, output_path)
        print(
            json.dumps(
                {
                    "event": "export_complete",
                    "format": "json",
                    "output_path": str(output_path),
                    "ts": utc_now(),
                    "stats": payload["stats"],
                }
            ),
            flush=True,
        )
        return 0

    if not config.database_url:
        raise ConfigError("Database URL is required for SQL export.")
    export_to_sql(payload, config.database_url)
    print(
        json.dumps(
            {
                "event": "export_complete",
                "format": "sql",
                "database_url": config.database_url,
                "ts": utc_now(),
                "stats": payload["stats"],
            }
        ),
        flush=True,
    )
    return 0


def run_status(args: argparse.Namespace) -> int:
    state_file = Path(args.state_file)
    menu_summary_file = Path(args.menu_summary_file)
    progress_file = Path(args.progress_file)
    error_log_file = Path(args.error_log_file)

    total = complete = incomplete = errored = 0
    if state_file.exists():
        records = load_records(state_file)
        total = len(records)
        complete = sum(1 for row in records if row.get("complete"))
        errored = sum(1 for row in records if row.get("last_error"))
        incomplete = total - complete

    menu_rows = 0
    if menu_summary_file.exists():
        menu_rows = len(load_menu_summary(menu_summary_file))

    progress_payload: dict[str, Any] | None = None
    if progress_file.exists():
        try:
            progress_payload = json.loads(progress_file.read_text(encoding="utf-8"))
        except Exception:
            progress_payload = None

    error_count = 0
    if error_log_file.exists():
        with error_log_file.open("r", encoding="utf-8", errors="ignore") as handle:
            for _ in handle:
                error_count += 1

    tmux_running = None
    if args.session_name:
        tmux_check = subprocess.run(
            ["tmux", "has-session", "-t", args.session_name],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        tmux_running = tmux_check.returncode == 0

    result: dict[str, Any] = {
        "state_file": str(state_file),
        "menu_summary_file": str(menu_summary_file),
        "progress_file": str(progress_file),
        "error_log_file": str(error_log_file),
        "total": total,
        "complete": complete,
        "incomplete": incomplete,
        "errored": errored,
        "menu_summary_rows": menu_rows,
        "error_log_lines": error_count,
    }
    if progress_payload is not None:
        result["progress"] = progress_payload
    if tmux_running is not None:
        result["session_name"] = args.session_name
        result["tmux_running"] = tmux_running

    print(json.dumps(result, indent=2), flush=True)
    return 0


def namespace_to_argv(args: argparse.Namespace, config: RunConfig) -> list[str]:
    argv = [
        sys.executable,
        str(SCRIPT_PATH),
        "run",
        "--start-date",
        config.start_date,
        "--end-date",
        config.end_date,
        "--format",
        config.output_format,
        "--state-file",
        args.state_file,
        "--menu-summary-file",
        args.menu_summary_file,
        "--progress-file",
        args.progress_file,
        "--error-log-file",
        args.error_log_file,
        "--env-file",
        args.env_file,
        "--user-data-dir",
        args.user_data_dir,
        "--workers",
        str(args.workers),
        "--max-pages",
        str(args.max_pages),
        "--limit",
        str(args.limit),
        "--auth-block-restarts",
        str(args.auth_block_restarts),
        "--auth-block-cooldown-sec",
        str(args.auth_block_cooldown_sec),
        "--challenge-timeout-sec",
        str(args.challenge_timeout_sec),
        "--human-min-delay-ms",
        str(args.human_min_delay_ms),
        "--human-max-delay-ms",
        str(args.human_max_delay_ms),
        "--detail-start-min-interval-ms",
        str(args.detail_start_min_interval_ms),
        "--no-prompt",
    ]
    if config.output_format == "json" and config.output_path:
        argv.extend(["--output-path", config.output_path])
    if config.output_format == "sql" and config.database_url:
        argv.extend(["--database-url", config.database_url])
    if args.browser_channel:
        argv.extend(["--browser-channel", args.browser_channel])
    if args.headless:
        argv.append("--headless")
    if args.skip_metadata:
        argv.append("--skip-metadata")
    if args.refresh_metadata:
        argv.append("--refresh-metadata")
    if args.metadata_only:
        argv.append("--metadata-only")
    return argv


def run_background(args: argparse.Namespace) -> int:
    config = resolve_run_config(args, allow_prompt=not args.no_prompt)

    run_dir = Path(args.run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)

    session_name = args.session_name.strip()
    if not session_name:
        session_name = f"toast-extract-{datetime.now().strftime('%Y%m%d-%H%M%S')}"

    log_file = run_dir / f"{session_name}.log"
    manifest_file = run_dir / f"{session_name}.json"

    argv = namespace_to_argv(args, config)
    command = " ".join(shlex.quote(token) for token in argv)
    ensure_parent_dir(log_file)

    subprocess.run(
        [
            "tmux",
            "new-session",
            "-d",
            "-s",
            session_name,
            f"{command} 2>&1 | tee -a {shlex.quote(str(log_file))}",
        ],
        check=True,
    )

    manifest = {
        "created_at": utc_now(),
        "session_name": session_name,
        "log_file": str(log_file),
        "state_file": args.state_file,
        "menu_summary_file": args.menu_summary_file,
        "progress_file": args.progress_file,
        "error_log_file": args.error_log_file,
        "run_cmd": argv,
        "date_range": {
            "start_date": config.start_date,
            "end_date": config.end_date,
            "date_query": config.date_query,
        },
        "format": config.output_format,
        "output_path": config.output_path,
        "database_url": config.database_url,
    }
    manifest_file.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(
        json.dumps(
            {
                "event": "background_started",
                "session_name": session_name,
                "manifest_file": str(manifest_file),
                "log_file": str(log_file),
            },
            indent=2,
        ),
        flush=True,
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Toast skill runner")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="Run extraction and export in foreground")
    add_run_arguments(run_parser)

    start_parser = subparsers.add_parser("start-bg", help="Run extraction in a tmux background session")
    add_run_arguments(start_parser)
    start_parser.add_argument("--session-name", default="")
    start_parser.add_argument("--run-dir", default=DEFAULT_RUN_DIR)

    status_parser = subparsers.add_parser("status", help="Print extraction progress from state files")
    status_parser.add_argument("--state-file", default=DEFAULT_STATE_FILE)
    status_parser.add_argument("--menu-summary-file", default=DEFAULT_MENU_SUMMARY_FILE)
    status_parser.add_argument("--progress-file", default="output/toast_progress.json")
    status_parser.add_argument("--error-log-file", default="output/toast_errors.jsonl")
    status_parser.add_argument("--session-name", default="")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        if args.command == "run":
            return run_foreground(args)
        if args.command == "start-bg":
            return run_background(args)
        if args.command == "status":
            return run_status(args)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except subprocess.CalledProcessError as exc:
        print(f"Command failed with exit code {exc.returncode}", file=sys.stderr)
        return exc.returncode or 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
