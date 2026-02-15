#!/usr/bin/env python3
"""Extract Toast checks from the Payments report and order detail pages.

This script is intentionally config-driven and resumable:
- Persistent browser profile keeps authentication across runs.
- Payments metadata is saved first with `complete=false`.
- Order details are fetched in parallel and merged back into the same state file.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import re
import shutil
import urllib.parse
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any

from playwright.async_api import BrowserContext, Error as PlaywrightError, Page, async_playwright

ORDER_DETAILS_URL = "https://www.toasttab.com/restaurants/admin/reports/home#sales-order-details"
HEADLESS_CHROME_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/144.0.7559.133 Safari/537.36"
)
DEFAULT_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "references" / "toast_selectors.json"
)


DEFAULT_SELECTORS: dict[str, Any] = {
    "payments": {
        "table_rows": "#sales-payments table tbody tr",
        "table_headers": "#sales-payments table thead th",
        "next_button": [
            "#sales-payments .dataTables_paginate li.next:not(.disabled) a",
            "#sales-payments .dataTables_paginate li.next a",
            "#sales-payments li.next:not(.disabled) a",
            "#sales-payments li.next a",
            "#sales-payments a:has-text('Next →')",
        ],
        "per_page_select": [
            "#sales-payments select[name='payments-report_length']",
            "#sales-payments select[name$='_length']",
            "#sales-payments select[aria-label*='per page' i]",
            "#sales-payments select[name*='pageSize' i]",
            "#sales-payments select[name*='perPage' i]",
        ],
        "per_page_100_option": [
            "#sales-payments .per-page-selector .dropdown-menu a[data-value='100']",
            "#sales-payments .per-page-selector .dropdown-menu a:has-text('100')",
            "#sales-payments a:has-text('100')",
        ],
        "date_start_input": [
            "#sales-payments input[name='reportDateStart']",
            "#sales-payments #startDate",
            "#sales-payments input[name*='start' i]",
            "#sales-payments input[aria-label*='start' i]",
            "#sales-payments input[placeholder*='Start' i]",
            "input[name='reportDateStart']",
            "#startDate",
            "input[name*='start' i]",
            "input[aria-label*='start' i]",
            "input[placeholder*='Start' i]",
        ],
        "date_end_input": [
            "#sales-payments input[name='reportDateEnd']",
            "#sales-payments #endDate",
            "#sales-payments input[name*='end' i]",
            "#sales-payments input[aria-label*='end' i]",
            "#sales-payments input[placeholder*='End' i]",
            "input[name='reportDateEnd']",
            "#endDate",
            "input[name*='end' i]",
            "input[aria-label*='end' i]",
            "input[placeholder*='End' i]",
        ],
        "apply_button": [
            "#sales-payments #update-btn",
            "#sales-payments #filter-apply-handler",
            "#sales-payments button:has-text('Apply')",
            "#sales-payments button:has-text('Update')",
            "#update-btn",
            "#filter-apply-handler",
            "button:has-text('Apply')",
            "button:has-text('Update')",
        ],
    },
    "order_details": {
        "tab_link": [
            "a[href='#sales-order-details']",
            "li a[data-report='ORDER_SUMMARY_DETAILS']",
            "a:has-text('Order Details')",
        ],
        "top_items_table": [
            "#top-items",
            "#sales-order-details #top-items",
        ],
        "top_items_per_page_select": [
            "select[name='top-items_length']",
            "#top-items_wrapper select[name$='_length']",
        ],
        "top_items_per_page_100_option": [
            "#top-items_wrapper .per-page-selector .dropdown-menu a[data-value='100']",
            "#top-items_wrapper .per-page-selector .dropdown-menu a:has-text('100')",
            "#top-items_wrapper a:has-text('100')",
        ],
        "top_items_next_button": [
            "#top-items_wrapper .dataTables_paginate li.next:not(.disabled) a",
            "#top-items_wrapper .dataTables_paginate li.next a",
            "#top-items_wrapper li.next:not(.disabled) a",
            "#top-items_wrapper li.next a",
            "#top-items_wrapper a:has-text('Next →')",
        ],
        "show_hide_columns_button": [
            "#top-items_wrapper .ColVis_MasterButton",
            "#sales-order-details .ColVis_MasterButton",
            "button:has-text('Show / hide columns')",
        ],
        "order_blocks": [
            "#sales-order-details .order-border",
            ".order-border",
        ],
        "order_next_button": [
            "#sales-order-details .pagination li.next:not(.disabled) a",
            "#sales-order-details .pagination li.next a",
            ".pagination li.next:not(.disabled) a",
            ".pagination li.next a",
            "a:has-text('Next ›')",
            "a:has-text('Next')",
        ],
    },
    "auth": {
        "logged_out_markers": [
            "input[type='password']",
            "button:has-text('Log in')",
            "button:has-text('Sign in')",
        ],
        "username_inputs": [
            "input[type='email']",
            "input[name*='email' i]",
            "input[id*='email' i]",
            "input[autocomplete='username']",
            "input[type='text']",
        ],
        "password_inputs": [
            "input[type='password']",
            "input[name*='pass' i]",
            "input[id*='pass' i]",
        ],
        "submit_buttons": [
            "button[type='submit']",
            "button:has-text('Next')",
            "button:has-text('Continue')",
            "button:has-text('Log in')",
            "button:has-text('Sign in')",
        ],
        "not_now_buttons": [
            "button[name='action'][value='snooze-enrollment']",
            "button[name='action'][value='refuse-add-device']",
            "button:has-text('Remind me later')",
            "button:has-text('Not on this device')",
            "button:has-text('Not now')",
            "button:has-text('No thanks')",
            "button:has-text('Skip')",
            "button:has-text('Not Now')",
        ],
        "authenticated_markers": [
            "[data-testid*='report' i]",
            "a[href*='reports']",
            "button:has-text('Reports')",
        ],
    },
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def log_event(event: str, **payload: Any) -> None:
    record = {"ts": utc_now(), "event": event, **payload}
    print(json.dumps(record, ensure_ascii=True), flush=True)


def jitter_ms(min_ms: int, max_ms: int) -> int:
    low = max(0, int(min_ms))
    high = max(low, int(max_ms))
    return random.randint(low, high) if high else low


async def human_pause(
    _page: Page | None,
    *,
    min_ms: int,
    max_ms: int,
    label: str | None = None,
) -> None:
    # Small jitter reduces "robotic" bursts and helps with throttling.
    delay = jitter_ms(min_ms, max_ms)
    if delay <= 0:
        return
    if label:
        log_event("human_pause", label=label, ms=delay)
    await asyncio.sleep(delay / 1000.0)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract Toast check data with resume support.")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD) for payments report")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD) for payments report")
    parser.add_argument(
        "--state-file",
        default="output/toast_checks_state.json",
        help="JSON state file used for metadata + detail resume",
    )
    parser.add_argument(
        "--user-data-dir",
        default=".toast_browser_profile",
        help="Persistent browser profile directory for auth",
    )
    parser.add_argument(
        "--browser-channel",
        default="chrome",
        help="Chromium channel to use (for example: chrome, msedge).",
    )
    parser.add_argument(
        "--env-file",
        default=".env",
        help="Optional env file for login credentials when session is not authenticated.",
    )
    parser.add_argument(
        "--user-var",
        default="TOAST_USERNAME",
        help="Env variable name for Toast username/email.",
    )
    parser.add_argument(
        "--pass-var",
        default="TOAST_PASSWORD",
        help="Env variable name for Toast password.",
    )
    parser.add_argument(
        "--auth-timeout-sec",
        type=int,
        default=45,
        help="Per-attempt auth timeout while waiting for dashboard redirect.",
    )
    parser.add_argument(
        "--auth-max-attempts",
        type=int,
        default=3,
        help="Maximum credential login attempts before failing.",
    )
    parser.add_argument(
        "--challenge-timeout-sec",
        type=int,
        default=120,
        help="Max time to wait for Cloudflare/human-verification gate to clear.",
    )
    parser.add_argument(
        "--auth-block-restarts",
        type=int,
        default=2,
        help="When AUTH_BLOCKED occurs, logout and restart from scratch this many times.",
    )
    parser.add_argument(
        "--auth-block-cooldown-sec",
        type=int,
        default=90,
        help="Cooldown before retrying after AUTH_BLOCKED (helps with throttling/Cloudflare).",
    )
    parser.add_argument(
        "--reset-profile-on-auth-block",
        dest="reset_profile_on_auth_block",
        action="store_true",
        help="Reset --user-data-dir before retrying an AUTH_BLOCKED run.",
    )
    parser.add_argument(
        "--no-reset-profile-on-auth-block",
        dest="reset_profile_on_auth_block",
        action="store_false",
        help="Keep existing browser profile on AUTH_BLOCKED retries.",
    )
    # Default to preserving the profile. Nuking it tends to trigger more Cloudflare challenges.
    parser.set_defaults(reset_profile_on_auth_block=False)
    parser.add_argument(
        "--allow-manual-login",
        action="store_true",
        help="Allow manual login fallback instead of failing fast in non-interactive runs.",
    )
    parser.add_argument(
        "--artifact-dir",
        default="output/toast_artifacts",
        help="Directory for auth/debug screenshots and HTML snapshots.",
    )
    parser.add_argument(
        "--config",
        default=str(DEFAULT_CONFIG_PATH),
        help="Optional selector config JSON (overrides defaults if present)",
    )
    parser.add_argument("--workers", type=int, default=6, help="Parallel order detail workers")
    parser.add_argument("--max-pages", type=int, default=0, help="Max payments pages (0 = all)")
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Only process first N pending payment IDs (0 = no limit)",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run browser in headless mode (default headful for manual login)",
    )
    parser.add_argument(
        "--headless-user-agent",
        default=HEADLESS_CHROME_USER_AGENT,
        help="User-Agent override used when running --headless.",
    )
    parser.add_argument(
        "--skip-metadata",
        action="store_true",
        help="Skip payments table crawl and only process pending IDs from existing state",
    )
    parser.add_argument(
        "--refresh-metadata",
        action="store_true",
        help="Re-crawl metadata and merge with existing state before details extraction",
    )
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Only crawl metadata into the state file and exit (resume details later with --skip-metadata).",
    )
    parser.add_argument(
        "--menu-summary-file",
        default="output/toast_menu_item_summary.json",
        help="JSON file used to persist Menu Item Summary rows from the Order Details report.",
    )
    parser.add_argument(
        "--progress-file",
        default="output/toast_progress.json",
        help="JSON file containing latest aggregate progress for background monitoring.",
    )
    parser.add_argument(
        "--error-log-file",
        default="output/toast_errors.jsonl",
        help="JSONL file where row-level extraction errors are appended for review.",
    )
    parser.add_argument(
        "--human-min-delay-ms",
        type=int,
        default=250,
        help="Minimum jitter delay between actions (milliseconds).",
    )
    parser.add_argument(
        "--human-max-delay-ms",
        type=int,
        default=900,
        help="Maximum jitter delay between actions (milliseconds).",
    )
    parser.add_argument(
        "--detail-start-min-interval-ms",
        type=int,
        default=700,
        help="Minimum spacing between starting each order-detail navigation across workers (milliseconds).",
    )
    return parser.parse_args()


def deep_merge(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = value
    return out


def load_config(path: str) -> dict[str, Any]:
    config = DEFAULT_SELECTORS
    config_path = Path(path)
    if config_path.exists():
        extra = json.loads(config_path.read_text(encoding="utf-8"))
        config = deep_merge(DEFAULT_SELECTORS, extra)
    return config


def load_env_values(path: str) -> dict[str, str]:
    env_path = Path(path)
    if not env_path.exists():
        return {}
    values: dict[str, str] = {}
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


def resolve_credentials(
    env_values: dict[str, str], user_var: str, pass_var: str
) -> tuple[str, str] | None:
    user_keys = [user_var, "TOAST_USERNAME", "TOAST_USER", "USER", "EMAIL"]
    pass_keys = [pass_var, "TOAST_PASSWORD", "TOAST_PASS", "PASS", "PASSWORD"]

    username = next((env_values.get(key, "").strip() for key in user_keys if env_values.get(key, "").strip()), "")
    password = next((env_values.get(key, "").strip() for key in pass_keys if env_values.get(key, "").strip()), "")
    if username and password:
        return (username, password)
    return None


def load_state(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    records = json.loads(path.read_text(encoding="utf-8"))
    normalized: dict[str, dict[str, Any]] = {}
    for record in records:
        payment_id = record.get("payment_id")
        if not payment_id:
            continue
        metadata = record.get("metadata")
        if isinstance(metadata, dict):
            record["metadata"] = normalize_metadata_fields(metadata)
        if not record.get("parsed_url"):
            record["parsed_url"] = ORDER_DETAILS_URL
        data = record.get("data")
        if isinstance(data, dict) and "parsed_url" in data:
            data.pop("parsed_url", None)
        normalized[payment_id] = record
    return normalized


def save_state(path: Path, state: dict[str, dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    payload = sorted(state.values(), key=lambda row: row["payment_id"])
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)


def save_menu_summary(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(rows, indent=2), encoding="utf-8")
    tmp.replace(path)


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=True) + "\n")


def save_progress(path: Path, state: dict[str, dict[str, Any]], run_id: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    total = len(state)
    complete = sum(1 for row in state.values() if row.get("complete"))
    errored = sum(1 for row in state.values() if row.get("last_error"))
    payload = {
        "run_id": run_id,
        "updated_at": utc_now(),
        "total": total,
        "complete": complete,
        "incomplete": total - complete,
        "errored": errored,
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tmp.replace(path)


def normalize_metadata_fields(metadata: dict[str, Any]) -> dict[str, Any]:
    # Backward compatibility: older state files stored table headers under metadata.columns.
    if isinstance(metadata.get("columns"), dict):
        flattened = dict(metadata["columns"])
        if metadata.get("payment_id"):
            flattened["payment_id"] = metadata["payment_id"]
        metadata = flattened

    cleaned: dict[str, Any] = {}
    for key, value in metadata.items():
        key_text = str(key).strip()
        if not key_text:
            continue
        if key_text.lower() in {"receipt", "detail_url", "columns", "raw_cells"}:
            continue
        cleaned[key_text] = value
    return cleaned


def to_us_date(date_str: str) -> str:
    parsed = datetime.strptime(date_str, "%Y-%m-%d")
    # Toast report date inputs use MM-DD-YYYY (for example: 02-06-2026).
    return parsed.strftime("%m-%d-%Y")


def to_short_us_date(date_str: str) -> str:
    parsed = datetime.strptime(date_str, "%Y-%m-%d")
    # Legacy hidden Toast fields use M/D/YY.
    return f"{parsed.month}/{parsed.day}/{parsed.strftime('%y')}"


def clean_text(value: Any) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = unescape(text)
    return re.sub(r"\s+", " ", text).strip()


async def first_usable_locator(
    page: Page, selectors: list[str], require_visible: bool = False
) -> str | None:
    for selector in selectors:
        candidates = [f"{selector}:visible", selector] if require_visible else [selector]
        for candidate in candidates:
            locator = page.locator(candidate).first
            try:
                if await locator.count() > 0:
                    return candidate
            except Exception:
                continue
    return None


async def click_first_available(
    page: Page, selectors: list[str], require_visible: bool = True
) -> bool:
    selector = await first_usable_locator(page, selectors, require_visible=require_visible)
    if not selector:
        return False
    try:
        await page.locator(selector).first.click()
        return True
    except Exception:
        return False


async def wait_for_payments_table_ready(page: Page, timeout_sec: int = 20) -> None:
    deadline = asyncio.get_event_loop().time() + max(1, timeout_sec)
    while asyncio.get_event_loop().time() < deadline:
        try:
            if await page.locator("#sales-payments #payments-report_info").count() > 0:
                return
            if await page.locator("#sales-payments .per-page-selector").count() > 0:
                return
            if await page.locator("#sales-payments table tbody tr").count() > 0:
                return
        except Exception:
            pass
        await asyncio.sleep(0.4)


async def ensure_payments_tab(page: Page) -> None:
    active_selector = "#sales-payments.tab-pane.active, #sales-payments.active"
    if await page.locator(active_selector).count() > 0:
        return

    tab_selectors = [
        "a[href='#sales-payments']",
        "li:has-text('Payments') a",
    ]
    for selector in tab_selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count() > 0 and await locator.is_visible():
                await locator.click()
                break
        except Exception:
            continue

    try:
        await page.wait_for_selector(active_selector, timeout=8000)
    except Exception:
        pass


async def wait_for_order_details_table_ready(page: Page, timeout_sec: int = 20) -> None:
    selectors = [
        "#sales-order-details #top-items",
        "#top-items_wrapper",
        "#sales-order-details .pagination",
    ]
    deadline = asyncio.get_event_loop().time() + max(1, timeout_sec)
    while asyncio.get_event_loop().time() < deadline:
        for selector in selectors:
            try:
                if await page.locator(selector).count() > 0:
                    return
            except Exception:
                continue
        await asyncio.sleep(0.4)


async def ensure_order_details_tab(page: Page, config: dict[str, Any]) -> None:
    active_selector = "#sales-order-details.tab-pane.active, #sales-order-details.active"
    if await page.locator(active_selector).count() > 0:
        return

    for selector in config.get("order_details", {}).get("tab_link", []):
        try:
            locator = page.locator(selector).first
            if await locator.count() > 0 and await locator.is_visible():
                await locator.click()
                break
        except Exception:
            continue

    try:
        await page.wait_for_selector(active_selector, timeout=8000)
    except Exception:
        pass


async def set_top_items_per_page(
    page: Page,
    config: dict[str, Any],
    per_page: int = 100,
    *,
    human_min_delay_ms: int = 250,
    human_max_delay_ms: int = 900,
) -> None:
    await wait_for_order_details_table_ready(page, timeout_sec=20)
    selectors = config.get("order_details", {}).get("top_items_per_page_select", [])
    for _ in range(4):
        selector = await first_usable_locator(page, selectors, require_visible=False)
        if selector:
            try:
                js_updated = await page.evaluate(
                    """({ selectors, value }) => {
                        for (const selector of selectors) {
                            const el = document.querySelector(selector);
                            if (!el || el.tagName.toLowerCase() !== "select") continue;
                            el.value = value;
                            el.dispatchEvent(new Event("input", { bubbles: true }));
                            el.dispatchEvent(new Event("change", { bubbles: true }));
                            return true;
                        }
                        return false;
                    }""",
                    {"selectors": selectors, "value": str(per_page)},
                )
                if not js_updated:
                    await page.locator(selector).first.select_option(str(per_page), force=True, timeout=2000)
                await page.wait_for_timeout(700)
                await human_pause(
                    page,
                    min_ms=human_min_delay_ms,
                    max_ms=human_max_delay_ms,
                    label="set_top_items_per_page",
                )
                return
            except Exception:
                pass
        await page.wait_for_timeout(500)

    if per_page == 100:
        option_selector = await first_usable_locator(
            page,
            config.get("order_details", {}).get("top_items_per_page_100_option", []),
            require_visible=True,
        )
        if option_selector:
            try:
                await page.locator(option_selector).first.click(timeout=3000)
                await page.wait_for_timeout(700)
                return
            except Exception:
                pass


async def expand_menu_item_summary_columns(page: Page, config: dict[str, Any]) -> None:
    # Column visibility is optional; if unavailable we keep default columns and continue.
    button_selector = await first_usable_locator(
        page,
        config.get("order_details", {}).get("show_hide_columns_button", []),
        require_visible=True,
    )
    if not button_selector:
        return
    try:
        await page.locator(button_selector).first.click(timeout=2000)
        await page.wait_for_timeout(250)
        await page.evaluate(
            """() => {
                const collection =
                    document.querySelector('.ColVis_collection') ||
                    document.querySelector('.ColVis_collectionBackground')?.nextElementSibling;
                if (!collection) return false;

                const clickNode = (node) => {
                    if (!node) return;
                    if (node.tagName && node.tagName.toLowerCase() === 'input') {
                        if (!node.checked) node.click();
                        return;
                    }
                    const checkbox = node.querySelector('input[type="checkbox"]');
                    if (checkbox && !checkbox.checked) {
                        checkbox.click();
                        return;
                    }
                    const marker = node.className || '';
                    if (String(marker).includes('ColVis')) {
                        node.click();
                    }
                };

                for (const item of Array.from(collection.querySelectorAll('li, button, a, span'))) {
                    clickNode(item);
                }
                return true;
            }"""
        )
    except Exception:
        return
    finally:
        try:
            await page.keyboard.press("Escape")
        except Exception:
            pass


async def extract_menu_item_summary_rows(page: Page, config: dict[str, Any]) -> list[dict[str, Any]]:
    table_selector = await first_usable_locator(
        page,
        config.get("order_details", {}).get("top_items_table", []),
        require_visible=False,
    )
    if not table_selector:
        return []

    payload = await page.evaluate(
        """(selector) => {
            const table = document.querySelector(selector);
            if (!table) return { headers: [], rows: [] };
            const headers = Array.from(table.querySelectorAll("thead th"))
                .map((el) => (el.textContent || "").trim());
            const rows = Array.from(table.querySelectorAll("tbody tr")).map((row) =>
                Array.from(row.querySelectorAll("th,td")).map((cell) => (cell.textContent || "").trim())
            );
            return { headers, rows };
        }""",
        table_selector,
    )
    headers = [clean_text(header) for header in payload.get("headers", []) if clean_text(header)]
    mapped_rows: list[dict[str, Any]] = []
    for row in payload.get("rows", []):
        cells = [clean_text(cell) for cell in row]
        mapped: dict[str, Any] = {}
        for index, cell in enumerate(cells):
            if not cell:
                continue
            key = headers[index] if index < len(headers) and headers[index] else f"col_{index}"
            mapped[key] = cell
        if mapped:
            mapped_rows.append(mapped)
    return mapped_rows


async def click_next_menu_item_summary_page(page: Page, config: dict[str, Any]) -> bool:
    selectors = config.get("order_details", {}).get("top_items_next_button", [])
    try:
        clicked = await page.evaluate(
            """(candidateSelectors) => {
                const isVisible = (el) => {
                    if (!el) return false;
                    const r = el.getBoundingClientRect();
                    if (r.width <= 0 || r.height <= 0) return false;
                    const style = window.getComputedStyle(el);
                    if (!style) return true;
                    if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
                    return true;
                };
                for (const selector of candidateSelectors || []) {
                    const nodes = Array.from(document.querySelectorAll(selector));
                    for (const node of nodes) {
                        if (!isVisible(node)) continue;
                        const ariaDisabled = (node.getAttribute('aria-disabled') || '').toLowerCase() === 'true';
                        const disabledAttr = node.getAttribute('disabled') != null;
                        const className = (node.getAttribute('class') || '').toLowerCase();
                        const parentClass = (node.parentElement?.getAttribute('class') || '').toLowerCase();
                        if (ariaDisabled || disabledAttr) continue;
                        if (className.includes('disabled') || parentClass.includes('disabled')) continue;
                        node.click();
                        return true;
                    }
                }
                return false;
            }""",
            selectors,
        )
        if clicked:
            await page.wait_for_timeout(700)
            return True
    except Exception:
        pass
    return False


async def crawl_menu_item_summary(
    page: Page,
    config: dict[str, Any],
    start_date: str,
    end_date: str,
    max_pages: int,
    *,
    human_min_delay_ms: int = 250,
    human_max_delay_ms: int = 900,
) -> list[dict[str, Any]]:
    await page.goto(ORDER_DETAILS_URL, wait_until="domcontentloaded", timeout=45000)
    await ensure_order_details_tab(page, config)
    await set_date_range(
        page,
        config,
        start_date,
        end_date,
        human_min_delay_ms=human_min_delay_ms,
        human_max_delay_ms=human_max_delay_ms,
    )
    await ensure_order_details_tab(page, config)
    await wait_for_order_details_table_ready(page, timeout_sec=20)
    await set_top_items_per_page(
        page,
        config,
        100,
        human_min_delay_ms=human_min_delay_ms,
        human_max_delay_ms=human_max_delay_ms,
    )
    await expand_menu_item_summary_columns(page, config)

    all_rows: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    page_signatures: set[str] = set()
    page_count = 0
    while True:
        page_count += 1
        rows = await extract_menu_item_summary_rows(page, config)
        signature_parts: list[str] = []
        for row in rows:
            key = "|".join(f"{k}:{row[k]}" for k in sorted(row.keys()))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            all_rows.append(row)
            if len(signature_parts) < 4:
                signature_parts.append(key)

        log_event("menu_summary_page_fetched", page=page_count, rows=len(rows), accepted=len(all_rows))

        signature = "|".join(signature_parts) if signature_parts else ""
        if signature:
            if signature in page_signatures:
                log_event(
                    "menu_summary_pagination_stalled",
                    page=page_count,
                    reason="repeated_page_signature",
                )
                break
            page_signatures.add(signature)

        if max_pages and page_count >= max_pages:
            break
        if not await click_next_menu_item_summary_page(page, config):
            break
        await human_pause(
            page,
            min_ms=max(400, human_min_delay_ms),
            max_ms=max(1200, human_max_delay_ms),
            label="menu_summary_page_pause",
        )

    return all_rows


async def detect_no_items_message(page: Page) -> str | None:
    """Return a snippet containing the 'no items/no data' message if present."""
    try:
        snippet = await page.evaluate(
            """() => {
                const text = (document.body?.innerText || '').replace(/\\s+/g, ' ').trim();
                const lower = text.toLowerCase();
                const patterns = [
                    'no items exist for this time period',
                    'no items exist',
                    'no results',
                    'no data',
                ];
                for (const pat of patterns) {
                    const idx = lower.indexOf(pat);
                    if (idx >= 0) {
                        return text.slice(Math.max(0, idx - 80), Math.min(text.length, idx + pat.length + 160));
                    }
                }
                return '';
            }"""
        )
        return snippet if isinstance(snippet, str) and snippet.strip() else None
    except Exception:
        return None


async def wait_for_order_details_idle(page: Page, timeout_sec: int = 35) -> None:
    """Heuristic wait for the report to stop showing spinners/loading overlays."""
    deadline = asyncio.get_event_loop().time() + max(1, timeout_sec)
    while asyncio.get_event_loop().time() < deadline:
        try:
            loading_visible = await page.evaluate(
                """() => {
                    const selectors = [
                        '[aria-busy=\"true\"]',
                        '.loading',
                        '.spinner',
                        '.progress',
                        'img[alt*=\"Loading\" i]',
                    ];
                    const isVisible = (el) => {
                        const r = el.getBoundingClientRect();
                        return r.width > 0 && r.height > 0;
                    };
                    for (const sel of selectors) {
                        const el = document.querySelector(sel);
                        if (el && isVisible(el)) return true;
                    }
                    return false;
                }"""
            )
            if not loading_visible:
                return
        except Exception:
            return
        await asyncio.sleep(0.4)


async def save_order_details_debug_artifacts(page: Page, artifact_dir: Path, label: str) -> None:
    """Write screenshot + HTML + basic DOM summary to help diagnose selector mismatches."""
    artifact_dir.mkdir(parents=True, exist_ok=True)
    safe = re.sub(r"[^a-zA-Z0-9_.-]+", "_", label).strip("_") or "debug"

    try:
        await page.screenshot(path=str(artifact_dir / f"{safe}.png"), full_page=True)
    except Exception:
        pass
    try:
        html = await page.content()
        (artifact_dir / f"{safe}.html").write_text(html, encoding="utf-8")
    except Exception:
        pass
    try:
        summary = await page.evaluate(
            """() => {
                const tables = Array.from(document.querySelectorAll('table')).map((t) => {
                    const id = t.id || '';
                    const cls = (t.className || '').toString();
                    const headers = Array.from(t.querySelectorAll('thead th'))
                        .map((th) => (th.textContent || '').trim())
                        .filter(Boolean);
                    const rows = t.querySelectorAll('tbody tr').length;
                    return { id, cls, headers: headers.slice(0, 12), rows };
                });
                const blocks = document.querySelectorAll('.order-border').length;
                return { url: location.href, title: document.title, blocks, tables };
            }"""
        )
        (artifact_dir / f"{safe}.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        log_event("order_details_debug_saved", label=label, artifact_dir=str(artifact_dir))
    except Exception:
        pass


async def wait_for_order_detail_blocks_ready(page: Page, config: dict[str, Any], timeout_sec: int = 45) -> None:
    selectors = config.get("order_details", {}).get("order_blocks", [])
    deadline = asyncio.get_event_loop().time() + max(1, timeout_sec)
    while asyncio.get_event_loop().time() < deadline:
        no_items = await detect_no_items_message(page)
        if no_items:
            log_event("order_details_no_items", snippet=no_items)
            return

        for selector in selectors:
            try:
                if await page.locator(selector).count() > 0:
                    return
            except Exception:
                continue
        # Some Toast views lazy-render order blocks after scrolling.
        try:
            await page.evaluate(
                "() => window.scrollBy(0, Math.max(400, Math.floor(window.innerHeight * 0.85)))"
            )
        except Exception:
            pass
        await asyncio.sleep(0.5)


async def get_pagination_summary(page: Page) -> dict[str, int]:
    """Read the LAST .pagination-summary span and parse 'Showing x through y of z'.

    Returns ``{"start": x, "end": y, "total": z}`` or an empty dict when the
    element is absent or the text doesn't match the expected pattern.
    """
    try:
        info = await page.evaluate(
            """() => {
                const spans = Array.from(document.querySelectorAll('.pagination-summary'));
                if (!spans.length) return null;
                const last = spans[spans.length - 1];
                const text = (last.textContent || '').trim();
                const m = text.match(/Showing\\s+(\\d+)\\s+through\\s+(\\d+)\\s+of\\s+(\\d+)/i);
                if (!m) return null;
                return { start: parseInt(m[1], 10), end: parseInt(m[2], 10), total: parseInt(m[3], 10) };
            }"""
        )
        return info if isinstance(info, dict) else {}
    except Exception:
        return {}


async def click_next_order_details_page(page: Page, config: dict[str, Any]) -> bool:
    """Click 'Next' in the LAST .pagination div on the page.

    The order-details page contains multiple ``.pagination`` divs (the first
    ones belong to the menu-item-summary table).  We always target the last
    one so that we paginate the *orders* table.
    """
    try:
        clicked = await page.evaluate(
            """() => {
                const isVisible = (el) => {
                    if (!el) return false;
                    const r = el.getBoundingClientRect();
                    if (r.width <= 0 || r.height <= 0) return false;
                    const style = window.getComputedStyle(el);
                    if (!style) return true;
                    if (style.display === 'none' || style.visibility === 'hidden' || style.opacity === '0') return false;
                    return true;
                };
                const paginationDivs = Array.from(document.querySelectorAll('.pagination'));
                if (!paginationDivs.length) return false;
                const lastPagination = paginationDivs[paginationDivs.length - 1];
                const nextLi = lastPagination.querySelector('li.next');
                if (!nextLi) return false;
                const className = (nextLi.getAttribute('class') || '').toLowerCase();
                if (className.includes('disabled')) return false;
                const anchor = nextLi.querySelector('a');
                if (!anchor || !isVisible(anchor)) return false;
                anchor.click();
                return true;
            }"""
        )
        return bool(clicked)
    except Exception:
        pass
    return False


async def wait_for_pagination_change(
    page: Page,
    old_summary: dict[str, int],
    timeout_sec: int = 30,
) -> dict[str, int]:
    """Poll until the pagination-summary text changes from *old_summary*.

    After clicking 'Next', Toast replaces the order-detail blocks
    asynchronously.  This helper watches the LAST ``.pagination-summary``
    span until its ``start``/``end`` values differ from *old_summary*,
    indicating the new page has loaded.

    Returns the new summary dict, or the old one on timeout.
    """
    deadline = asyncio.get_event_loop().time() + max(1, timeout_sec)
    while asyncio.get_event_loop().time() < deadline:
        await asyncio.sleep(0.5)
        # Also wait for loading spinners to clear.
        await wait_for_order_details_idle(page, timeout_sec=5)
        new_summary = await get_pagination_summary(page)
        if not new_summary:
            continue
        # The page has changed when start or end differs.
        if (
            new_summary.get("start") != old_summary.get("start")
            or new_summary.get("end") != old_summary.get("end")
        ):
            return new_summary
    return old_summary


async def extract_order_detail_blocks(page: Page, config: dict[str, Any]) -> list[dict[str, Any]]:
    selectors = config.get("order_details", {}).get("order_blocks", [])
    payload = await page.evaluate(
        """(orderSelectors) => {
            const blocks = [];
            const seen = new Set();
            for (const selector of orderSelectors) {
                for (const node of Array.from(document.querySelectorAll(selector))) {
                    if (seen.has(node)) continue;
                    seen.add(node);
                    blocks.push(node);
                }
            }

            const normalize = (text) => (text || "").replace(/\\s+/g, " ").trim();
            const records = [];
            for (let idx = 0; idx < blocks.length; idx += 1) {
                const order = blocks[idx];
                const pairs = {};

                for (const row of Array.from(order.querySelectorAll("tr"))) {
                    const cells = Array.from(row.querySelectorAll("th, td"))
                        .map((el) => normalize(el.textContent))
                        .filter(Boolean);
                    if (cells.length === 2) {
                        const key = cells[0];
                        if (key && !pairs[key]) {
                            pairs[key] = cells[1];
                        }
                    }
                }

                for (const dl of Array.from(order.querySelectorAll("dl"))) {
                    const dts = Array.from(dl.querySelectorAll("dt"));
                    const dds = Array.from(dl.querySelectorAll("dd"));
                    for (let i = 0; i < Math.min(dts.length, dds.length); i += 1) {
                        const key = normalize(dts[i].textContent);
                        const val = normalize(dds[i].textContent);
                        if (key && !pairs[key]) {
                            pairs[key] = val;
                        }
                    }
                }

                const tables = Array.from(order.querySelectorAll("table")).map((table) => {
                    const headers = Array.from(table.querySelectorAll("thead th"))
                        .map((el) => normalize(el.textContent));
                    const rows = Array.from(table.querySelectorAll("tbody tr")).map((row) =>
                        Array.from(row.querySelectorAll("th, td")).map((el) => normalize(el.textContent))
                    );
                    return { headers, rows };
                });

                const byClassText = (selector) => normalize(order.querySelector(selector)?.textContent);
                const summary = {
                    discount: byClassText(".check-discounts"),
                    credits: byClassText(".check-credits"),
                    subtotal: byClassText(".check-subtotal"),
                    tax: byClassText(".check-tax"),
                    tip: byClassText(".check-tip"),
                    gratuity: byClassText(".check-gratuity"),
                    total: byClassText(".check-total"),
                };

                const summaryDetails = {};
                const detailsBlock = order.querySelector(".check-server-details");
                if (detailsBlock) {
                    const lines = (detailsBlock.innerText || "")
                        .split(/\\n+/)
                        .map((line) => normalize(line))
                        .filter(Boolean);
                    const labelBlock = detailsBlock.previousElementSibling;
                    const labels = [];
                    if (labelBlock) {
                        for (const el of Array.from(labelBlock.querySelectorAll("b"))) {
                            const label = normalize(el.textContent).replace(/:$/, "").toLowerCase();
                            if (label) labels.push(label);
                        }
                    }
                    const byLabel = {};
                    let labelIndex = 0;
                    let lastLabel = "";
                    for (const line of lines) {
                        const isContinuation = line.startsWith("(") && lastLabel;
                        if (isContinuation) {
                            byLabel[lastLabel] = `${byLabel[lastLabel]} ${line}`.trim();
                            continue;
                        }
                        if (labelIndex < labels.length) {
                            const label = labels[labelIndex];
                            byLabel[label] = line;
                            lastLabel = label;
                            labelIndex += 1;
                        } else if (lastLabel) {
                            byLabel[lastLabel] = `${byLabel[lastLabel]} ${line}`.trim();
                        }
                    }
                    if (byLabel["time opened"]) summaryDetails.time_opened = byLabel["time opened"];
                    if (byLabel["server"]) summaryDetails.server = byLabel["server"];
                    if (!summaryDetails.server && byLabel["opened by server"]) {
                        summaryDetails.server = byLabel["opened by server"];
                    }
                    if (byLabel["table"]) summaryDetails.table = byLabel["table"];
                    if (!summaryDetails.time_opened && lines.length > 0) {
                        summaryDetails.time_opened = lines[0];
                    }
                    if (!summaryDetails.server && lines.length > 1) {
                        summaryDetails.server = lines[1];
                    }
                    if (!summaryDetails.table && lines.length > 1) {
                        const fallbackIndex = Math.max(0, lines.length - 2);
                        summaryDetails.table = lines[fallbackIndex] || lines[lines.length - 1];
                    }
                }
                const guestInput = order.querySelector("#num-guests");
                if (guestInput && guestInput.value) {
                    summaryDetails.guest_count = normalize(guestInput.value);
                }
                const revenueCenter = order.querySelector("#revenue-center-name");
                if (revenueCenter) {
                    summaryDetails.revenue_center = normalize(revenueCenter.textContent);
                }

                const bodyText = order.innerText || "";
                let orderNumber = "";
                const orderHeaderText = normalize(order.querySelector("#order-summary-header")?.textContent);
                if (orderHeaderText) {
                    const match = orderHeaderText.match(/Order\\s*#\\s*(\\d+)/i);
                    if (match) orderNumber = match[1];
                }

                let source = "";
                const sourceMatch = bodyText.match(/Source\\s*:\\s*\\n+([^\\n]+)/i);
                if (sourceMatch) {
                    source = normalize(sourceMatch[1]);
                }

                let checkId = "";
                for (const el of Array.from(order.querySelectorAll(".order-detail-meta-id"))) {
                    const match = normalize(el.textContent).match(/ID\\s*:\\s*([A-Za-z0-9_-]+)/i);
                    if (match) {
                        checkId = match[1];
                        break;
                    }
                }
                if (!checkId) {
                    const form = order.querySelector("form[action*='reopencheck?id=']");
                    if (form) {
                        const action = form.getAttribute("action") || "";
                        const match = action.match(/id=([A-Za-z0-9_-]+)/i);
                        if (match) checkId = match[1];
                    }
                }
                if (!checkId) {
                    checkId = `order-${orderNumber || idx + 1}`;
                }

                const metadata = {
                    payment_id: checkId,
                    "Order #": orderNumber,
                    Source: source,
                    "Revenue Center": summaryDetails.revenue_center || "",
                };

                records.push({
                    payment_id: checkId,
                    metadata,
                    payload: {
                        pairs,
                        tables,
                        summary,
                        summaryDetails,
                        bodyText,
                    },
                    parsed_url: `${window.location.origin}${window.location.pathname}${window.location.search}#check-${checkId}`,
                });
            }

            return records;
        }""",
        selectors,
    )
    return [row for row in payload if isinstance(row, dict)]


async def capture_debug_artifacts(page: Page, artifact_dir: Path, label: str) -> None:
    artifact_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    base = artifact_dir / f"{stamp}_{label}"
    try:
        await page.screenshot(path=str(base.with_suffix(".png")), full_page=True)
    except Exception:
        pass
    try:
        html = await page.content()
        base.with_suffix(".html").write_text(html, encoding="utf-8")
    except Exception:
        pass


async def is_cloudflare_challenge(page: Page) -> bool:
    try:
        title = (await page.title()).lower()
    except Exception:
        title = ""
    if "just a moment" in title:
        return True

    markers = [
        "text=Verifying you are human",
        "text=needs to review the security of your connection",
        "input[name='cf-turnstile-response']",
        "script[src*='challenge-platform']",
    ]
    for marker in markers:
        try:
            if await page.locator(marker).first.count() > 0:
                return True
        except Exception:
            continue
    return False


async def wait_for_challenge_clear(page: Page, timeout_sec: int) -> bool:
    if timeout_sec <= 0:
        return not await is_cloudflare_challenge(page)
    deadline = asyncio.get_event_loop().time() + timeout_sec
    while asyncio.get_event_loop().time() < deadline:
        if not await is_cloudflare_challenge(page):
            return True
        await asyncio.sleep(1)
    return False


async def is_logged_out(page: Page, config: dict[str, Any]) -> bool:
    if "login" in page.url.lower():
        return True
    for selector in config["auth"]["logged_out_markers"]:
        try:
            if await page.locator(selector).first.count() > 0:
                return True
        except Exception:
            continue
    return False


async def is_authenticated(page: Page, config: dict[str, Any]) -> bool:
    url = page.url.lower()
    if "restaurants/admin/reports" in url and not await is_logged_out(page, config):
        return True
    for selector in config["auth"].get("authenticated_markers", []):
        try:
            if await page.locator(selector).first.count() > 0:
                return True
        except Exception:
            continue
    return False


async def dismiss_post_login_prompts(page: Page, config: dict[str, Any]) -> bool:
    clicked = await click_first_available(
        page,
        config["auth"].get("not_now_buttons", []),
        require_visible=True,
    )
    if clicked:
        await page.wait_for_timeout(800)
        log_event("auth_prompt_dismissed", prompt="not_now")
    return clicked


async def try_login_with_credentials(
    page: Page,
    config: dict[str, Any],
    username: str,
    password: str,
    *,
    human_min_delay_ms: int = 250,
    human_max_delay_ms: int = 900,
) -> bool:
    user_selector = await first_usable_locator(
        page,
        config["auth"].get("username_inputs", []),
    )
    if user_selector:
        await page.locator(user_selector).first.fill(username)
        await human_pause(
            page,
            min_ms=human_min_delay_ms,
            max_ms=human_max_delay_ms,
            label="auth_filled_username",
        )
        continue_selector = await first_usable_locator(page, config["auth"].get("submit_buttons", []))
        if continue_selector:
            await page.locator(continue_selector).first.click()
            await page.wait_for_timeout(1200)
            await human_pause(
                page,
                min_ms=human_min_delay_ms,
                max_ms=human_max_delay_ms,
                label="auth_clicked_continue",
            )

    pass_selector = await first_usable_locator(
        page,
        config["auth"].get("password_inputs", []),
    )
    if not pass_selector:
        return False

    await page.locator(pass_selector).first.fill(password)
    await human_pause(
        page,
        min_ms=human_min_delay_ms,
        max_ms=human_max_delay_ms,
        label="auth_filled_password",
    )
    submit_selector = await first_usable_locator(page, config["auth"].get("submit_buttons", []))
    if submit_selector:
        await page.locator(submit_selector).first.click()
        await human_pause(
            page,
            min_ms=human_min_delay_ms,
            max_ms=human_max_delay_ms,
            label="auth_clicked_submit",
        )
    return True


async def ensure_authenticated(
    page: Page,
    config: dict[str, Any],
    timeout_sec: int = 45,
    max_attempts: int = 3,
    challenge_timeout_sec: int = 120,
    credentials: tuple[str, str] | None = None,
    allow_manual_login: bool = False,
    artifact_dir: Path | None = None,
    human_min_delay_ms: int = 250,
    human_max_delay_ms: int = 900,
) -> None:
    try:
        await page.goto(ORDER_DETAILS_URL, wait_until="domcontentloaded", timeout=45000)
    except Exception as exc:
        log_event("auth_nav_warning", error=str(exc))
    await human_pause(
        page,
        min_ms=human_min_delay_ms,
        max_ms=human_max_delay_ms,
        label="auth_post_nav",
    )
    if await is_cloudflare_challenge(page):
        log_event("auth_challenge_detected", phase="initial")
        if not await wait_for_challenge_clear(page, challenge_timeout_sec):
            if artifact_dir:
                await capture_debug_artifacts(page, artifact_dir, "cloudflare_challenge_timeout")
            raise RuntimeError("AUTH_BLOCKED: Cloudflare challenge did not clear.")

    await dismiss_post_login_prompts(page, config)
    if await is_authenticated(page, config):
        return

    if not credentials:
        if allow_manual_login:
            print("Authentication required. Complete login in the opened browser window...")
            deadline = asyncio.get_event_loop().time() + timeout_sec
            while asyncio.get_event_loop().time() < deadline:
                await dismiss_post_login_prompts(page, config)
                if await is_authenticated(page, config):
                    print("Login detected. Continuing extraction.")
                    return
                await asyncio.sleep(1)
            if artifact_dir:
                await capture_debug_artifacts(page, artifact_dir, "manual_auth_timeout")
            raise TimeoutError("AUTH_FAILED: manual login timeout.")
        if artifact_dir:
            await capture_debug_artifacts(page, artifact_dir, "missing_credentials")
        raise RuntimeError("AUTH_FAILED: no usable credentials found in env file.")

    username, password = credentials
    for attempt in range(1, max(1, max_attempts) + 1):
        log_event("auth_attempt_start", attempt=attempt, max_attempts=max_attempts)
        try:
            await try_login_with_credentials(
                page,
                config,
                username,
                password,
                human_min_delay_ms=human_min_delay_ms,
                human_max_delay_ms=human_max_delay_ms,
            )
        except Exception as exc:
            log_event("auth_attempt_error", attempt=attempt, error=str(exc))

        if await is_cloudflare_challenge(page):
            log_event("auth_challenge_detected", phase="post_submit", attempt=attempt)
            if not await wait_for_challenge_clear(page, challenge_timeout_sec):
                continue

        deadline = asyncio.get_event_loop().time() + timeout_sec
        while asyncio.get_event_loop().time() < deadline:
            await dismiss_post_login_prompts(page, config)
            if await is_authenticated(page, config):
                log_event("auth_success", attempt=attempt)
                return
            await asyncio.sleep(1)

        # Re-anchor to the reports URL once per attempt; no rapid refresh loop.
        try:
            await page.goto(ORDER_DETAILS_URL, wait_until="domcontentloaded")
        except Exception:
            pass
        await dismiss_post_login_prompts(page, config)
        log_event("auth_attempt_timeout", attempt=attempt)

    if artifact_dir:
        await capture_debug_artifacts(page, artifact_dir, "auth_failed")
    raise RuntimeError("AUTH_FAILED: credential login did not reach Toast reports dashboard.")


async def set_per_page(
    page: Page,
    config: dict[str, Any],
    per_page: int = 100,
    *,
    human_min_delay_ms: int = 250,
    human_max_delay_ms: int = 900,
) -> None:
    await wait_for_payments_table_ready(page, timeout_sec=20)
    for _ in range(5):
        selector = await first_usable_locator(
            page, config["payments"]["per_page_select"], require_visible=False
        )
        if selector is not None:
            try:
                js_updated = await page.evaluate(
                    """({ selectors, value }) => {
                        for (const selector of selectors) {
                            const el = document.querySelector(selector);
                            if (!el) continue;
                            if (el.tagName.toLowerCase() !== 'select') continue;
                            el.value = value;
                            el.dispatchEvent(new Event('input', { bubbles: true }));
                            el.dispatchEvent(new Event('change', { bubbles: true }));
                            return true;
                        }
                        return false;
                    }""",
                    {"selectors": config["payments"]["per_page_select"], "value": str(per_page)},
                )
                if not js_updated:
                    await page.locator(selector).first.select_option(
                        str(per_page),
                        force=True,
                        timeout=2000,
                    )
                await page.wait_for_timeout(700)
                await human_pause(
                    page,
                    min_ms=human_min_delay_ms,
                    max_ms=human_max_delay_ms,
                    label="set_per_page",
                )
                return
            except Exception:
                pass
        await page.wait_for_timeout(500)

    if per_page == 100:
        option_selector = await first_usable_locator(
            page,
            config["payments"].get("per_page_100_option", []),
            require_visible=True,
        )
        if option_selector is not None:
            try:
                await page.locator(option_selector).first.click(timeout=3000)
                await page.wait_for_timeout(700)
                await human_pause(
                    page,
                    min_ms=human_min_delay_ms,
                    max_ms=human_max_delay_ms,
                    label="set_per_page_100",
                )
                return
            except Exception:
                pass

    print("Warning: could not find per-page selector; continuing with default.")


async def set_date_range(
    page: Page,
    config: dict[str, Any],
    start: str,
    end: str,
    *,
    human_min_delay_ms: int = 250,
    human_max_delay_ms: int = 900,
) -> None:
    # Toast reports use a date-range dropdown (Today / Last 7 Days / Custom Date).
    # If we don't switch to "Custom Date", the report can keep using the preset even if
    # we mutate the start/end inputs.
    try:
        if await page.locator("#date-dropdown-container").count() > 0:
            try:
                await page.locator("#date-dropdown-container button.dropdown-toggle").first.click(
                    timeout=2000
                )
                await page.locator(
                    "#date-dropdown-container ul.dropdown-menu a[data-value='custom']"
                ).first.click(timeout=2000)
            except Exception:
                # Fallback: click any visible custom-date option.
                try:
                    await page.locator("a[data-value='custom']").first.click(timeout=2000)
                except Exception:
                    pass
            try:
                await page.wait_for_selector(".custom-range:visible", timeout=6000)
            except Exception:
                pass
    except Exception:
        pass

    start_selector = await first_usable_locator(
        page, config["payments"]["date_start_input"], require_visible=False
    )
    end_selector = await first_usable_locator(
        page, config["payments"]["date_end_input"], require_visible=False
    )

    if start_selector is None or end_selector is None:
        print("Warning: could not find date inputs; continuing with current report dates.")
        return

    start_value = to_us_date(start)
    end_value = to_us_date(end)
    start_short = to_short_us_date(start)
    end_short = to_short_us_date(end)

    # Many Toast legacy reports (including sales reports) rely on hidden #startDate/#endDate
    # in M/D/YY format, even if other visible inputs exist.
    try:
        await page.evaluate(
            """({ startShort, endShort, startValue, endValue }) => {
                const setValue = (selector, value) => {
                    const el = document.querySelector(selector);
                    if (!el) return 0;
                    el.value = value;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    return 1;
                };
                let touched = 0;
                touched += setValue('#startDate', startShort);
                touched += setValue('#endDate', endShort);
                touched += setValue(\"input[name='reportDateStart']\", startValue);
                touched += setValue(\"input[name='reportDateEnd']\", endValue);
                return touched;
            }""",
            {
                "startShort": start_short,
                "endShort": end_short,
                "startValue": start_value,
                "endValue": end_value,
            },
        )
    except Exception:
        pass

    updated_inputs = 0
    # Keep both backing and visible date inputs in sync.
    for selector in config["payments"]["date_start_input"]:
        locator = page.locator(selector).first
        try:
            if await locator.count() > 0:
                value = start_short if "startDate" in selector else start_value
                await locator.fill(value, timeout=1000)
                updated_inputs += 1
                await human_pause(
                    page,
                    min_ms=human_min_delay_ms,
                    max_ms=human_max_delay_ms,
                    label="date_start_fill",
                )
        except Exception:
            continue
    for selector in config["payments"]["date_end_input"]:
        locator = page.locator(selector).first
        try:
            if await locator.count() > 0:
                value = end_short if "endDate" in selector else end_value
                await locator.fill(value, timeout=1000)
                updated_inputs += 1
                await human_pause(
                    page,
                    min_ms=human_min_delay_ms,
                    max_ms=human_max_delay_ms,
                    label="date_end_fill",
                )
        except Exception:
            continue

    if updated_inputs < 2:
        # Some Toast views use hidden backing inputs with custom visible controls.
        js_updated = await page.evaluate(
            """({ startSelectors, endSelectors, startValue, endValue, startShort, endShort }) => {
                const setAll = (selectors, primary, legacy, token) => {
                    let touched = 0;
                    for (const selector of selectors) {
                        const nodes = Array.from(document.querySelectorAll(selector));
                        for (const node of nodes) {
                            const id = (node.id || '').toLowerCase();
                            const name = (node.name || '').toLowerCase();
                            const useLegacy = id === token || name === token;
                            node.value = useLegacy ? legacy : primary;
                            node.dispatchEvent(new Event('input', { bubbles: true }));
                            node.dispatchEvent(new Event('change', { bubbles: true }));
                            touched += 1;
                        }
                    }
                    return touched;
                };
                const startTouched = setAll(startSelectors, startValue, startShort, 'startdate');
                const endTouched = setAll(endSelectors, endValue, endShort, 'enddate');
                return startTouched > 0 && endTouched > 0;
            }""",
            {
                "startSelectors": config["payments"]["date_start_input"],
                "endSelectors": config["payments"]["date_end_input"],
                "startValue": start_value,
                "endValue": end_value,
                "startShort": start_short,
                "endShort": end_short,
            },
        )
        if not js_updated:
            print("Warning: could not set date range; continuing with current report dates.")
            return

    # Regardless of which selectors were fill()-able, force-sync the hidden fields Toast actually reads
    # right before clicking Update. This avoids the "backing inputs changed but report still uses today"
    # failure mode.
    try:
        await page.evaluate(
            """({ startShort, endShort, startValue, endValue }) => {
                const setValue = (selector, value) => {
                    const el = document.querySelector(selector);
                    if (!el) return 0;
                    el.value = value;
                    el.dispatchEvent(new Event('input', { bubbles: true }));
                    el.dispatchEvent(new Event('change', { bubbles: true }));
                    return 1;
                };
                let touched = 0;
                touched += setValue('#startDate', startShort);
                touched += setValue('#endDate', endShort);
                touched += setValue(\"input[name='reportDateStart']\", startValue);
                touched += setValue(\"input[name='reportDateEnd']\", endValue);
                return touched;
            }""",
            {
                "startShort": start_short,
                "endShort": end_short,
                "startValue": start_value,
                "endValue": end_value,
            },
        )
    except Exception:
        pass

    apply_selector = await first_usable_locator(
        page, config["payments"]["apply_button"], require_visible=True
    )
    applied = False
    if apply_selector:
        try:
            await page.locator(apply_selector).first.click(timeout=3000)
            applied = True
            await human_pause(
                page,
                min_ms=human_min_delay_ms,
                max_ms=human_max_delay_ms,
                label="date_apply_click",
            )
        except Exception:
            applied = False

    if not applied:
        js_apply_selectors = [
            selector
            for selector in config["payments"]["apply_button"]
            if ":has-text(" not in selector and "text=" not in selector
        ]
        js_applied = await page.evaluate(
            """({ applySelectors }) => {
                const findVisible = (selector) => {
                    const nodes = Array.from(document.querySelectorAll(selector));
                    return nodes.find((el) => {
                        const rect = el.getBoundingClientRect();
                        return rect.width > 0 && rect.height > 0;
                    }) || nodes[0] || null;
                };
                for (const selector of applySelectors) {
                    const btn = findVisible(selector);
                    if (!btn) continue;
                    btn.click();
                    btn.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                    return true;
                }
                const byId = document.querySelector('#filter-apply-handler');
                if (byId) {
                    byId.click();
                    byId.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
                    return true;
                }
                return false;
            }""",
            {"applySelectors": js_apply_selectors},
        )
        if not js_applied:
            await page.keyboard.press("Enter")
    await page.wait_for_timeout(1200)
    await human_pause(
        page,
        min_ms=human_min_delay_ms,
        max_ms=human_max_delay_ms,
        label="post_date_apply",
    )
    # Helpful when debugging "0 rows" in live runs: confirm what the report thinks the date inputs are.
    try:
        values = await page.evaluate(
            """() => {
                const getVal = (sel) => {
                    const el = document.querySelector(sel);
                    return el ? (el.value || el.getAttribute('value') || '') : '';
                };
                const dateDropdown = document.querySelector('#date-dropdown-container');
                const dateLabel = dateDropdown?.querySelector('.dropdown-label')?.textContent || '';
                const customRange = document.querySelector('.custom-range');
                const customVisible = !!customRange && window.getComputedStyle(customRange).display !== 'none';
                return {
                    startDateHidden: getVal('#startDate'),
                    endDateHidden: getVal('#endDate'),
                    startDateBacking: getVal(\"input[name='reportDateStart']\"),
                    endDateBacking: getVal(\"input[name='reportDateEnd']\"),
                    dateRangeValue: dateDropdown?.getAttribute('data-value') || '',
                    dateRangeLabel: (dateLabel || '').trim(),
                    customRangeVisible: customVisible,
                };
            }"""
        )
        if isinstance(values, dict):
            log_event("date_range_values", **values)
    except Exception:
        pass


async def extract_metadata_rows(page: Page, config: dict[str, Any]) -> list[dict[str, Any]]:
    row_selector = config["payments"]["table_rows"]
    header_selector = config["payments"]["table_headers"]
    return await page.evaluate(
        """({ rowSelector, headerSelector }) => {
            const headers = Array.from(document.querySelectorAll(headerSelector))
                .map((el) => el.textContent?.trim() || "");
            const rows = Array.from(document.querySelectorAll(rowSelector));
            return rows.map((row) => {
                const cells = Array.from(row.querySelectorAll("th, td"))
                    .map((el) => (el.textContent || "").trim());
                const links = Array.from(row.querySelectorAll("a[href]"))
                    .map((el) => el.getAttribute("href") || "");
                let paymentId = "";
                for (const href of links) {
                    const match = href.match(/[?&]paymentId=([^&#]+)/i);
                    if (match) {
                        paymentId = decodeURIComponent(match[1]);
                        break;
                    }
                }
                if (!paymentId) {
                    const dataNode = row.querySelector("[data-payment-id]");
                    if (dataNode) {
                        paymentId = (dataNode.getAttribute("data-payment-id") || "").trim();
                    }
                }
                if (!paymentId) {
                    const inline = (row.textContent || "").match(/paymentId[:=\s]+([A-Za-z0-9_-]+)/i);
                    if (inline) paymentId = inline[1];
                }
                const mapped = {};
                for (let i = 0; i < Math.min(headers.length, cells.length); i += 1) {
                    if (headers[i]) mapped[headers[i]] = cells[i];
                }
                return { payment_id: paymentId, ...mapped };
            });
        }""",
        {"rowSelector": row_selector, "headerSelector": header_selector},
    )


async def click_next_page(page: Page, config: dict[str, Any]) -> bool:
    for selector in config["payments"]["next_button"]:
        locator = page.locator(selector)
        try:
            count = await locator.count()
        except Exception:
            continue
        for i in range(count):
            candidate = locator.nth(i)
            try:
                if not await candidate.is_visible():
                    continue
                disabled = await candidate.get_attribute("disabled")
                aria_disabled = await candidate.get_attribute("aria-disabled")
                class_name = (await candidate.get_attribute("class") or "").lower()
                if disabled is not None or aria_disabled == "true" or "disabled" in class_name:
                    continue
                parent_class = await candidate.evaluate(
                    "(node) => (node.parentElement?.className || '').toLowerCase()"
                )
                if "disabled" in (parent_class or ""):
                    continue
                await candidate.click()
                await wait_for_payments_table_ready(page, timeout_sec=15)
                return True
            except Exception:
                continue
    return False


PAYMENTDETAILS_PATH_FRAGMENT = "/restaurants/admin/reports/paymentdetails"
DEFAULT_PAYMENTDETAILS_URL = f"https://www.toasttab.com{PAYMENTDETAILS_PATH_FRAGMENT}"
PAYMENTDETAILS_FALLBACK_HEADERS: dict[int, str] = {
    0: "payment_id",
    3: "Order #",
    5: "Order Date",
    8: "Server",
    9: "Table",
    10: "Guest Count",
    15: "Subtotal",
    16: "Tip",
    17: "Gratuity",
    18: "Total",
    19: "Tax",
    30: "Type",
    40: "Source",
}


def extract_payment_id_from_cells(cells: list[str]) -> str:
    patterns = [
        r"[?&]paymentId=([A-Za-z0-9_-]+)",
        r"data-payment-id=['\"]?([A-Za-z0-9_-]+)",
        r"\bpaymentid[:=\s\"']+([A-Za-z0-9_-]+)",
    ]
    for cell in cells:
        text = str(cell or "")
        for pattern in patterns:
            match = re.search(pattern, text, flags=re.I)
            if match:
                return match.group(1).strip()

    first = str(cells[0] if cells else "").strip()
    if re.fullmatch(r"\d{12,}", first):
        return first
    return ""


def build_paymentdetails_url(
    template_url: str,
    start_date: str,
    end_date: str,
    offset: int,
    page_size: int,
) -> str:
    parsed = urllib.parse.urlsplit(template_url or DEFAULT_PAYMENTDETAILS_URL)
    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)

    query["reportDateRange"] = ["custom"]
    query["reportDateStart"] = [to_us_date(start_date)]
    query["reportDateEnd"] = [to_us_date(end_date)]
    query["iDisplayStart"] = [str(max(0, offset))]
    query["iDisplayLength"] = [str(max(1, page_size))]

    scheme = parsed.scheme or "https"
    netloc = parsed.netloc or "www.toasttab.com"
    path = parsed.path or PAYMENTDETAILS_PATH_FRAGMENT
    return urllib.parse.urlunsplit(
        (scheme, netloc, path, urllib.parse.urlencode(query, doseq=True), "")
    )


async def get_response_headers(response: Any) -> dict[str, str]:
    headers: dict[str, str] = {}
    try:
        raw = response.headers or {}
        headers = {str(k).lower(): str(v) for k, v in raw.items()}
    except Exception:
        headers = {}
    if headers:
        return headers

    try:
        pairs = await response.headers_array()
        return {
            str(item.get("name", "")).lower(): str(item.get("value", ""))
            for item in pairs
            if item.get("name")
        }
    except Exception:
        return {}


async def response_to_json(response: Any) -> dict[str, Any] | None:
    try:
        payload = await response.json()
        if isinstance(payload, dict):
            return payload
    except Exception:
        pass

    try:
        text = await response.text()
        payload = json.loads(text)
        if isinstance(payload, dict):
            return payload
    except Exception:
        return None
    return None


async def poll_paymentdetails_location(
    context: BrowserContext,
    location: str,
    timeout_sec: int = 45,
) -> dict[str, Any]:
    deadline = asyncio.get_event_loop().time() + max(5, timeout_sec)
    last_status = ""
    last_message = ""

    while asyncio.get_event_loop().time() < deadline:
        response = await context.request.get(location, timeout=45000)
        status = int(getattr(response, "status", 0) or 0)
        last_status = str(status)

        if status == 403:
            # Toast report exports can briefly return AccessDenied while S3 object is pending.
            await asyncio.sleep(1.0)
            continue

        payload = await response_to_json(response)
        if isinstance(payload, dict):
            if isinstance(payload.get("aaData"), list):
                return payload
            nested = payload.get("data")
            if isinstance(nested, dict) and isinstance(nested.get("aaData"), list):
                return nested
            if payload.get("status"):
                last_message = str(payload.get("message") or payload.get("status"))
        await asyncio.sleep(1.0)

    raise RuntimeError(
        f"paymentdetails_location_timeout status={last_status} message={last_message or 'n/a'}"
    )


async def fetch_paymentdetails_page(context: BrowserContext, request_url: str) -> dict[str, Any]:
    response = await context.request.get(request_url, timeout=45000)
    status = int(getattr(response, "status", 0) or 0)
    headers = await get_response_headers(response)

    if status == 202 and headers.get("location"):
        return await poll_paymentdetails_location(context, headers["location"], timeout_sec=45)
    if status == 200:
        payload = await response_to_json(response)
        if isinstance(payload, dict):
            return payload

    snippet = ""
    try:
        snippet = (await response.text())[:300].replace("\n", " ")
    except Exception:
        snippet = "n/a"
    raise RuntimeError(f"paymentdetails_request_failed status={status} body={snippet}")


def map_paymentdetails_row(
    row: Any,
    headers: list[str],
) -> dict[str, Any]:
    if isinstance(row, dict):
        mapped = {clean_text(k): clean_text(v) for k, v in row.items() if clean_text(k)}
        payment_id = (
            clean_text(mapped.get("payment_id"))
            or clean_text(mapped.get("Payment ID"))
            or extract_payment_id_from_cells([str(v) for v in mapped.values()])
        )
        return {"payment_id": payment_id, **mapped}

    if not isinstance(row, list):
        text = clean_text(row)
        payment_id = extract_payment_id_from_cells([text]) if text else ""
        return {"payment_id": payment_id, "raw_row": text}

    cells = [clean_text(cell) for cell in row]
    payment_id = extract_payment_id_from_cells(cells)
    mapped: dict[str, Any] = {}
    for index, cell in enumerate(cells):
        if not cell:
            continue
        key = ""
        if index < len(headers) and headers[index]:
            key = headers[index]
        if not key:
            key = PAYMENTDETAILS_FALLBACK_HEADERS.get(index, f"col_{index}")
        mapped[key] = cell

    if payment_id:
        mapped["payment_id"] = payment_id
    elif mapped.get("payment_id"):
        payment_id = clean_text(mapped.get("payment_id"))
    return {"payment_id": payment_id, **mapped}


def extract_paymentdetails_rows(payload: dict[str, Any]) -> tuple[int | None, list[Any]]:
    rows = payload.get("aaData")
    if isinstance(rows, list):
        total = parse_int(payload.get("iTotalDisplayRecords"))
        if total is None:
            total = parse_int(payload.get("recordsTotal"))
        return total, rows

    nested = payload.get("data")
    if isinstance(nested, dict):
        rows = nested.get("aaData")
        if isinstance(rows, list):
            total = parse_int(nested.get("iTotalDisplayRecords"))
            if total is None:
                total = parse_int(nested.get("recordsTotal"))
            return total, rows
    return None, []


async def discover_paymentdetails_template(
    page: Page,
    config: dict[str, Any],
    start_date: str,
    end_date: str,
    *,
    human_min_delay_ms: int = 250,
    human_max_delay_ms: int = 900,
) -> str:
    captured: dict[str, str] = {"url": ""}

    def on_request(request: Any) -> None:
        url = str(getattr(request, "url", ""))
        if PAYMENTDETAILS_PATH_FRAGMENT not in url:
            return
        if "?" in url or not captured["url"]:
            captured["url"] = url

    page.on("request", on_request)
    try:
        await set_date_range(
            page,
            config,
            start_date,
            end_date,
            human_min_delay_ms=human_min_delay_ms,
            human_max_delay_ms=human_max_delay_ms,
        )
        await ensure_payments_tab(page)
        await wait_for_payments_table_ready(page, timeout_sec=20)

        for _ in range(16):
            if captured["url"]:
                return captured["url"]
            await page.wait_for_timeout(250)

        perf_url = await page.evaluate(
            """() => {
                const urls = performance.getEntriesByType('resource')
                    .map((entry) => entry.name || '');
                for (let i = urls.length - 1; i >= 0; i -= 1) {
                    if (urls[i].includes('/restaurants/admin/reports/paymentdetails')) {
                        return urls[i];
                    }
                }
                return '';
            }"""
        )
        if isinstance(perf_url, str) and perf_url:
            return perf_url
    finally:
        try:
            page.remove_listener("request", on_request)
        except Exception:
            pass
    return DEFAULT_PAYMENTDETAILS_URL


async def crawl_metadata_via_ui(
    page: Page,
    config: dict[str, Any],
    max_pages: int,
) -> list[dict[str, Any]]:
    all_rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    page_count = 0

    while True:
        page_count += 1
        rows = await extract_metadata_rows(page, config)
        if not rows:
            # DataTables refresh can lag after filter/pagination actions.
            for _ in range(4):
                await page.wait_for_timeout(800)
                rows = await extract_metadata_rows(page, config)
                if rows:
                    break
        for row in rows:
            payment_id = (row.get("payment_id") or "").strip()
            if payment_id and payment_id not in seen_ids:
                seen_ids.add(payment_id)
                all_rows.append(row)

        if max_pages and page_count >= max_pages:
            break

        if not await click_next_page(page, config):
            break

    return all_rows


async def crawl_metadata(
    page: Page,
    config: dict[str, Any],
    start_date: str,
    end_date: str,
    max_pages: int,
    limit: int = 0,
    *,
    human_min_delay_ms: int = 250,
    human_max_delay_ms: int = 900,
) -> list[dict[str, Any]]:
    await page.goto(ORDER_DETAILS_URL, wait_until="domcontentloaded", timeout=45000)
    await ensure_order_details_tab(page, config)
    await set_date_range(
        page,
        config,
        start_date,
        end_date,
        human_min_delay_ms=human_min_delay_ms,
        human_max_delay_ms=human_max_delay_ms,
    )
    await ensure_order_details_tab(page, config)
    # Mirror the manual flow: wait for "Loading" to clear before deciding whether we have data.
    await wait_for_order_details_idle(page, timeout_sec=35)
    await wait_for_order_detail_blocks_ready(page, config)

    all_rows: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    page_signatures: set[str] = set()
    page_count = 0

    while True:
        page_count += 1
        raw_rows = await extract_order_detail_blocks(page, config)
        if not raw_rows:
            # Order detail pages can render asynchronously after date changes.
            for _ in range(4):
                await page.wait_for_timeout(700)
                await wait_for_order_details_idle(page, timeout_sec=15)
                raw_rows = await extract_order_detail_blocks(page, config)
                if raw_rows:
                    break

        page_added = 0
        signature_ids: list[str] = []
        for row in raw_rows:
            payment_id = clean_text(row.get("payment_id") or "")
            payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
            metadata = normalize_metadata_fields(row.get("metadata") or {})
            if not payment_id:
                payment_id = clean_text(metadata.get("payment_id") or "")
            if not payment_id or payment_id in seen_ids:
                continue

            seen_ids.add(payment_id)
            if len(signature_ids) < 6:
                signature_ids.append(payment_id)
            detail = map_detail_payload(payload, metadata_fields=metadata)
            validation_errors = detail.get("validation_errors") or []
            last_error = "; ".join(validation_errors) if validation_errors else None
            all_rows.append(
                {
                    "payment_id": payment_id,
                    "metadata": metadata,
                    "data": detail,
                    "complete": bool(detail.get("complete")),
                    "last_error": last_error,
                    "parsed_url": clean_text(row.get("parsed_url") or ORDER_DETAILS_URL),
                }
            )
            page_added += 1
            if limit and len(all_rows) >= limit:
                break

        signature = "|".join(signature_ids) if signature_ids else ""
        if signature:
            if signature in page_signatures:
                log_event(
                    "order_details_pagination_stalled",
                    page=page_count,
                    reason="repeated_page_signature",
                )
                break
            page_signatures.add(signature)

        # Read the pagination summary *after* extracting the current page so
        # we can detect when the next page has finished loading.
        current_summary = await get_pagination_summary(page)

        log_event(
            "order_details_page_fetched",
            page=page_count,
            rows=len(raw_rows),
            accepted=len(all_rows),
            page_added=page_added,
            pagination=current_summary or None,
        )

        if limit and len(all_rows) >= limit:
            break
        if page_count > 1 and page_added == 0:
            log_event(
                "order_details_pagination_stalled",
                page=page_count,
                reason="no_new_ids",
            )
            break
        if max_pages and page_count >= max_pages:
            break
        if await detect_no_items_message(page):
            break
        if not raw_rows:
            break

        # Check whether there are more pages via the pagination summary.
        if current_summary:
            if current_summary.get("end", 0) >= current_summary.get("total", 0):
                log_event(
                    "order_details_pagination_complete",
                    page=page_count,
                    collected=len(all_rows),
                    total=current_summary.get("total"),
                )
                break

        if not await click_next_order_details_page(page, config):
            break

        # Wait for the DOM to reflect the new page data instead of relying on
        # a fixed timeout.  The pagination-summary text will change once the
        # server response is rendered.
        if current_summary:
            await wait_for_pagination_change(page, current_summary, timeout_sec=30)
        else:
            # Fallback: no summary available, use heuristic idle wait.
            await human_pause(
                page,
                min_ms=max(400, human_min_delay_ms),
                max_ms=max(1200, human_max_delay_ms),
                label="order_details_page_pause",
            )
            await wait_for_order_details_idle(page, timeout_sec=15)

        await human_pause(
            page,
            min_ms=max(400, human_min_delay_ms),
            max_ms=max(1200, human_max_delay_ms),
            label="order_details_page_pause",
        )

    # Final verification: compare collected checks against the pagination total.
    final_summary = await get_pagination_summary(page)
    expected_total = final_summary.get("total", 0) if final_summary else 0
    collected = len(all_rows)
    if expected_total and collected != expected_total:
        log_event(
            "order_details_pagination_mismatch",
            collected=collected,
            expected=expected_total,
            pagination=final_summary,
        )
    elif expected_total:
        log_event(
            "order_details_pagination_verified",
            collected=collected,
            expected=expected_total,
        )

    return all_rows


def parse_decimal(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    cleaned = re.sub(r"[^0-9.-]", "", text)
    if cleaned in {"", "-", ".", "-."}:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_int(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    match = re.search(r"-?\d+", text)
    if not match:
        return None
    return int(match.group(0))


def normalize_payment_type(value: Any) -> str | None:
    text = clean_text(value)
    if not text:
        return None
    lowered = text.lower()
    if "gift" in lowered and "card" in lowered:
        return "Gift Card"
    if "credit" in lowered:
        return "credit"
    if "debit" in lowered:
        return "debit"
    if "cash" in lowered:
        return "cash"
    return text


DATETIME_INPUT_FORMATS: tuple[str, ...] = (
    "%m/%d/%Y, %I:%M:%S %p",
    "%m/%d/%Y, %I:%M %p",
    "%m/%d/%y, %I:%M:%S %p",
    "%m/%d/%y, %I:%M %p",
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%y %I:%M:%S %p",
    "%m/%d/%y %I:%M %p",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%m/%d/%y %H:%M:%S",
    "%m/%d/%y %H:%M",
    "%b %d, %Y %I:%M:%S %p",
    "%b %d, %Y %I:%M %p",
    "%b %d, %y %I:%M:%S %p",
    "%b %d, %y %I:%M %p",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %I:%M %p",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f%z",
)


def parse_datetime_flexible(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    normalized = re.sub(r"\s+", " ", text.replace(" at ", " "))
    iso_candidate = normalized.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(iso_candidate)
    except Exception:
        pass
    for fmt in DATETIME_INPUT_FORMATS:
        try:
            return datetime.strptime(normalized, fmt)
        except ValueError:
            continue
    fallback = normalized.replace(",", "")
    if fallback != normalized:
        for fmt in DATETIME_INPUT_FORMATS:
            try:
                return datetime.strptime(fallback, fmt)
            except ValueError:
                continue
    return None


def compute_turnover_minutes(opened: Any, closed: Any) -> float | None:
    opened_dt = parse_datetime_flexible(opened)
    closed_dt = parse_datetime_flexible(closed)
    if not opened_dt or not closed_dt:
        return None
    if opened_dt.tzinfo is None:
        opened_dt = opened_dt.replace(tzinfo=timezone.utc)
    else:
        opened_dt = opened_dt.astimezone(timezone.utc)
    if closed_dt.tzinfo is None:
        closed_dt = closed_dt.replace(tzinfo=timezone.utc)
    else:
        closed_dt = closed_dt.astimezone(timezone.utc)
    delta = (closed_dt - opened_dt).total_seconds()
    if delta < 0:
        return None
    return round(delta / 60.0, 2)


def pick_value(pairs: dict[str, str], candidates: list[str]) -> str | None:
    for key, value in pairs.items():
        normalized = key.lower()
        for candidate in candidates:
            if candidate in normalized and value:
                return value
    return None


def pick_metadata_value(metadata: dict[str, Any] | None, candidates: list[str]) -> str | None:
    if not metadata:
        return None
    lowered = {str(key).lower(): value for key, value in metadata.items()}
    for candidate in candidates:
        needle = candidate.lower()
        for key, value in lowered.items():
            if needle in key:
                text = str(value).strip()
                if text:
                    return text
    return None


def sanitize_server_value(value: Any) -> str | None:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    if not text:
        return None
    text = re.sub(r"^[^A-Za-z0-9]+", "", text)
    text = re.sub(r"^(?:opened by\s+server|server)\s*:\s*", "", text, flags=re.I)
    text = text.strip(" :-")
    if not text:
        return None
    if "station" in text.lower() or "device" in text.lower():
        return None
    if "(" in text and ")" in text:
        return None
    if text.lower() in {"none", "null", "n/a"}:
        return None
    if "opened by server" in text.lower():
        return None
    if re.fullmatch(r"[A-Za-z ]+:", text):
        return None
    if not re.search(r"[A-Za-z0-9]", text):
        return None
    words = text.split()
    if len(words) >= 4 and len(words) % 2 == 0:
        half = len(words) // 2
        if words[:half] == words[half:]:
            text = " ".join(words[:half])
    return text


def regex_pick(text: str, patterns: list[str]) -> str | None:
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I)
        if match:
            value = (match.group(1) or "").strip()
            if value:
                return value
    return None


def normalize_header(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def pick_row_value(mapped: dict[str, Any], candidates: list[str]) -> Any:
    for candidate in candidates:
        needle = normalize_header(candidate)
        for key, value in mapped.items():
            key_text = normalize_header(key)
            if needle == key_text or needle in key_text:
                if str(value or "").strip():
                    return value
    return None


def extract_items_from_tables(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for table in tables:
        headers = [normalize_header(h) for h in table.get("headers", [])]
        if not headers:
            continue
        has_item = any("item" in h or "menu" in h for h in headers)
        has_qty = any("qty" in h or "quantity" in h for h in headers)
        if not (has_item and has_qty):
            continue

        items: list[dict[str, Any]] = []
        for row in table.get("rows", []):
            mapped = {headers[i]: row[i] for i in range(min(len(headers), len(row)))}
            item_name = (
                pick_row_value(mapped, ["menu item", "item", "item name", "menu"])
                or next((v for k, v in mapped.items() if "item" in k), None)
            )
            modifiers = pick_row_value(mapped, ["modifiers", "modifier"])

            quantity = parse_decimal(
                pick_row_value(mapped, ["qty", "quantity", "item qty"])
                or next((v for k, v in mapped.items() if "qty" in k), None)
            )
            unit_price = parse_decimal(
                pick_row_value(mapped, ["price", "unit price", "avg price"])
                or next((v for k, v in mapped.items() if "price" in k), None)
            )
            line_discount = parse_decimal(
                pick_row_value(mapped, ["discount", "discount amount"])
                or next((v for k, v in mapped.items() if k == "discount"), None)
            )
            if line_discount is None:
                line_discount = 0.0
            line_total_net = parse_decimal(
                pick_row_value(mapped, ["net", "line total", "subtotal"])
                or next((v for k, v in mapped.items() if k == "net"), None)
            )
            if line_total_net is None and quantity is not None and unit_price is not None:
                line_total_net = round((quantity * unit_price) - (line_discount or 0.0), 2)
            line_tax = parse_decimal(
                pick_row_value(mapped, ["tax", "item tax"])
                or next((v for k, v in mapped.items() if "tax" in k), None)
            )
            line_total_with_tax = parse_decimal(
                pick_row_value(mapped, ["total", "amount", "line total with tax", "gross amount"])
                or next((v for k, v in mapped.items() if "total" in k or "amount" in k), None)
            )
            if line_total_with_tax is None and line_total_net is not None and line_tax is not None:
                line_total_with_tax = round(line_total_net + line_tax, 2)
            if line_total_with_tax is None:
                line_total_with_tax = line_total_net
            voided_value = pick_row_value(mapped, ["voided", "voided?", "void"])
            reason_value = pick_row_value(mapped, ["reason", "void reason"])
            voided = str(voided_value or "").strip().lower() in {"true", "yes", "1"}

            items.append(
                {
                    "item_name": item_name,
                    "modifiers": modifiers,
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "discount": line_discount,
                    "line_total": line_total_net,
                    "line_tax": line_tax,
                    "line_total_with_tax": line_total_with_tax,
                    "voided": voided,
                    "reason": reason_value,
                }
            )
        filtered = [item for item in items if item["item_name"]]
        if filtered:
            return filtered
    return []


def extract_discounts_from_tables(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for table in tables:
        headers = [normalize_header(h) for h in table.get("headers", [])]
        if not headers:
            continue
        has_name = any(h == "name" or "name" in h for h in headers)
        has_amount = any("amount" in h for h in headers)
        has_applied = any("applied" in h and "date" in h for h in headers)
        if not (has_name and has_amount and has_applied):
            continue

        discounts: list[dict[str, Any]] = []
        for row in table.get("rows", []):
            mapped = {headers[i]: row[i] for i in range(min(len(headers), len(row)))}
            name = pick_row_value(mapped, ["name"])
            amount = parse_decimal(pick_row_value(mapped, ["amount"]))
            if amount is None:
                amount = 0.0
            discounts.append(
                {
                    "name": name,
                    "amount": amount,
                    "applied_date": pick_row_value(mapped, ["applied date", "date applied"]),
                    "approver": pick_row_value(mapped, ["approver", "approved by"]),
                    "reason": pick_row_value(mapped, ["reason"]),
                    "comment": pick_row_value(mapped, ["comment", "notes", "note"]),
                }
            )
        filtered = [row for row in discounts if row.get("name") or row.get("amount") is not None]
        if filtered:
            return filtered
    return []


def extract_payments_from_tables(tables: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for table in tables:
        headers = [normalize_header(h) for h in table.get("headers", [])]
        if not headers:
            continue
        has_payment = any("payment" in h or "method" in h or "card" in h for h in headers)
        has_amount = any("amount" in h or "total" in h for h in headers)
        if not (has_payment and has_amount):
            continue

        payments: list[dict[str, Any]] = []
        for row in table.get("rows", []):
            mapped = {headers[i]: row[i] for i in range(min(len(headers), len(row)))}
            raw_payment_type = (
                pick_row_value(mapped, ["payment", "payment method", "method", "type"])
                or next((v for k, v in mapped.items() if "payment" in k or "method" in k), None)
            )
            payment_type = normalize_payment_type(raw_payment_type)
            card_type = pick_row_value(mapped, ["card type"]) or next(
                (v for k, v in mapped.items() if "card" in k and "last" not in k),
                None,
            )
            card_last_4 = pick_row_value(mapped, ["card last 4", "last 4"])
            if not card_last_4 and payment_type:
                card_match = re.search(r"(?:\*{4}|x{4}|ending in)\s*(\d{4})", str(payment_type), re.I)
                if card_match:
                    card_last_4 = card_match.group(1)
            if not card_last_4 and payment_type:
                suffix_match = re.search(r"\b(\d{4})\b", str(payment_type))
                if suffix_match:
                    card_last_4 = suffix_match.group(1)
            if not card_type and payment_type:
                card_type_match = re.search(
                    r"(?:credit|debit)\s*:\s*([A-Za-z]+)",
                    str(payment_type),
                    re.I,
                )
                if card_type_match:
                    card_type = card_type_match.group(1)
            if payment_type and payment_type.lower() == "gift card":
                card_type = None
                card_last_4 = None
            payments.append(
                {
                    "payment_type": payment_type,
                    "payment_date": pick_row_value(mapped, ["date", "paid at", "payment date"]),
                    "amount": parse_decimal(
                        pick_row_value(mapped, ["amount", "paid", "charge amount"])
                        or mapped.get("total")
                        or next((v for k, v in mapped.items() if "amount" in k or "total" in k), None)
                    ),
                    "tip": parse_decimal(
                        pick_row_value(mapped, ["tip"])
                        or next((v for k, v in mapped.items() if "tip" in k), None)
                    ),
                    "gratuity": parse_decimal(
                        pick_row_value(mapped, ["gratuity", "service charge"])
                        or next((v for k, v in mapped.items() if "gratuity" in k), None)
                    ),
                    "total": parse_decimal(
                        pick_row_value(mapped, ["total"])
                        or next((v for k, v in mapped.items() if "total" in k), None)
                    ),
                    "refund": parse_decimal(
                        pick_row_value(mapped, ["refund"])
                        or next((v for k, v in mapped.items() if "refund" in k), None)
                    ),
                    "status": pick_row_value(mapped, ["status"]),
                    "card_type": card_type,
                    "card_last_4": card_last_4,
                }
            )
        filtered = [payment for payment in payments if payment["payment_type"] or payment["amount"] is not None]
        if filtered:
            return filtered
    return []


def validate_detail_payload(mapped: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    subtotal = parse_decimal(mapped.get("subtotal"))
    tax = parse_decimal(mapped.get("tax"))
    tip = parse_decimal(mapped.get("tip"))
    gratuity = parse_decimal(mapped.get("gratuity"))
    discount = parse_decimal(mapped.get("discount")) or 0.0
    total = parse_decimal(mapped.get("total"))

    if subtotal is not None and tax is not None and tip is not None and gratuity is not None and total is not None:
        expected = round(subtotal + tax + tip + gratuity - discount, 2)
        if abs(expected - total) > 0.05:
            errors.append(
                f"total_mismatch: expected={expected:.2f} actual={total:.2f} "
                f"(subtotal={subtotal:.2f}, tax={tax:.2f}, tip={tip:.2f}, gratuity={gratuity:.2f}, discount={discount:.2f})"
            )

    for idx, item in enumerate(mapped.get("items") or []):
        quantity = parse_decimal(item.get("quantity"))
        unit_price = parse_decimal(item.get("unit_price"))
        line_total = parse_decimal(item.get("line_total"))
        if quantity is None or unit_price is None or line_total is None:
            continue
        line_discount = parse_decimal(item.get("discount")) or 0.0
        expected_line = round((quantity * unit_price) - line_discount, 2)
        if abs(expected_line - line_total) > 0.05:
            errors.append(
                f"line_total_mismatch[{idx}]: expected={expected_line:.2f} actual={line_total:.2f}"
            )

    return errors


def map_detail_payload(
    payload: dict[str, Any],
    metadata_fields: dict[str, Any] | None = None,
) -> dict[str, Any]:
    pairs = payload.get("pairs") or {}
    tables = payload.get("tables") or []
    body_text = payload.get("bodyText") or ""
    summary = payload.get("summary") or {}
    summary_details = payload.get("summaryDetails") or {}
    metadata = normalize_metadata_fields(metadata_fields or {})

    payments = extract_payments_from_tables(tables)
    items = extract_items_from_tables(tables)
    discounts_table = extract_discounts_from_tables(tables)

    card_type = pick_value(pairs, ["card type", "card"])
    card_last_4 = pick_value(pairs, ["last 4", "last4", "last four"])
    if not card_last_4 and payments:
        card_last_4 = payments[0].get("card_last_4")
    if not card_type and payments:
        card_type = payments[0].get("card_type")
    if not card_type:
        card_type = pick_metadata_value(metadata, ["card type", "type", "payment"])
    if not card_last_4:
        card_last_4 = pick_metadata_value(metadata, ["last 4", "last4"])
    if payments:
        first = payments[0]
        first_type = (first.get("payment_type") or "").strip().lower()
        allow_card_fill = first_type not in {"gift card"}
        if allow_card_fill and not first.get("card_type") and card_type:
            first["card_type"] = card_type
        if allow_card_fill and not first.get("card_last_4") and card_last_4:
            first["card_last_4"] = card_last_4

    regex_check_number = parse_int(
        regex_pick(
            body_text,
            [
                r"check\s*#?\s*(\d+)",
                r"order\s*#?\s*(\d+)",
            ],
        )
    )
    regex_time_opened = regex_pick(
        body_text,
        [
            r"(?:time opened|opened)\s*[:\-]?\s*(?:\n|\r\n)\s*([0-9/:\sapmAPM,]+)",
            r"(?:time opened|opened)\s*[:\-]?\s*([0-9/:\sapmAPM]+)",
        ],
    )
    regex_guest_count = parse_int(
        regex_pick(
            body_text,
            [
                r"(?:guest count|guests?|covers?)\s*[:\-]?\s*(?:\n|\r\n)\s*(\d+)",
                r"(?:guest count|guests?|covers?)\s*[:\-]?\s*(\d+)",
            ],
        )
    )
    regex_server = regex_pick(
        body_text,
        [
            r"server\s*[:\-]?\s*(?:\n|\r\n)\s*([^\n]+)",
            r"server\s*[:\-]?\s*([^\n]+)",
        ],
    )
    regex_table = regex_pick(
        body_text,
        [
            r"table\s*[:\-]?\s*(?:\n|\r\n)\s*([^\n]+)",
            r"table\s*[:\-]?\s*([^\n]+)",
        ],
    )
    regex_revenue_center = regex_pick(
        body_text,
        [
            r"revenue center\s*[:\-]?\s*(?:\n|\r\n)\s*([^\n]+)",
            r"revenue center\s*[:\-]?\s*([^\n]+)",
        ],
    )
    # Toast often renders "TOTAL:" on one line and "$0.00" on the next; allow optional "$".
    regex_subtotal = parse_decimal(regex_pick(body_text, [r"subtotal\s*:?\s*\$?\s*([0-9,]+\.\d{2})"]))
    regex_tax = parse_decimal(regex_pick(body_text, [r"\btax\b\s*:?\s*\$?\s*([0-9,]+\.\d{2})"]))
    regex_tip = parse_decimal(regex_pick(body_text, [r"\btip\b\s*:?\s*\$?\s*([0-9,]+\.\d{2})"]))
    regex_gratuity = parse_decimal(regex_pick(body_text, [r"gratuity\s*:?\s*\$?\s*([0-9,]+\.\d{2})"]))
    regex_total = parse_decimal(
        regex_pick(
            body_text,
            [
                r"\btotal\b\s*:?\s*\$?\s*([0-9,]+\.\d{2})",
                r"\btotal\b\s*:\s*(?:[A-Za-z ]+:\s*)*\$?\s*([0-9,]+\.\d{2})",
            ],
        )
    )

    subtotal = parse_decimal(summary.get("subtotal"))
    if subtotal is None:
        subtotal = parse_decimal(pick_value(pairs, ["subtotal"])) or regex_subtotal
    if subtotal is None:
        subtotal = parse_decimal(
            pick_metadata_value(metadata, ["subtotal", "amount", "net sales", "pre-tax"])
        )

    tip = parse_decimal(summary.get("tip"))
    if tip is None and payments:
        tip_values = [parse_decimal(payment.get("tip")) for payment in payments]
        tip_numbers = [value for value in tip_values if value is not None]
        if tip_numbers:
            tip = round(sum(tip_numbers), 2)
    if tip is None:
        tip = parse_decimal(pick_value(pairs, ["tip"])) or regex_tip
    if tip is None:
        tip = parse_decimal(pick_metadata_value(metadata, ["tip"]))

    gratuity = parse_decimal(summary.get("gratuity"))
    if gratuity is None and payments:
        gratuity_values = [parse_decimal(payment.get("gratuity")) for payment in payments]
        gratuity_numbers = [value for value in gratuity_values if value is not None]
        if gratuity_numbers:
            gratuity = round(sum(gratuity_numbers), 2)
    if gratuity is None:
        gratuity = parse_decimal(pick_value(pairs, ["gratuity"])) or regex_gratuity
    if gratuity is None:
        gratuity = parse_decimal(pick_metadata_value(metadata, ["gratuity", "service charge"]))

    total = parse_decimal(summary.get("total"))
    if total is None:
        total = parse_decimal(pick_value(pairs, ["total"])) or regex_total
    if total is None:
        total = parse_decimal(pick_metadata_value(metadata, ["total"]))
    if total is None and payments:
        payment_totals = [parse_decimal(payment.get("total")) for payment in payments]
        payment_total_numbers = [value for value in payment_totals if value is not None]
        if payment_total_numbers:
            total = round(sum(payment_total_numbers), 2)
    if total is None and payments:
        amount_values = [parse_decimal(payment.get("amount")) for payment in payments]
        amount_numbers = [value for value in amount_values if value is not None]
        if amount_numbers:
            tip_component = tip or 0.0
            gratuity_component = gratuity or 0.0
            total = round(sum(amount_numbers) + tip_component + gratuity_component, 2)

    discount = parse_decimal(summary.get("discount"))
    if discount is None:
        discount = parse_decimal(pick_value(pairs, ["discount"]))
    if discount is None:
        discount = parse_decimal(pick_metadata_value(metadata, ["discount"]))
    if discount is None:
        discount = 0.0

    tax = parse_decimal(summary.get("tax"))
    if tax is None:
        tax = parse_decimal(pick_value(pairs, ["tax"])) or regex_tax
    if tax is None:
        tax = parse_decimal(pick_metadata_value(metadata, ["tax"]))
    if tax is None and subtotal is not None and total is not None:
        tip_component = tip or 0.0
        gratuity_component = gratuity or 0.0
        computed = round(total - subtotal - tip_component - gratuity_component, 2)
        if computed >= 0:
            tax = computed
    if tax is None and items:
        net_sum = sum((item.get("line_total") or 0.0) for item in items)
        gross_sum = sum((item.get("line_total_with_tax") or item.get("line_total") or 0.0) for item in items)
        computed = round(gross_sum - net_sum, 2)
        if computed >= 0:
            tax = computed
    if tax is not None and abs(tax) < 0.005:
        tax = 0.0

    mapped = {
        "check_number": parse_int(pick_value(pairs, ["check #", "check number"])) or regex_check_number,
        "time_opened": (
            pick_value(pairs, ["opened", "time opened", "open time"])
            or summary_details.get("time_opened")
            or regex_time_opened
        ),
        "guest_count": (
            parse_int(pick_value(pairs, ["guest", "covers"]))
            or parse_int(summary_details.get("guest_count"))
            or regex_guest_count
        ),
        "server": (
            pick_value(pairs, ["server", "employee"])
            or summary_details.get("server")
            or regex_server
        ),
        "table": pick_value(pairs, ["table", "tab"]) or summary_details.get("table") or regex_table,
        "discount": discount,
        "discounts": discounts_table,
        "subtotal": subtotal,
        "tax": tax,
        "tip": tip,
        "gratuity": gratuity,
        "total": total,
        "revenue_center": (
            pick_value(pairs, ["revenue center", "location"])
            or summary_details.get("revenue_center")
            or regex_revenue_center
        ),
        "items": items,
        "payments": payments,
        "complete": False,
    }

    if mapped["check_number"] is None:
        mapped["check_number"] = parse_int(pick_metadata_value(metadata, ["order #", "check #"]))
    if not mapped["time_opened"]:
        mapped["time_opened"] = pick_metadata_value(metadata, ["order date", "opened"])
    if mapped["guest_count"] is None:
        mapped["guest_count"] = parse_int(pick_metadata_value(metadata, ["guest"]))
    mapped["server"] = sanitize_server_value(mapped["server"])
    if not mapped["server"]:
        mapped["server"] = sanitize_server_value(
            pick_metadata_value(metadata, ["server", "opened by"])
        )
    if not mapped["server"]:
        server_from_body = regex_pick(
            body_text,
            [
                r"Created by\s*:\s*([^\n]+)",
                r"Created by\s*\[[^\]]+\]\s*:\s*([^\n]+)",
            ],
        )
        mapped["server"] = sanitize_server_value(server_from_body)
    if not mapped["table"]:
        mapped["table"] = pick_metadata_value(metadata, ["table"])
    if not mapped["revenue_center"]:
        mapped["revenue_center"] = pick_metadata_value(metadata, ["revenue center", "dining area"])

    payment_dates = [payment.get("payment_date") for payment in payments if payment.get("payment_date")]
    time_closed = payment_dates[0] if payment_dates else None
    if not time_closed:
        time_closed = pick_value(pairs, ["payment date", "closed", "closed at"])
    if not time_closed:
        time_closed = pick_metadata_value(metadata, ["payment date", "closed", "closed at"])
    mapped["time_closed"] = time_closed
    mapped["turnover_time"] = compute_turnover_minutes(mapped.get("time_opened"), mapped.get("time_closed"))

    has_financial = mapped["total"] is not None or any(
        payment.get("amount") is not None for payment in payments
    )
    has_identity = any(
        [
            mapped["check_number"] is not None,
            bool(mapped["time_opened"]),
            bool(mapped["server"]),
        ]
    )
    if not mapped["time_closed"]:
        metadata_closed = pick_metadata_value(metadata, ["payment date", "closed", "closed at"])
        if metadata_closed:
            mapped["time_closed"] = metadata_closed
    mapped["turnover_time"] = compute_turnover_minutes(mapped.get("time_opened"), mapped.get("time_closed"))

    validation_errors = validate_detail_payload(mapped)
    mapped["validation_errors"] = validation_errors
    has_payments_or_zero_total = bool(payments) or (
        mapped.get("total") is not None and abs(float(mapped["total"])) < 0.005
    )
    mapped["complete"] = (
        bool(items)
        and has_payments_or_zero_total
        and has_financial
        and has_identity
        and not validation_errors
    )
    return mapped


async def extract_detail_payload(
    page: Page,
    payment_id: str,
    metadata_fields: dict[str, Any] | None = None,
    *,
    challenge_timeout_sec: int = 120,
) -> dict[str, Any]:
    url = ORDER_DETAILS_URL
    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
    if await is_cloudflare_challenge(page):
        log_event("auth_challenge_detected", phase="detail", payment_id=payment_id)
        if not await wait_for_challenge_clear(page, challenge_timeout_sec):
            raise RuntimeError("AUTH_BLOCKED: Cloudflare challenge did not clear during detail extraction.")
    await page.wait_for_timeout(900)
    payload = await page.evaluate(
        """() => {
            const pairs = {};

            // 2-column rows frequently contain label/value pairs in Toast reports.
            for (const row of Array.from(document.querySelectorAll("tr"))) {
                const cells = Array.from(row.querySelectorAll("th, td"))
                    .map((el) => (el.textContent || "").trim())
                    .filter(Boolean);
                if (cells.length === 2) {
                    const key = cells[0].replace(/\s+/g, " ").trim();
                    if (key && !pairs[key]) {
                        pairs[key] = cells[1];
                    }
                }
            }

            for (const dl of Array.from(document.querySelectorAll("dl"))) {
                const dts = Array.from(dl.querySelectorAll("dt"));
                const dds = Array.from(dl.querySelectorAll("dd"));
                for (let i = 0; i < Math.min(dts.length, dds.length); i += 1) {
                    const key = (dts[i].textContent || "").trim();
                    const val = (dds[i].textContent || "").trim();
                    if (key && !pairs[key]) {
                        pairs[key] = val;
                    }
                }
            }

            const tables = Array.from(document.querySelectorAll("table")).map((table) => {
                const headers = Array.from(table.querySelectorAll("thead th"))
                    .map((el) => (el.textContent || "").trim());
                const rows = Array.from(table.querySelectorAll("tbody tr")).map((row) =>
                    Array.from(row.querySelectorAll("th, td"))
                        .map((el) => (el.textContent || "").trim())
                );
                return { headers, rows };
            });

            const byClassText = (selector) => {
                const el = document.querySelector(selector);
                return (el?.textContent || "").trim();
            };
            const summary = {
                discount: byClassText(".check-discounts"),
                credits: byClassText(".check-credits"),
                subtotal: byClassText(".check-subtotal"),
                tax: byClassText(".check-tax"),
                tip: byClassText(".check-tip"),
                gratuity: byClassText(".check-gratuity"),
                total: byClassText(".check-total"),
            };

            const summaryDetails = {};
            const detailsBlock = document.querySelector(".check-server-details");
            if (detailsBlock) {
                const lines = (detailsBlock.innerText || "")
                    .split(/\\n+/)
                    .map((line) => line.trim())
                    .filter(Boolean);
                if (lines.length > 0) summaryDetails.time_opened = lines[0];
                if (lines.length > 1) summaryDetails.server = lines[1];
                if (lines.length > 4) summaryDetails.table = lines[lines.length - 2];
            }

            const guestInput = document.querySelector("#num-guests");
            if (guestInput && guestInput.value) {
                summaryDetails.guest_count = guestInput.value;
            }
            const revenueCenter = document.querySelector("#revenue-center-name");
            if (revenueCenter) {
                summaryDetails.revenue_center = (revenueCenter.textContent || "").trim();
            }

            return {
                pairs,
                tables,
                summary,
                summaryDetails,
                bodyText: (document.body?.innerText || ""),
            };
        }"""
    )
    return map_detail_payload(payload, metadata_fields=metadata_fields)


def merge_metadata(
    state: dict[str, dict[str, Any]], metadata_rows: list[dict[str, Any]]
) -> tuple[dict[str, dict[str, Any]], int]:
    added = 0
    for row in metadata_rows:
        payment_id = (row.get("payment_id") or "").strip()
        if not payment_id:
            continue
        # New flow: rows already include parsed check details from ORDER_DETAILS_URL.
        if isinstance(row.get("data"), dict):
            detail = row.get("data") or {}
            metadata = normalize_metadata_fields(row.get("metadata") or {})
            existing = state.get(payment_id) or {}
            validation_errors = detail.get("validation_errors") or []
            last_error = row.get("last_error")
            if not last_error and validation_errors:
                last_error = "; ".join(str(item) for item in validation_errors)
            if payment_id not in state:
                added += 1
            state[payment_id] = {
                "payment_id": payment_id,
                "metadata": metadata,
                "complete": bool(row.get("complete", detail.get("complete"))),
                "attempts": int(existing.get("attempts") or 0) + 1,
                "last_error": last_error,
                "extracted_at": utc_now(),
                "data": detail,
                "parsed_url": clean_text(row.get("parsed_url") or ORDER_DETAILS_URL),
            }
            continue

        if payment_id not in state:
            added += 1
            state[payment_id] = {
                "payment_id": payment_id,
                "metadata": normalize_metadata_fields(row),
                "complete": False,
                "attempts": 0,
                "last_error": None,
                "extracted_at": None,
                "data": None,
                "parsed_url": ORDER_DETAILS_URL,
            }
        else:
            state[payment_id]["metadata"] = normalize_metadata_fields(row)
            state[payment_id]["parsed_url"] = ORDER_DETAILS_URL
    return state, added


async def process_details(
    context: BrowserContext,
    state: dict[str, dict[str, Any]],
    state_path: Path,
    workers: int,
    limit: int,
    run_id: str,
    progress_path: Path,
    error_log_path: Path,
    *,
    challenge_timeout_sec: int = 120,
    human_min_delay_ms: int = 250,
    human_max_delay_ms: int = 900,
    detail_start_min_interval_ms: int = 700,
) -> None:
    pending_ids = [pid for pid, row in state.items() if not row.get("complete")]
    if limit > 0:
        pending_ids = pending_ids[:limit]
    if not pending_ids:
        print("No pending payments left.")
        return

    print(f"Processing {len(pending_ids)} payment IDs with {workers} workers...")
    semaphore = asyncio.Semaphore(max(1, workers))
    lock = asyncio.Lock()
    rate_lock = asyncio.Lock()
    throttle_lock = asyncio.Lock()
    next_start_at = 0.0
    throttle_multiplier = 1.0
    throttle_until = 0.0
    throttle_events = 0

    async def run_one(payment_id: str) -> None:
        nonlocal next_start_at, throttle_multiplier, throttle_until, throttle_events
        async with semaphore:
            page = await context.new_page()
            try:
                # Global rate-limit navigation bursts across workers.
                async with rate_lock:
                    now = asyncio.get_event_loop().time()
                    global_wait = max(0.0, throttle_until - now)
                    if global_wait:
                        await asyncio.sleep(global_wait)
                    now = asyncio.get_event_loop().time()
                    wait_for = max(0.0, next_start_at - now)
                    if wait_for:
                        await asyncio.sleep(wait_for)
                    # Add jitter around the minimum spacing.
                    interval_ms = jitter_ms(
                        max(0, int(detail_start_min_interval_ms * 0.8)),
                        max(0, int(detail_start_min_interval_ms * 1.3)),
                    )
                    interval_ms = int(max(100, interval_ms * max(1.0, throttle_multiplier)))
                    next_start_at = asyncio.get_event_loop().time() + (interval_ms / 1000.0)

                metadata_fields = normalize_metadata_fields(state[payment_id].get("metadata") or {})
                detail = await extract_detail_payload(
                    page,
                    payment_id,
                    metadata_fields=metadata_fields,
                    challenge_timeout_sec=challenge_timeout_sec,
                )
                async with lock:
                    row = state[payment_id]
                    row["attempts"] = int(row.get("attempts") or 0) + 1
                    row["data"] = detail
                    row["complete"] = bool(detail.get("complete"))
                    row["extracted_at"] = utc_now()
                    row["last_error"] = None
                    row["parsed_url"] = ORDER_DETAILS_URL
                    save_state(state_path, state)
                    save_progress(progress_path, state, run_id)
                async with throttle_lock:
                    if throttle_multiplier > 1.0:
                        throttle_multiplier = max(1.0, round(throttle_multiplier * 0.9, 3))
                await human_pause(
                    page,
                    min_ms=max(human_min_delay_ms, 500),
                    max_ms=max(human_max_delay_ms, 1500),
                    label="post_detail",
                )
            except Exception as exc:
                message = str(exc)
                async with lock:
                    row = state[payment_id]
                    row["attempts"] = int(row.get("attempts") or 0) + 1
                    row["last_error"] = message
                    row["complete"] = False
                    row["extracted_at"] = utc_now()
                    save_state(state_path, state)
                    save_progress(progress_path, state, run_id)
                    append_jsonl(
                        error_log_path,
                        {
                            "ts": utc_now(),
                            "run_id": run_id,
                            "payment_id": payment_id,
                            "error": message,
                            "attempts": row["attempts"],
                        },
                    )
                if "AUTH_BLOCKED" in message or "status=429" in message or "status=403" in message:
                    async with throttle_lock:
                        throttle_events += 1
                        throttle_multiplier = min(8.0, max(1.5, throttle_multiplier * 1.65))
                        cooldown_base = min(120.0, float(2 ** min(throttle_events, 7)))
                        cooldown = cooldown_base + (jitter_ms(0, 1500) / 1000.0)
                        throttle_until = max(throttle_until, asyncio.get_event_loop().time() + cooldown)
                        log_event(
                            "detail_throttle_backoff",
                            run_id=run_id,
                            payment_id=payment_id,
                            throttle_multiplier=round(throttle_multiplier, 3),
                            cooldown_seconds=round(cooldown, 2),
                            throttle_events=throttle_events,
                        )
                if "AUTH_BLOCKED" in message:
                    raise
            finally:
                await page.close()

    await asyncio.gather(*(run_one(payment_id) for payment_id in pending_ids))


def build_launch_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    launch_kwargs: dict[str, Any] = {
        "user_data_dir": args.user_data_dir,
        "headless": args.headless,
        "args": ["--disable-blink-features=AutomationControlled"],
    }
    # Default to stable system Chrome (avoids some macOS crashes with bundled/Testing Chromium builds).
    launch_kwargs["channel"] = (args.browser_channel or "chrome").strip() or "chrome"
    if args.headless:
        launch_kwargs["user_agent"] = args.headless_user_agent
    return launch_kwargs


async def logout_and_reset_profile(args: argparse.Namespace) -> None:
    log_event("auth_block_recovery", step="logout_start")
    try:
        async with async_playwright() as p:
            launch_kwargs = build_launch_kwargs(args)
            context = await p.chromium.launch_persistent_context(**launch_kwargs)
            page = context.pages[0] if context.pages else await context.new_page()
            await page.goto("https://www.toasttab.com/logout", wait_until="domcontentloaded", timeout=45000)
            await page.wait_for_timeout(1000)
            await context.close()
    except Exception as exc:
        log_event("auth_block_recovery", step="logout_error", error=str(exc))

    if args.reset_profile_on_auth_block:
        profile_path = Path(args.user_data_dir)
        try:
            if profile_path.exists():
                shutil.rmtree(profile_path)
            profile_path.mkdir(parents=True, exist_ok=True)
            log_event("auth_block_recovery", step="profile_reset_done", profile=str(profile_path))
        except Exception as exc:
            log_event("auth_block_recovery", step="profile_reset_error", error=str(exc))


async def run_once(
    args: argparse.Namespace,
    config: dict[str, Any],
    state_path: Path,
    credentials: tuple[str, str] | None,
    run_id: str,
) -> None:
    state = load_state(state_path)
    menu_summary_path = Path(args.menu_summary_file)
    progress_path = Path(args.progress_file)
    error_log_path = Path(args.error_log_file)
    artifact_dir = Path(args.artifact_dir) / run_id
    artifact_dir.mkdir(parents=True, exist_ok=True)
    log_event("run_start", run_id=run_id, state_file=str(state_path))
    save_progress(progress_path, state, run_id)

    metadata_required = args.refresh_metadata or not args.skip_metadata
    if metadata_required and (not args.start_date or not args.end_date):
        raise SystemExit("--start-date and --end-date are required unless --skip-metadata is used.")

    async with async_playwright() as p:
        launch_kwargs = build_launch_kwargs(args)
        try:
            context = await p.chromium.launch_persistent_context(**launch_kwargs)
        except PlaywrightError as exc:
            if "ProcessSingleton" in str(exc) or "SingletonLock" in str(exc):
                raise RuntimeError(
                    "PROFILE_LOCKED: close any Chrome/Chromium using this --user-data-dir and retry."
                ) from exc
            raise

        page = context.pages[0] if context.pages else await context.new_page()
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
        )

        await ensure_authenticated(
            page=page,
            config=config,
            timeout_sec=max(5, args.auth_timeout_sec),
            max_attempts=max(1, args.auth_max_attempts),
            challenge_timeout_sec=max(5, args.challenge_timeout_sec),
            credentials=credentials,
            allow_manual_login=args.allow_manual_login,
            artifact_dir=artifact_dir,
            human_min_delay_ms=max(0, args.human_min_delay_ms),
            human_max_delay_ms=max(0, args.human_max_delay_ms),
        )

        if metadata_required:
            log_event("metadata_crawl_start", run_id=run_id)
            metadata_rows = await crawl_metadata(
                page=page,
                config=config,
                start_date=args.start_date,
                end_date=args.end_date,
                max_pages=max(0, args.max_pages),
                limit=max(0, args.limit),
                human_min_delay_ms=max(0, args.human_min_delay_ms),
                human_max_delay_ms=max(0, args.human_max_delay_ms),
            )
            if not metadata_rows:
                no_items = await detect_no_items_message(page)
                log_event(
                    "order_details_zero_rows",
                    run_id=run_id,
                    no_items_snippet=no_items,
                    url=ORDER_DETAILS_URL,
                )
                await save_order_details_debug_artifacts(
                    page, artifact_dir, "order_details_zero_rows"
                )
            state, added = merge_metadata(state, metadata_rows)
            save_state(state_path, state)
            save_progress(progress_path, state, run_id)
            log_event("metadata_crawl_done", run_id=run_id, rows=len(metadata_rows), new_payment_ids=added)

            log_event("menu_summary_crawl_start", run_id=run_id)
            try:
                menu_summary_rows = await crawl_menu_item_summary(
                    page=page,
                    config=config,
                    start_date=args.start_date,
                    end_date=args.end_date,
                    max_pages=max(0, args.max_pages),
                    human_min_delay_ms=max(0, args.human_min_delay_ms),
                    human_max_delay_ms=max(0, args.human_max_delay_ms),
                )
                save_menu_summary(menu_summary_path, menu_summary_rows)
                log_event(
                    "menu_summary_crawl_done",
                    run_id=run_id,
                    rows=len(menu_summary_rows),
                    output=str(menu_summary_path),
                )
            except Exception as exc:
                log_event("menu_summary_crawl_failed", run_id=run_id, error=str(exc))

            if args.metadata_only:
                log_event("run_exit", run_id=run_id, reason="metadata_only", total=len(state))
                await context.close()
                return
        else:
            log_event("metadata_crawl_skipped", run_id=run_id)
        log_event(
            "detail_fetch_skipped",
            run_id=run_id,
            reason="order_details_page_contains_full_check_data",
        )

        completed = sum(1 for row in state.values() if row.get("complete"))
        incomplete = len(state) - completed
        save_progress(progress_path, state, run_id)
        log_event("run_complete", run_id=run_id, total=len(state), complete=completed, incomplete=incomplete)
        await context.close()


async def run() -> None:
    args = parse_args()
    state_path = Path(args.state_file)
    config = load_config(args.config)
    env_values = load_env_values(args.env_file)
    credentials = resolve_credentials(env_values, args.user_var, args.pass_var)

    total_attempts = max(0, args.auth_block_restarts) + 1
    for attempt in range(1, total_attempts + 1):
        run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"_r{attempt}"
        try:
            await run_once(
                args=args,
                config=config,
                state_path=state_path,
                credentials=credentials,
                run_id=run_id,
            )
            return
        except RuntimeError as exc:
            message = str(exc)
            if "AUTH_BLOCKED" in message and attempt < total_attempts:
                log_event(
                    "run_restart",
                    reason="auth_blocked",
                    attempt=attempt,
                    max_attempts=total_attempts,
                )
                cooldown = max(0, int(args.auth_block_cooldown_sec))
                if cooldown:
                    extra = jitter_ms(0, min(30, cooldown))
                    sleep_for = cooldown + extra
                    log_event("auth_block_cooldown", seconds=sleep_for)
                    await asyncio.sleep(sleep_for)
                await logout_and_reset_profile(args)
                continue
            raise


if __name__ == "__main__":
    asyncio.run(run())
