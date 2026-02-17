"""ETL loader: loads a single daily JSON file into PostgreSQL.

Processes one file at a time:
1. Reads the daily JSON (envelope or bare list format)
2. Auto-upserts dimension records (servers, revenue centers, menu items)
3. Computes all derived columns
4. Inserts/upserts checks, items, payments, discounts
5. Tracks price changes in menu_item_prices
6. Logs the load in etl_load_log

All operations are idempotent via ON CONFLICT.
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from transforms import (
    classify_meal_period,
    classify_menu_item,
    classify_party_size,
    dollars_to_cents,
    dollars_to_cents_or_zero,
    parse_currency,
    parse_toast_datetime,
    safe_int,
    safe_numeric,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _parse_business_date(file_path: Path, envelope: dict | None) -> date:
    """Extract business_date from the envelope or filename."""
    if envelope and envelope.get("from_date"):
        return date.fromisoformat(envelope["from_date"])
    # Filename convention: YYYY-MM-DD.json or state_YYYY-MM-DD.json
    stem = file_path.stem.replace("state_", "")
    return date.fromisoformat(stem)


def _ensure_restaurant(cur: Any, name: str, city: str = "New York", state: str = "NY") -> int:
    """Get or create restaurant, return restaurant_id."""
    cur.execute(
        """INSERT INTO restaurants (name, city, state)
           VALUES (%s, %s, %s)
           ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
           RETURNING restaurant_id""",
        (name, city, state),
    )
    return cur.fetchone()[0]


def _ensure_revenue_center(cur: Any, restaurant_id: int, name: str) -> int | None:
    """Get or create revenue_center, return revenue_center_id."""
    if not name:
        return None
    cur.execute(
        """INSERT INTO revenue_centers (restaurant_id, name)
           VALUES (%s, %s)
           ON CONFLICT (restaurant_id, name) DO UPDATE SET name = EXCLUDED.name
           RETURNING revenue_center_id""",
        (restaurant_id, name),
    )
    return cur.fetchone()[0]


def _ensure_server(cur: Any, restaurant_id: int, name: str, business_date: date) -> int | None:
    """Get or create server, update first/last seen dates, return server_id."""
    if not name:
        return None
    cur.execute(
        """INSERT INTO servers (restaurant_id, name, first_seen_at, last_seen_at)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT (restaurant_id, name) DO UPDATE SET
               first_seen_at = LEAST(servers.first_seen_at, EXCLUDED.first_seen_at),
               last_seen_at = GREATEST(servers.last_seen_at, EXCLUDED.last_seen_at)
           RETURNING server_id""",
        (restaurant_id, name, business_date, business_date),
    )
    return cur.fetchone()[0]


def _ensure_menu_item(
    cur: Any, restaurant_id: int, item_name: str,
    menu_group: str | None, menu: str | None, business_date: date,
) -> int | None:
    """Get or create menu_item, return menu_item_id."""
    if not item_name:
        return None
    classification = classify_menu_item(item_name, menu_group, menu)
    cur.execute(
        """INSERT INTO menu_items (
               restaurant_id, item_name, menu_group, menu,
               category, is_food, is_beverage, is_alcohol,
               first_seen_at, last_seen_at
           )
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
           ON CONFLICT (restaurant_id, item_name) DO UPDATE SET
               menu_group = COALESCE(EXCLUDED.menu_group, menu_items.menu_group),
               menu = COALESCE(EXCLUDED.menu, menu_items.menu),
               category = COALESCE(EXCLUDED.category, menu_items.category),
               is_food = EXCLUDED.is_food,
               is_beverage = EXCLUDED.is_beverage,
               is_alcohol = EXCLUDED.is_alcohol,
               first_seen_at = LEAST(menu_items.first_seen_at, EXCLUDED.first_seen_at),
               last_seen_at = GREATEST(menu_items.last_seen_at, EXCLUDED.last_seen_at)
           RETURNING menu_item_id""",
        (
            restaurant_id, item_name, menu_group, menu,
            classification["category"], classification["is_food"],
            classification["is_beverage"], classification["is_alcohol"],
            business_date, business_date,
        ),
    )
    return cur.fetchone()[0]


def _track_price(
    cur: Any, restaurant_id: int, menu_item_id: int | None,
    item_name: str, unit_price_cents: int | None, business_date: date,
) -> None:
    """Track a price observation for a menu item. unit_price_cents is in cents."""
    if unit_price_cents is None or unit_price_cents <= 0 or not item_name:
        return
    unit_price = unit_price_cents
    cur.execute(
        """INSERT INTO menu_item_prices (
               restaurant_id, menu_item_id, item_name, unit_price,
               first_seen_date, last_seen_date, observation_count
           )
           VALUES (%s, %s, %s, %s, %s, %s, 1)
           ON CONFLICT (restaurant_id, item_name, unit_price) DO UPDATE SET
               menu_item_id = COALESCE(EXCLUDED.menu_item_id, menu_item_prices.menu_item_id),
               first_seen_date = LEAST(menu_item_prices.first_seen_date, EXCLUDED.first_seen_date),
               last_seen_date = GREATEST(menu_item_prices.last_seen_date, EXCLUDED.last_seen_date),
               observation_count = menu_item_prices.observation_count + 1""",
        (restaurant_id, menu_item_id, item_name, unit_price, business_date, business_date),
    )


def _load_check(
    cur: Any, restaurant_id: int, record: dict, business_date: date,
    menu_item_cache: dict,
) -> tuple[int, int]:
    """Load a single check record. Returns (check_id, items_loaded)."""
    payment_id = str(record.get("payment_id") or "").strip()
    if not payment_id:
        return 0, 0

    data = record.get("data") or {}
    metadata = record.get("metadata") or {}

    # Parse times
    time_opened = parse_toast_datetime(data.get("time_opened"))
    time_closed = parse_toast_datetime(data.get("time_closed"))
    turnover_minutes = data.get("turnover_time")

    # Dimension lookups
    server_name = data.get("server")
    rev_center_name = data.get("revenue_center")
    server_id = _ensure_server(cur, restaurant_id, server_name, business_date)
    rev_center_id = _ensure_revenue_center(cur, restaurant_id, rev_center_name)

    # Derived fields
    hour_opened = time_opened.hour if time_opened else None
    day_of_week = time_opened.weekday() if time_opened else None  # 0=Mon, 6=Sun
    is_weekend = day_of_week >= 5 if day_of_week is not None else None
    meal_period = classify_meal_period(hour_opened, day_of_week)

    guest_count = safe_int(data.get("guest_count"))
    party_size_category = classify_party_size(guest_count)

    # All monetary values stored as integer cents
    subtotal = dollars_to_cents_or_zero(data.get("subtotal"))
    discount = dollars_to_cents_or_zero(data.get("discount"))
    tax = dollars_to_cents_or_zero(data.get("tax"))
    tip = dollars_to_cents_or_zero(data.get("tip"))
    gratuity = dollars_to_cents_or_zero(data.get("gratuity"))
    total = dollars_to_cents_or_zero(data.get("total"))

    # tip_percentage stays as a float percentage (not cents)
    tip_percentage = (
        min(round(tip / subtotal * 100, 2), 999.99)
        if subtotal and subtotal > 0 else None
    )
    # check_avg_per_guest in cents
    check_avg_per_guest = (
        round(subtotal / guest_count) if guest_count and guest_count > 0 else None
    )

    has_discount = discount > 0
    items_list = data.get("items") or []
    has_void = any(item.get("voided") for item in items_list)

    source = metadata.get("Source", "In Store")
    order_number = safe_int(metadata.get("Order #"))

    # Upsert check
    cur.execute(
        """INSERT INTO checks (
               restaurant_id, payment_id, check_number, business_date,
               time_opened, time_closed, turnover_minutes,
               server_id, revenue_center_id, server_name, revenue_center,
               table_name, tab_name, guest_count,
               subtotal, discount, tax, tip, gratuity, total,
               hour_opened, meal_period, day_of_week, is_weekend,
               party_size_category, tip_percentage, check_avg_per_guest,
               has_discount, has_void,
               source, order_number, extracted_at, raw_data, loaded_at
           )
           VALUES (
               %s, %s, %s, %s,
               %s, %s, %s,
               %s, %s, %s, %s,
               %s, %s, %s,
               %s, %s, %s, %s, %s, %s,
               %s, %s, %s, %s,
               %s, %s, %s,
               %s, %s,
               %s, %s, %s, %s::jsonb, NOW()
           )
           ON CONFLICT (restaurant_id, payment_id) DO UPDATE SET
               check_number = EXCLUDED.check_number,
               business_date = EXCLUDED.business_date,
               time_opened = EXCLUDED.time_opened,
               time_closed = EXCLUDED.time_closed,
               turnover_minutes = EXCLUDED.turnover_minutes,
               server_id = EXCLUDED.server_id,
               revenue_center_id = EXCLUDED.revenue_center_id,
               server_name = EXCLUDED.server_name,
               revenue_center = EXCLUDED.revenue_center,
               table_name = EXCLUDED.table_name,
               tab_name = EXCLUDED.tab_name,
               guest_count = EXCLUDED.guest_count,
               subtotal = EXCLUDED.subtotal,
               discount = EXCLUDED.discount,
               tax = EXCLUDED.tax,
               tip = EXCLUDED.tip,
               gratuity = EXCLUDED.gratuity,
               total = EXCLUDED.total,
               hour_opened = EXCLUDED.hour_opened,
               meal_period = EXCLUDED.meal_period,
               day_of_week = EXCLUDED.day_of_week,
               is_weekend = EXCLUDED.is_weekend,
               party_size_category = EXCLUDED.party_size_category,
               tip_percentage = EXCLUDED.tip_percentage,
               check_avg_per_guest = EXCLUDED.check_avg_per_guest,
               has_discount = EXCLUDED.has_discount,
               has_void = EXCLUDED.has_void,
               source = EXCLUDED.source,
               order_number = EXCLUDED.order_number,
               extracted_at = EXCLUDED.extracted_at,
               raw_data = EXCLUDED.raw_data,
               loaded_at = NOW()
           RETURNING check_id""",
        (
            restaurant_id, payment_id, safe_int(data.get("check_number")), business_date,
            time_opened, time_closed, turnover_minutes,
            server_id, rev_center_id, server_name, rev_center_name,
            data.get("table"), data.get("tab_name"), guest_count,
            subtotal, discount, tax, tip, gratuity, total,
            hour_opened, meal_period, day_of_week, is_weekend,
            party_size_category, tip_percentage, check_avg_per_guest,
            has_discount, has_void,
            source, order_number, record.get("extracted_at"),
            json.dumps(record),
        ),
    )
    check_id = cur.fetchone()[0]

    # Delete existing child rows for idempotent reload
    cur.execute("DELETE FROM check_items WHERE check_id = %s", (check_id,))
    cur.execute("DELETE FROM check_payments WHERE check_id = %s", (check_id,))
    cur.execute("DELETE FROM check_discounts WHERE check_id = %s", (check_id,))

    # Load items
    items_loaded = 0
    for idx, item in enumerate(items_list):
        item_name = item.get("item_name")
        unit_price_cents = dollars_to_cents(item.get("unit_price"))

        # Lookup menu_item_id from cache or DB
        cache_key = (restaurant_id, item_name)
        if cache_key not in menu_item_cache:
            menu_item_id = _ensure_menu_item(cur, restaurant_id, item_name, None, None, business_date)
            menu_item_cache[cache_key] = menu_item_id
        else:
            menu_item_id = menu_item_cache[cache_key]

        # Track price (in cents)
        _track_price(cur, restaurant_id, menu_item_id, item_name, unit_price_cents, business_date)

        cur.execute(
            """INSERT INTO check_items (
                   check_id, restaurant_id, menu_item_id, item_index,
                   item_name, modifiers, quantity, unit_price, discount,
                   line_total, line_tax, line_total_with_tax, voided, void_reason
               )
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (check_id, item_index) DO UPDATE SET
                   menu_item_id = EXCLUDED.menu_item_id,
                   item_name = EXCLUDED.item_name,
                   modifiers = EXCLUDED.modifiers,
                   quantity = EXCLUDED.quantity,
                   unit_price = EXCLUDED.unit_price,
                   discount = EXCLUDED.discount,
                   line_total = EXCLUDED.line_total,
                   line_tax = EXCLUDED.line_tax,
                   line_total_with_tax = EXCLUDED.line_total_with_tax,
                   voided = EXCLUDED.voided,
                   void_reason = EXCLUDED.void_reason""",
            (
                check_id, restaurant_id, menu_item_id, idx,
                item_name, item.get("modifiers"), item.get("quantity"),
                unit_price_cents, dollars_to_cents(item.get("discount")),
                dollars_to_cents(item.get("line_total")), dollars_to_cents(item.get("line_tax")),
                dollars_to_cents(item.get("line_total_with_tax")),
                bool(item.get("voided")), item.get("reason"),
            ),
        )
        items_loaded += 1

    # Load payments (monetary values in cents)
    for idx, payment in enumerate(data.get("payments") or []):
        payment_date = parse_toast_datetime(payment.get("payment_date"))
        cur.execute(
            """INSERT INTO check_payments (
                   check_id, restaurant_id, payment_index,
                   payment_type, payment_date, amount, tip, gratuity,
                   total, refund, status, card_type, card_last_4
               )
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (check_id, payment_index) DO UPDATE SET
                   payment_type = EXCLUDED.payment_type,
                   payment_date = EXCLUDED.payment_date,
                   amount = EXCLUDED.amount,
                   tip = EXCLUDED.tip,
                   gratuity = EXCLUDED.gratuity,
                   total = EXCLUDED.total,
                   refund = EXCLUDED.refund,
                   status = EXCLUDED.status,
                   card_type = EXCLUDED.card_type,
                   card_last_4 = EXCLUDED.card_last_4""",
            (
                check_id, restaurant_id, idx,
                payment.get("payment_type"), payment_date,
                dollars_to_cents(payment.get("amount")), dollars_to_cents(payment.get("tip")),
                dollars_to_cents(payment.get("gratuity")), dollars_to_cents(payment.get("total")),
                dollars_to_cents(payment.get("refund")), payment.get("status"),
                payment.get("card_type"), payment.get("card_last_4"),
            ),
        )

    # Load discounts (amount in cents)
    for idx, disc in enumerate(data.get("discounts") or []):
        applied_date = parse_toast_datetime(disc.get("applied_date"))
        cur.execute(
            """INSERT INTO check_discounts (
                   check_id, restaurant_id, discount_index,
                   discount_name, amount, applied_date,
                   approver, reason, comment
               )
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (check_id, discount_index) DO UPDATE SET
                   discount_name = EXCLUDED.discount_name,
                   amount = EXCLUDED.amount,
                   applied_date = EXCLUDED.applied_date,
                   approver = EXCLUDED.approver,
                   reason = EXCLUDED.reason,
                   comment = EXCLUDED.comment""",
            (
                check_id, restaurant_id, idx,
                disc.get("name"), dollars_to_cents(disc.get("amount")), applied_date,
                disc.get("approver"), disc.get("reason"), disc.get("comment"),
            ),
        )

    return check_id, items_loaded


def _load_menu_summary(
    cur: Any, restaurant_id: int, business_date: date,
    summary_rows: list[dict], menu_item_cache: dict,
) -> int:
    """Load menu item daily summary rows. Returns count loaded."""
    loaded = 0
    for row in summary_rows:
        item_name = row.get("Menu Item")
        if not item_name:
            continue
        menu_group = row.get("Menu Group")
        menu = row.get("Menu")
        item_qty = safe_int(row.get("Item Qty"))
        net_amount = parse_currency(row.get("Net Amount"))

        cache_key = (restaurant_id, item_name)
        if cache_key not in menu_item_cache:
            menu_item_id = _ensure_menu_item(
                cur, restaurant_id, item_name, menu_group, menu, business_date,
            )
            menu_item_cache[cache_key] = menu_item_id
        else:
            menu_item_id = menu_item_cache[cache_key]

        cur.execute(
            """INSERT INTO menu_item_daily_summary (
                   restaurant_id, business_date, menu_item_id,
                   item_name, menu_group, menu, item_qty, net_amount
               )
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (restaurant_id, business_date, item_name) DO UPDATE SET
                   menu_item_id = EXCLUDED.menu_item_id,
                   menu_group = EXCLUDED.menu_group,
                   menu = EXCLUDED.menu,
                   item_qty = EXCLUDED.item_qty,
                   net_amount = EXCLUDED.net_amount""",
            (restaurant_id, business_date, menu_item_id,
             item_name, menu_group, menu, item_qty, net_amount),
        )
        loaded += 1
    return loaded


def load_daily_file(
    conn: Any,
    file_path: Path,
    restaurant_name: str = "Quality Italian",
) -> dict:
    """Load a single daily JSON file into the database.

    Args:
        conn: psycopg connection
        file_path: Path to the daily JSON file
        restaurant_name: Name of the restaurant

    Returns:
        dict with load statistics
    """
    started_at = _utc_now()

    # Read and parse
    raw = json.loads(file_path.read_text(encoding="utf-8"))

    # Handle two formats: envelope (with checks key) or bare list
    if isinstance(raw, dict):
        envelope = raw
        checks = raw.get("checks") or []
        menu_summary = raw.get("menu_items_summary") or []
    elif isinstance(raw, list):
        envelope = None
        checks = raw
        menu_summary = []
    else:
        raise ValueError(f"Unexpected JSON structure in {file_path}")

    business_date = _parse_business_date(file_path, envelope)

    with conn.cursor() as cur:
        restaurant_id = _ensure_restaurant(cur, restaurant_name)

        # Log the load start
        cur.execute(
            """INSERT INTO etl_load_log (restaurant_id, business_date, source_file, status)
               VALUES (%s, %s, %s, 'running')
               ON CONFLICT (restaurant_id, business_date, source_file) DO UPDATE SET
                   started_at = NOW(),
                   status = 'running',
                   error_message = NULL
               RETURNING load_id""",
            (restaurant_id, business_date, file_path.name),
        )
        load_id = cur.fetchone()[0]

        # Shared menu item cache for this file
        menu_item_cache: dict[tuple[int, str], int | None] = {}

        checks_loaded = 0
        total_items = 0

        for record in checks:
            if not isinstance(record, dict):
                continue
            check_id, items_loaded = _load_check(
                cur, restaurant_id, record, business_date, menu_item_cache,
            )
            if check_id:
                checks_loaded += 1
                total_items += items_loaded

        # Load menu summary
        summary_loaded = _load_menu_summary(
            cur, restaurant_id, business_date, menu_summary, menu_item_cache,
        )

        # Update load log
        completed_at = _utc_now()
        cur.execute(
            """UPDATE etl_load_log SET
                   checks_loaded = %s, items_loaded = %s,
                   completed_at = %s, status = 'complete'
               WHERE load_id = %s""",
            (checks_loaded, total_items, completed_at, load_id),
        )

    conn.commit()

    return {
        "file": str(file_path),
        "business_date": business_date.isoformat(),
        "checks_loaded": checks_loaded,
        "items_loaded": total_items,
        "menu_summary_loaded": summary_loaded,
        "duration_sec": round((_utc_now() - started_at).total_seconds(), 2),
    }


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Load a single daily JSON file")
    parser.add_argument("file", type=Path, help="Path to daily JSON file")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    parser.add_argument("--restaurant", default="Quality Italian")
    args = parser.parse_args()

    if not args.database_url:
        print("DATABASE_URL is required", file=sys.stderr)
        raise SystemExit(1)

    if not args.file.exists():
        print(f"File not found: {args.file}", file=sys.stderr)
        raise SystemExit(1)

    import psycopg

    with psycopg.connect(args.database_url) as conn:
        result = load_daily_file(conn, args.file, args.restaurant)
        print(json.dumps(result, indent=2))
