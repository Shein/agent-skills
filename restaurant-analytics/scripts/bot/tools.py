"""Query functions the bot can call. Each function runs a query and returns formatted results.

All functions take a database connection and return dicts suitable for bot responses.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from . import queries


def _decimal_default(obj: Any) -> Any:
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError(f"Not JSON serializable: {type(obj)}")


def _fetchall_dicts(cur: Any) -> list[dict]:
    """Fetch all rows as list of dicts."""
    cols = [desc[0] for desc in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


def _get_restaurant_id(cur: Any, restaurant_name: str = "Quality Italian") -> int:
    cur.execute("SELECT restaurant_id FROM restaurants WHERE name = %s", (restaurant_name,))
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Restaurant '{restaurant_name}' not found")
    return row[0]


def daily_summary(
    conn: Any,
    start_date: str,
    end_date: str,
    restaurant_name: str = "Quality Italian",
) -> dict:
    """Get daily revenue summary for a date range."""
    with conn.cursor() as cur:
        rid = _get_restaurant_id(cur, restaurant_name)

        # Period totals
        cur.execute(queries.PERIOD_SUMMARY, {
            "restaurant_id": rid, "start_date": start_date, "end_date": end_date,
        })
        totals = _fetchall_dicts(cur)

        # Daily breakdown
        cur.execute(queries.DAILY_SUMMARY, {
            "restaurant_id": rid, "start_date": start_date, "end_date": end_date,
        })
        daily = _fetchall_dicts(cur)

    return {"period_totals": totals[0] if totals else {}, "daily": daily}


def server_leaderboard(
    conn: Any,
    start_date: str,
    end_date: str,
    limit: int = 20,
    restaurant_name: str = "Quality Italian",
) -> dict:
    """Rank servers by revenue for a date range."""
    with conn.cursor() as cur:
        rid = _get_restaurant_id(cur, restaurant_name)
        cur.execute(queries.SERVER_LEADERBOARD, {
            "restaurant_id": rid, "start_date": start_date,
            "end_date": end_date, "limit": limit,
        })
        rows = _fetchall_dicts(cur)
    return {"servers": rows}


def server_detail(
    conn: Any,
    server_name: str,
    start_date: str,
    end_date: str,
    restaurant_name: str = "Quality Italian",
) -> dict:
    """Get detailed performance for a specific server."""
    with conn.cursor() as cur:
        rid = _get_restaurant_id(cur, restaurant_name)
        cur.execute(queries.SERVER_DETAIL, {
            "restaurant_id": rid, "server_name": server_name,
            "start_date": start_date, "end_date": end_date,
        })
        rows = _fetchall_dicts(cur)
    return {"server_name": server_name, "daily": rows}


def menu_item_performance(
    conn: Any,
    start_date: str,
    end_date: str,
    item_name: str | None = None,
    limit: int = 25,
    restaurant_name: str = "Quality Italian",
) -> dict:
    """Get menu item performance. If item_name is provided, show weekly trend."""
    with conn.cursor() as cur:
        rid = _get_restaurant_id(cur, restaurant_name)

        if item_name:
            cur.execute(queries.MENU_ITEM_TREND, {
                "restaurant_id": rid, "item_name": item_name,
                "start_date": start_date, "end_date": end_date,
            })
            trend = _fetchall_dicts(cur)
            return {"item_name": item_name, "weekly_trend": trend}

        cur.execute(queries.TOP_MENU_ITEMS, {
            "restaurant_id": rid, "start_date": start_date,
            "end_date": end_date, "limit": limit,
        })
        items = _fetchall_dicts(cur)
    return {"top_items": items}


def discount_analysis(
    conn: Any,
    start_date: str,
    end_date: str,
    limit: int = 25,
    restaurant_name: str = "Quality Italian",
) -> dict:
    """Analyze discounts/comps by type and approver."""
    with conn.cursor() as cur:
        rid = _get_restaurant_id(cur, restaurant_name)

        cur.execute(queries.DISCOUNT_SUMMARY, {
            "restaurant_id": rid, "start_date": start_date,
            "end_date": end_date, "limit": limit,
        })
        by_type = _fetchall_dicts(cur)

        cur.execute(queries.DISCOUNT_DAILY, {
            "restaurant_id": rid, "start_date": start_date, "end_date": end_date,
        })
        daily = _fetchall_dicts(cur)

    return {"by_type_and_approver": by_type, "daily": daily}


def time_analysis(
    conn: Any,
    start_date: str,
    end_date: str,
    group_by: str = "hour",
    restaurant_name: str = "Quality Italian",
) -> dict:
    """Analyze revenue by time dimension (hour, day_of_week, meal_period, week)."""
    query_map = {
        "hour": queries.REVENUE_BY_HOUR,
        "day_of_week": queries.REVENUE_BY_DAY_OF_WEEK,
        "meal_period": queries.REVENUE_BY_MEAL_PERIOD,
        "week": queries.REVENUE_BY_WEEK,
    }
    query = query_map.get(group_by)
    if not query:
        raise ValueError(f"Invalid group_by: {group_by}. Use: {list(query_map.keys())}")

    with conn.cursor() as cur:
        rid = _get_restaurant_id(cur, restaurant_name)
        cur.execute(query, {
            "restaurant_id": rid, "start_date": start_date, "end_date": end_date,
        })
        rows = _fetchall_dicts(cur)

    return {"group_by": group_by, "data": rows}


def customer_segmentation(
    conn: Any,
    start_date: str,
    end_date: str,
    segment_by: str = "party_size",
    restaurant_name: str = "Quality Italian",
) -> dict:
    """Segment customers by party size, meal period, or revenue center."""
    query_map = {
        "party_size": queries.BY_PARTY_SIZE,
        "meal_period": queries.REVENUE_BY_MEAL_PERIOD,
        "revenue_center": queries.BY_REVENUE_CENTER,
    }
    query = query_map.get(segment_by)
    if not query:
        raise ValueError(f"Invalid segment_by: {segment_by}. Use: {list(query_map.keys())}")

    with conn.cursor() as cur:
        rid = _get_restaurant_id(cur, restaurant_name)
        cur.execute(query, {
            "restaurant_id": rid, "start_date": start_date, "end_date": end_date,
        })
        rows = _fetchall_dicts(cur)

    return {"segment_by": segment_by, "data": rows}


def price_history(
    conn: Any,
    item_name: str,
    restaurant_name: str = "Quality Italian",
) -> dict:
    """Get price history for a menu item (supports partial/wildcard matching)."""
    pattern = f"%{item_name}%" if "%" not in item_name else item_name

    with conn.cursor() as cur:
        rid = _get_restaurant_id(cur, restaurant_name)
        cur.execute(queries.PRICE_HISTORY, {
            "restaurant_id": rid, "item_pattern": pattern,
        })
        rows = _fetchall_dicts(cur)

    return {"search": item_name, "prices": rows}


def compare_periods(
    conn: Any,
    p1_start: str, p1_end: str,
    p2_start: str, p2_end: str,
    restaurant_name: str = "Quality Italian",
) -> dict:
    """Compare two time periods side by side."""
    with conn.cursor() as cur:
        rid = _get_restaurant_id(cur, restaurant_name)
        cur.execute(queries.COMPARE_PERIODS, {
            "restaurant_id": rid,
            "p1_start": p1_start, "p1_end": p1_end,
            "p2_start": p2_start, "p2_end": p2_end,
        })
        rows = _fetchall_dicts(cur)

    return {
        "period_1": {"start": p1_start, "end": p1_end},
        "period_2": {"start": p2_start, "end": p2_end},
        "comparison": rows[0] if rows else {},
    }


def run_sql_query(
    conn: Any,
    sql: str,
    row_limit: int = 200,
    timeout_ms: int = 10000,
) -> dict:
    """Execute a read-only SELECT query with safety limits."""
    sql_stripped = sql.strip().rstrip(";")

    # Basic safety check
    first_word = sql_stripped.split()[0].upper() if sql_stripped else ""
    if first_word not in ("SELECT", "WITH", "EXPLAIN"):
        raise ValueError("Only SELECT, WITH, and EXPLAIN queries are allowed")

    with conn.cursor() as cur:
        cur.execute(f"SET LOCAL statement_timeout = '{timeout_ms}'")
        cur.execute(f"{sql_stripped} LIMIT {row_limit}")
        rows = _fetchall_dicts(cur)

    return {"row_count": len(rows), "rows": rows}


def to_json(result: dict) -> str:
    """Serialize a result dict to JSON."""
    return json.dumps(result, indent=2, default=_decimal_default)
