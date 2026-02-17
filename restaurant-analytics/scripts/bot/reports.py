"""Automated report generation for daily and weekly summaries.

Generates formatted text reports suitable for messaging (WhatsApp/Telegram/Discord).
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from decimal import Decimal
from typing import Any

from . import tools


def _fmt_money(val: Any) -> str:
    if val is None:
        return "$0"
    return f"${float(val):,.0f}"


def _fmt_pct(val: Any) -> str:
    if val is None:
        return "N/A"
    return f"{float(val):.1f}%"


def _change_indicator(current: float | None, previous: float | None) -> str:
    if current is None or previous is None or previous == 0:
        return ""
    change = (current - previous) / previous * 100
    arrow = "+" if change >= 0 else ""
    return f" ({arrow}{change:.1f}%)"


def daily_report(
    conn: Any,
    report_date: date,
    restaurant_name: str = "Quality Italian",
) -> str:
    """Generate a daily summary report."""
    start = report_date.isoformat()
    end = report_date.isoformat()

    # Current day
    summary = tools.daily_summary(conn, start, end, restaurant_name)
    totals = summary["period_totals"]

    if not totals or not totals.get("check_count"):
        return f"No data for {report_date.isoformat()}"

    # Prior week same day for comparison
    prior_date = report_date - timedelta(days=7)
    prior = tools.daily_summary(conn, prior_date.isoformat(), prior_date.isoformat(), restaurant_name)
    prior_totals = prior.get("period_totals", {})

    # Top items
    items = tools.menu_item_performance(conn, start, end, limit=10, restaurant_name=restaurant_name)
    top_items = items.get("top_items", [])[:5]

    # Server highlights
    servers = tools.server_leaderboard(conn, start, end, limit=5, restaurant_name=restaurant_name)
    top_servers = servers.get("servers", [])[:3]

    day_name = report_date.strftime("%A")
    lines = [
        f"*Daily Report - {day_name}, {report_date.strftime('%b %d, %Y')}*",
        "",
        f"Revenue: {_fmt_money(totals.get('total_revenue'))}"
        f"{_change_indicator(totals.get('total_revenue'), prior_totals.get('total_revenue'))} vs last {day_name}",
        f"Checks: {totals.get('check_count', 0)}"
        f"{_change_indicator(totals.get('check_count'), prior_totals.get('check_count'))}",
        f"Guests: {totals.get('total_guests', 0)}",
        f"Avg Check: {_fmt_money(totals.get('avg_check'))}",
        f"Avg/Guest: {_fmt_money(totals.get('avg_per_guest'))}",
        f"Avg Tip: {_fmt_pct(totals.get('avg_tip_pct'))}",
        f"Avg Turnover: {totals.get('avg_turnover_min', 'N/A')} min",
        f"Discounts: {_fmt_money(totals.get('total_discounts'))}",
    ]

    if top_items:
        lines.append("")
        lines.append("*Top Items:*")
        for i, item in enumerate(top_items, 1):
            lines.append(
                f"  {i}. {item['item_name']} - {_fmt_money(item['total_revenue'])} "
                f"({int(item.get('total_qty', 0))} sold)"
            )

    if top_servers:
        lines.append("")
        lines.append("*Top Servers:*")
        for i, srv in enumerate(top_servers, 1):
            lines.append(
                f"  {i}. {srv['server_name']} - {_fmt_money(srv['gross_sales'])} "
                f"({srv['check_count']} checks, {_fmt_pct(srv.get('avg_tip_pct'))} tips)"
            )

    return "\n".join(lines)


def weekly_report(
    conn: Any,
    week_end: date | None = None,
    restaurant_name: str = "Quality Italian",
) -> str:
    """Generate a weekly summary report. week_end defaults to last Sunday."""
    if week_end is None:
        today = date.today()
        # Last Sunday
        week_end = today - timedelta(days=today.weekday() + 1)

    week_start = week_end - timedelta(days=6)

    start = week_start.isoformat()
    end = week_end.isoformat()

    # Current week
    summary = tools.daily_summary(conn, start, end, restaurant_name)
    totals = summary["period_totals"]

    if not totals or not totals.get("check_count"):
        return f"No data for week of {week_start.isoformat()}"

    # Prior week
    prior_end = week_start - timedelta(days=1)
    prior_start = prior_end - timedelta(days=6)
    prior = tools.daily_summary(conn, prior_start.isoformat(), prior_end.isoformat(), restaurant_name)
    prior_totals = prior.get("period_totals", {})

    # Day-by-day
    daily = summary.get("daily", [])

    # Servers
    servers = tools.server_leaderboard(conn, start, end, limit=10, restaurant_name=restaurant_name)
    top_servers = servers.get("servers", [])[:5]

    # Meal period breakdown
    meals = tools.time_analysis(conn, start, end, group_by="meal_period", restaurant_name=restaurant_name)
    meal_data = meals.get("data", [])

    # Top items
    items = tools.menu_item_performance(conn, start, end, limit=10, restaurant_name=restaurant_name)
    top_items = items.get("top_items", [])[:10]

    lines = [
        f"*Weekly Report - {week_start.strftime('%b %d')} to {week_end.strftime('%b %d, %Y')}*",
        "",
        f"Revenue: {_fmt_money(totals.get('total_revenue'))}"
        f"{_change_indicator(totals.get('total_revenue'), prior_totals.get('total_revenue'))} WoW",
        f"Checks: {totals.get('check_count', 0)}"
        f"{_change_indicator(totals.get('check_count'), prior_totals.get('check_count'))}",
        f"Guests: {totals.get('total_guests', 0)}",
        f"Avg Check: {_fmt_money(totals.get('avg_check'))}",
        f"Avg Tip: {_fmt_pct(totals.get('avg_tip_pct'))}",
        f"Discounts: {_fmt_money(totals.get('total_discounts'))}",
    ]

    if daily:
        lines.append("")
        lines.append("*Daily Breakdown:*")
        for day in daily:
            d = day.get("business_date")
            day_str = d.strftime("%a %m/%d") if isinstance(d, date) else str(d)
            lines.append(
                f"  {day_str}: {_fmt_money(day.get('total_revenue'))} "
                f"({day.get('check_count', 0)} checks)"
            )

    if meal_data:
        lines.append("")
        lines.append("*By Meal Period:*")
        for m in meal_data:
            lines.append(
                f"  {m.get('meal_period', 'N/A')}: {_fmt_money(m.get('gross_sales'))} "
                f"({m.get('check_count', 0)} checks, avg {_fmt_money(m.get('avg_check'))})"
            )

    if top_servers:
        lines.append("")
        lines.append("*Server Leaderboard:*")
        for i, srv in enumerate(top_servers, 1):
            lines.append(
                f"  {i}. {srv['server_name']} - {_fmt_money(srv['gross_sales'])} "
                f"({srv['check_count']} checks, {_fmt_pct(srv.get('avg_tip_pct'))} tips)"
            )

    if top_items:
        lines.append("")
        lines.append("*Top 10 Items:*")
        for i, item in enumerate(top_items, 1):
            lines.append(
                f"  {i}. {item['item_name']} - {_fmt_money(item['total_revenue'])} "
                f"({int(item.get('total_qty', 0))} sold)"
            )

    return "\n".join(lines)


if __name__ == "__main__":
    import argparse
    import os

    sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
    from config import get_connection

    parser = argparse.ArgumentParser(description="Generate restaurant reports")
    parser.add_argument("report_type", choices=["daily", "weekly"])
    parser.add_argument("--date", help="Report date (YYYY-MM-DD)")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    args = parser.parse_args()

    with get_connection(args.database_url) as conn:
        if args.report_type == "daily":
            report_date = date.fromisoformat(args.date) if args.date else date.today() - timedelta(days=1)
            print(daily_report(conn, report_date))
        else:
            week_end = date.fromisoformat(args.date) if args.date else None
            print(weekly_report(conn, week_end))
