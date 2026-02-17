"""OpenClaw AgentSkill definition for restaurant analytics bot.

Exposes restaurant query tools as an AgentSkill that can be connected
to WhatsApp, Telegram, Discord, or any OpenClaw-compatible channel.
"""

from __future__ import annotations

import json
import sys
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

# Add parent to path for config import
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import get_connection

from . import tools, reports


SYSTEM_PROMPT = """You are a restaurant analytics assistant for Quality Italian NYC.
You have access to a full year of POS data (2025+) from their Toast system.

CONTEXT:
- Restaurant: Quality Italian, New York City
- Revenue centers: Dining Room, Upstairs Bar, Downstairs Bar, Banquets
- Meal periods: Brunch (weekend before 3pm), Lunch (weekday before 3pm),
  Afternoon (3-5pm), Dinner (5-10pm), Late Night (after 10pm)
- Timezone: America/New_York

When answering questions:
1. Use the appropriate tool for the query type
2. Format numbers as currency ($X,XXX) and percentages (X.X%)
3. Always include the date range in your response
4. Compare to prior periods when relevant
5. Be concise but insightful - highlight notable trends or anomalies

If the user's question doesn't map to a specific tool, use run_sql_query as a fallback.
"""


TOOL_DEFINITIONS = [
    {
        "name": "daily_summary",
        "description": "Get revenue, checks, avg check, tips, and other daily metrics for a date range.",
        "parameters": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string", "description": "Start date (YYYY-MM-DD)"},
                "end_date": {"type": "string", "description": "End date (YYYY-MM-DD)"},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "server_leaderboard",
        "description": "Rank servers by revenue, tips, and check count for a date range.",
        "parameters": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string"},
                "end_date": {"type": "string"},
                "limit": {"type": "integer", "default": 20},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "menu_item_performance",
        "description": "Get menu item sales data. Omit item_name for top sellers, include it for a specific item's trend.",
        "parameters": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string"},
                "end_date": {"type": "string"},
                "item_name": {"type": "string", "description": "Specific item name (optional)"},
                "limit": {"type": "integer", "default": 25},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "discount_analysis",
        "description": "Analyze discounts and comps by type, approver, and daily trend.",
        "parameters": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string"},
                "end_date": {"type": "string"},
            },
            "required": ["start_date", "end_date"],
        },
    },
    {
        "name": "time_analysis",
        "description": "Analyze revenue by time: hour, day_of_week, meal_period, or week.",
        "parameters": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string"},
                "end_date": {"type": "string"},
                "group_by": {
                    "type": "string",
                    "enum": ["hour", "day_of_week", "meal_period", "week"],
                },
            },
            "required": ["start_date", "end_date", "group_by"],
        },
    },
    {
        "name": "customer_segmentation",
        "description": "Segment analysis by party_size, meal_period, or revenue_center.",
        "parameters": {
            "type": "object",
            "properties": {
                "start_date": {"type": "string"},
                "end_date": {"type": "string"},
                "segment_by": {
                    "type": "string",
                    "enum": ["party_size", "meal_period", "revenue_center"],
                },
            },
            "required": ["start_date", "end_date", "segment_by"],
        },
    },
    {
        "name": "price_history",
        "description": "Get price change history for a menu item. Supports partial matching.",
        "parameters": {
            "type": "object",
            "properties": {
                "item_name": {"type": "string"},
            },
            "required": ["item_name"],
        },
    },
    {
        "name": "compare_periods",
        "description": "Compare two time periods side by side (e.g., this month vs last month).",
        "parameters": {
            "type": "object",
            "properties": {
                "p1_start": {"type": "string"},
                "p1_end": {"type": "string"},
                "p2_start": {"type": "string"},
                "p2_end": {"type": "string"},
            },
            "required": ["p1_start", "p1_end", "p2_start", "p2_end"],
        },
    },
    {
        "name": "run_sql_query",
        "description": "Execute a read-only SQL SELECT query. Use as a fallback when other tools don't fit.",
        "parameters": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "SELECT SQL query"},
            },
            "required": ["sql"],
        },
    },
    {
        "name": "daily_report",
        "description": "Generate a formatted daily summary report for a specific date.",
        "parameters": {
            "type": "object",
            "properties": {
                "date": {"type": "string", "description": "Report date (YYYY-MM-DD)"},
            },
            "required": ["date"],
        },
    },
    {
        "name": "weekly_report",
        "description": "Generate a formatted weekly summary report. Defaults to last complete week.",
        "parameters": {
            "type": "object",
            "properties": {
                "week_end": {"type": "string", "description": "Last day of the week (YYYY-MM-DD, optional)"},
            },
        },
    },
]


def handle_tool_call(
    tool_name: str,
    parameters: dict,
    database_url: str | None = None,
) -> str:
    """Execute a tool call and return JSON result."""
    with get_connection(database_url) as conn:
        if tool_name == "daily_summary":
            result = tools.daily_summary(conn, **parameters)
        elif tool_name == "server_leaderboard":
            result = tools.server_leaderboard(conn, **parameters)
        elif tool_name == "menu_item_performance":
            result = tools.menu_item_performance(conn, **parameters)
        elif tool_name == "discount_analysis":
            result = tools.discount_analysis(conn, **parameters)
        elif tool_name == "time_analysis":
            result = tools.time_analysis(conn, **parameters)
        elif tool_name == "customer_segmentation":
            result = tools.customer_segmentation(conn, **parameters)
        elif tool_name == "price_history":
            result = tools.price_history(conn, **parameters)
        elif tool_name == "compare_periods":
            result = tools.compare_periods(conn, **parameters)
        elif tool_name == "run_sql_query":
            result = tools.run_sql_query(conn, **parameters)
        elif tool_name == "daily_report":
            report_date = date.fromisoformat(parameters["date"])
            text = reports.daily_report(conn, report_date)
            result = {"report": text}
        elif tool_name == "weekly_report":
            week_end = date.fromisoformat(parameters["week_end"]) if parameters.get("week_end") else None
            text = reports.weekly_report(conn, week_end)
            result = {"report": text}
        else:
            result = {"error": f"Unknown tool: {tool_name}"}

    return tools.to_json(result)


def get_skill_definition() -> dict:
    """Return the AgentSkill definition for OpenClaw registration."""
    return {
        "name": "restaurant-analytics",
        "description": "Query and analyze restaurant performance data from Quality Italian NYC. "
                       "Covers revenue, server performance, menu items, discounts, time trends, "
                       "and customer segmentation.",
        "system_prompt": SYSTEM_PROMPT,
        "tools": TOOL_DEFINITIONS,
    }
