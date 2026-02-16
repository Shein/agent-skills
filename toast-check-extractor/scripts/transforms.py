"""Data transformation functions for Toast check data.

Handles date parsing, meal period classification, party size bucketing,
and currency parsing. All functions are pure (no side effects or DB access).
"""

from __future__ import annotations

import re
from datetime import datetime
from zoneinfo import ZoneInfo

NYC_TZ = ZoneInfo("America/New_York")

# Toast datetime format: "M/D/YY, H:MM AM/PM" (e.g. "1/1/25, 11:19 AM")
TOAST_DT_FORMAT = "%m/%d/%y, %I:%M %p"


def parse_toast_datetime(raw: str | None) -> datetime | None:
    """Parse a Toast datetime string into a timezone-aware datetime (America/New_York)."""
    if not raw or not isinstance(raw, str):
        return None
    raw = raw.strip()
    if not raw:
        return None
    try:
        naive = datetime.strptime(raw, TOAST_DT_FORMAT)
        return naive.replace(tzinfo=NYC_TZ)
    except ValueError:
        return None


def classify_meal_period(hour: int | None, day_of_week: int | None) -> str | None:
    """Classify a check into a meal period based on hour opened and day of week.

    Args:
        hour: 0-23 hour of day
        day_of_week: 0=Monday, 6=Sunday (ISO weekday - 1)

    Returns:
        One of: Brunch, Lunch, Afternoon, Dinner, Late Night, or None
    """
    if hour is None:
        return None
    is_weekend = day_of_week is not None and day_of_week >= 5  # Sat=5, Sun=6
    if hour < 15:  # Before 3pm
        return "Brunch" if is_weekend else "Lunch"
    if hour < 17:  # 3pm - 5pm
        return "Afternoon"
    if hour < 22:  # 5pm - 10pm
        return "Dinner"
    return "Late Night"  # 10pm+


def classify_party_size(guest_count: int | None) -> str | None:
    """Classify guest count into a party size category."""
    if guest_count is None or guest_count <= 0:
        return None
    if guest_count == 1:
        return "Solo"
    if guest_count == 2:
        return "Couple"
    if guest_count <= 4:
        return "Small Group"
    if guest_count <= 8:
        return "Large Group"
    return "Party"


def parse_currency(raw: str | None) -> int | None:
    """Parse a currency string like '$3,392.00' or '-$50.00' into integer cents.

    Returns 339200 for '$3,392.00', -5000 for '-$50.00'.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return round(float(raw) * 100)
    raw = str(raw).strip()
    if not raw:
        return None
    # Remove $ and commas, handle negative
    cleaned = re.sub(r"[$,]", "", raw)
    try:
        return round(float(cleaned) * 100)
    except ValueError:
        return None


def classify_menu_item(item_name: str, menu_group: str | None, menu: str | None) -> dict:
    """Derive category, is_food, is_beverage, is_alcohol from menu item metadata.

    Returns dict with keys: category, is_food, is_beverage, is_alcohol
    """
    name_lower = (item_name or "").lower()
    group_lower = (menu_group or "").lower()
    menu_lower = (menu or "").lower()

    is_alcohol = False
    is_beverage = False
    is_food = False
    category = "Other"

    # Determine from menu field
    if menu_lower in ("liquor/beer/na bev", "liquor", "beer", "wine"):
        is_beverage = True
        is_alcohol = menu_lower != "na bev"
        if "wine" in menu_lower:
            category = "Wine"
            is_alcohol = True
        elif "beer" in menu_lower:
            category = "Beer"
            is_alcohol = True
        elif "liquor" in menu_lower or "cocktail" in group_lower:
            category = "Cocktail"
            is_alcohol = True
        else:
            category = "Beverage"
    elif "wine" in menu_lower:
        is_beverage = True
        is_alcohol = True
        category = "Wine"

    # Determine from menu_group
    if "appetizer" in group_lower:
        category = "Appetizer"
        is_food = True
    elif "pasta" in group_lower:
        category = "Pasta"
        is_food = True
    elif "entree" in group_lower or "entreé" in group_lower:
        category = "Entree"
        is_food = True
    elif "dessert" in group_lower:
        category = "Dessert"
        is_food = True
    elif "side" in group_lower:
        category = "Side"
        is_food = True
    elif "salad" in group_lower:
        category = "Salad"
        is_food = True
    elif "soup" in group_lower:
        category = "Soup"
        is_food = True
    elif "bread" in group_lower:
        category = "Bread"
        is_food = True
    elif "steak" in group_lower:
        category = "Entree"
        is_food = True
    elif "seafood" in group_lower or "fish" in group_lower:
        category = "Entree"
        is_food = True
    elif "sandwich" in group_lower or "burger" in group_lower:
        category = "Entree"
        is_food = True
    elif "brunch" in group_lower:
        category = "Brunch"
        is_food = True

    # Beverage detection from group/name when menu didn't catch it
    if not is_food and not is_beverage:
        if any(kw in group_lower for kw in ("beverage", "coffee", "tea", "juice", "soda", "water")):
            is_beverage = True
            category = "Beverage"
        elif any(kw in name_lower for kw in ("coffee", "espresso", "tea", "juice", "soda", "water", "lemonade")):
            is_beverage = True
            category = "Beverage"
        elif any(kw in group_lower for kw in ("cocktail", "martini", "spirit", "liquor", "beer", "wine")):
            is_beverage = True
            is_alcohol = True
            category = "Cocktail"

    # If nothing matched, assume food
    if not is_food and not is_beverage:
        is_food = True

    return {
        "category": category,
        "is_food": is_food,
        "is_beverage": is_beverage,
        "is_alcohol": is_alcohol,
    }


def dollars_to_cents(value: float | int | None) -> int | None:
    """Convert a dollar amount to integer cents. $52.81 -> 5281.

    Uses round() to avoid floating-point drift (e.g. 52.81 * 100 = 5280.999...).
    Returns None if input is None.
    """
    if value is None:
        return None
    try:
        return round(float(value) * 100)
    except (TypeError, ValueError):
        return None


def dollars_to_cents_or_zero(value: float | int | None) -> int:
    """Like dollars_to_cents but returns 0 instead of None."""
    result = dollars_to_cents(value)
    return result if result is not None else 0


def safe_numeric(value: float | int | None, default: float = 0.0) -> float:
    """Safely convert a value to float, returning default if None or invalid."""
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def safe_int(value: int | float | str | None, default: int | None = None) -> int | None:
    """Safely convert a value to int, returning default if None or invalid."""
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
