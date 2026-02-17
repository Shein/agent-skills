"""Pre-built SQL query templates for restaurant analytics.

All queries are parameterized and read-only. They reference the schema
created by toast-check-extractor.

IMPORTANT: All monetary columns in the database are stored as integer cents.
Queries convert to dollars (/ 100.0) in SELECT output for display.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Daily / Period Summary
# ---------------------------------------------------------------------------

DAILY_SUMMARY = """
SELECT
    c.business_date,
    COUNT(*)                                        AS check_count,
    SUM(c.guest_count)                              AS total_guests,
    ROUND(AVG(c.guest_count)::numeric, 1)           AS avg_party_size,
    ROUND(SUM(c.subtotal) / 100.0, 2)              AS gross_sales,
    ROUND(SUM(c.discount) / 100.0, 2)              AS total_discounts,
    ROUND(SUM(c.tax) / 100.0, 2)                   AS total_tax,
    ROUND(SUM(c.tip) / 100.0, 2)                   AS total_tips,
    ROUND(SUM(c.total) / 100.0, 2)                 AS total_revenue,
    ROUND(AVG(c.subtotal) / 100.0, 2)              AS avg_check,
    ROUND(AVG(c.check_avg_per_guest) / 100.0, 2)   AS avg_per_guest,
    ROUND(AVG(c.tip_percentage)::numeric, 1)        AS avg_tip_pct,
    ROUND(AVG(c.turnover_minutes)::numeric, 1)      AS avg_turnover_min
FROM checks c
WHERE c.restaurant_id = %(restaurant_id)s
  AND c.business_date BETWEEN %(start_date)s AND %(end_date)s
GROUP BY c.business_date
ORDER BY c.business_date
"""

PERIOD_SUMMARY = """
SELECT
    COUNT(*)                                        AS check_count,
    SUM(c.guest_count)                              AS total_guests,
    ROUND(AVG(c.guest_count)::numeric, 1)           AS avg_party_size,
    ROUND(SUM(c.subtotal) / 100.0, 2)              AS gross_sales,
    ROUND(SUM(c.discount) / 100.0, 2)              AS total_discounts,
    ROUND(SUM(c.tip) / 100.0, 2)                   AS total_tips,
    ROUND(SUM(c.total) / 100.0, 2)                 AS total_revenue,
    ROUND(AVG(c.subtotal) / 100.0, 2)              AS avg_check,
    ROUND(AVG(c.check_avg_per_guest) / 100.0, 2)   AS avg_per_guest,
    ROUND(AVG(c.tip_percentage)::numeric, 1)        AS avg_tip_pct,
    ROUND(AVG(c.turnover_minutes)::numeric, 1)      AS avg_turnover_min
FROM checks c
WHERE c.restaurant_id = %(restaurant_id)s
  AND c.business_date BETWEEN %(start_date)s AND %(end_date)s
"""

# ---------------------------------------------------------------------------
# Server Performance
# ---------------------------------------------------------------------------

SERVER_LEADERBOARD = """
SELECT
    c.server_name,
    COUNT(*)                                        AS check_count,
    SUM(c.guest_count)                              AS total_guests,
    ROUND(SUM(c.subtotal) / 100.0, 2)              AS gross_sales,
    ROUND(AVG(c.subtotal) / 100.0, 2)              AS avg_check,
    ROUND(SUM(c.tip) / 100.0, 2)                   AS total_tips,
    ROUND(AVG(c.tip_percentage)::numeric, 1)        AS avg_tip_pct,
    ROUND(AVG(c.turnover_minutes)::numeric, 1)      AS avg_turnover_min
FROM checks c
WHERE c.restaurant_id = %(restaurant_id)s
  AND c.business_date BETWEEN %(start_date)s AND %(end_date)s
  AND c.server_name IS NOT NULL
GROUP BY c.server_name
ORDER BY SUM(c.subtotal) DESC
LIMIT %(limit)s
"""

SERVER_DETAIL = """
SELECT
    c.business_date,
    c.meal_period,
    COUNT(*)                                        AS check_count,
    ROUND(SUM(c.subtotal) / 100.0, 2)              AS gross_sales,
    ROUND(AVG(c.subtotal) / 100.0, 2)              AS avg_check,
    ROUND(AVG(c.tip_percentage)::numeric, 1)        AS avg_tip_pct
FROM checks c
WHERE c.restaurant_id = %(restaurant_id)s
  AND c.server_name = %(server_name)s
  AND c.business_date BETWEEN %(start_date)s AND %(end_date)s
GROUP BY c.business_date, c.meal_period
ORDER BY c.business_date, c.meal_period
"""

# ---------------------------------------------------------------------------
# Menu Item Performance
# ---------------------------------------------------------------------------

TOP_MENU_ITEMS = """
SELECT
    ci.item_name,
    mi.category,
    mi.menu_group,
    SUM(ci.quantity)                                 AS total_qty,
    ROUND(SUM(ci.line_total) / 100.0, 2)            AS total_revenue,
    ROUND(AVG(ci.unit_price) / 100.0, 2)            AS avg_price,
    COUNT(DISTINCT c.check_id)                       AS check_appearances
FROM check_items ci
JOIN checks c ON c.check_id = ci.check_id
LEFT JOIN menu_items mi ON mi.menu_item_id = ci.menu_item_id
WHERE c.restaurant_id = %(restaurant_id)s
  AND c.business_date BETWEEN %(start_date)s AND %(end_date)s
  AND ci.voided = FALSE
GROUP BY ci.item_name, mi.category, mi.menu_group
ORDER BY SUM(ci.line_total) DESC
LIMIT %(limit)s
"""

MENU_ITEM_TREND = """
SELECT
    DATE_TRUNC('week', c.business_date)::date        AS week_start,
    SUM(ci.quantity)                                  AS total_qty,
    ROUND(SUM(ci.line_total) / 100.0, 2)             AS total_revenue,
    ROUND(AVG(ci.unit_price) / 100.0, 2)             AS avg_price
FROM check_items ci
JOIN checks c ON c.check_id = ci.check_id
WHERE c.restaurant_id = %(restaurant_id)s
  AND ci.item_name = %(item_name)s
  AND c.business_date BETWEEN %(start_date)s AND %(end_date)s
  AND ci.voided = FALSE
GROUP BY DATE_TRUNC('week', c.business_date)
ORDER BY week_start
"""

# ---------------------------------------------------------------------------
# Discount Analysis
# ---------------------------------------------------------------------------

DISCOUNT_SUMMARY = """
SELECT
    cd.discount_name,
    cd.approver,
    COUNT(*)                                         AS times_applied,
    ROUND(SUM(cd.amount) / 100.0, 2)                AS total_amount,
    ROUND(AVG(cd.amount) / 100.0, 2)                AS avg_amount
FROM check_discounts cd
JOIN checks c ON c.check_id = cd.check_id
WHERE c.restaurant_id = %(restaurant_id)s
  AND c.business_date BETWEEN %(start_date)s AND %(end_date)s
GROUP BY cd.discount_name, cd.approver
ORDER BY SUM(cd.amount) DESC
LIMIT %(limit)s
"""

DISCOUNT_DAILY = """
SELECT
    c.business_date,
    COUNT(DISTINCT c.check_id)                       AS checks_with_discount,
    ROUND(SUM(cd.amount) / 100.0, 2)                AS total_discounted,
    ROUND(AVG(cd.amount) / 100.0, 2)                AS avg_discount
FROM check_discounts cd
JOIN checks c ON c.check_id = cd.check_id
WHERE c.restaurant_id = %(restaurant_id)s
  AND c.business_date BETWEEN %(start_date)s AND %(end_date)s
GROUP BY c.business_date
ORDER BY c.business_date
"""

# ---------------------------------------------------------------------------
# Time Analysis
# ---------------------------------------------------------------------------

REVENUE_BY_HOUR = """
SELECT
    c.hour_opened,
    COUNT(*)                                         AS check_count,
    ROUND(SUM(c.subtotal) / 100.0, 2)               AS gross_sales,
    ROUND(AVG(c.subtotal) / 100.0, 2)               AS avg_check
FROM checks c
WHERE c.restaurant_id = %(restaurant_id)s
  AND c.business_date BETWEEN %(start_date)s AND %(end_date)s
  AND c.hour_opened IS NOT NULL
GROUP BY c.hour_opened
ORDER BY c.hour_opened
"""

REVENUE_BY_DAY_OF_WEEK = """
SELECT
    c.day_of_week,
    CASE c.day_of_week
        WHEN 0 THEN 'Monday' WHEN 1 THEN 'Tuesday' WHEN 2 THEN 'Wednesday'
        WHEN 3 THEN 'Thursday' WHEN 4 THEN 'Friday'
        WHEN 5 THEN 'Saturday' WHEN 6 THEN 'Sunday'
    END AS day_name,
    COUNT(*)                                         AS check_count,
    ROUND(SUM(c.subtotal) / 100.0, 2)               AS gross_sales,
    ROUND(AVG(c.subtotal) / 100.0, 2)               AS avg_check
FROM checks c
WHERE c.restaurant_id = %(restaurant_id)s
  AND c.business_date BETWEEN %(start_date)s AND %(end_date)s
  AND c.day_of_week IS NOT NULL
GROUP BY c.day_of_week
ORDER BY c.day_of_week
"""

REVENUE_BY_MEAL_PERIOD = """
SELECT
    c.meal_period,
    COUNT(*)                                         AS check_count,
    SUM(c.guest_count)                               AS total_guests,
    ROUND(SUM(c.subtotal) / 100.0, 2)               AS gross_sales,
    ROUND(AVG(c.subtotal) / 100.0, 2)               AS avg_check,
    ROUND(AVG(c.tip_percentage)::numeric, 1)         AS avg_tip_pct
FROM checks c
WHERE c.restaurant_id = %(restaurant_id)s
  AND c.business_date BETWEEN %(start_date)s AND %(end_date)s
  AND c.meal_period IS NOT NULL
GROUP BY c.meal_period
ORDER BY SUM(c.subtotal) DESC
"""

REVENUE_BY_WEEK = """
SELECT
    DATE_TRUNC('week', c.business_date)::date        AS week_start,
    COUNT(*)                                         AS check_count,
    ROUND(SUM(c.subtotal) / 100.0, 2)               AS gross_sales,
    ROUND(SUM(c.total) / 100.0, 2)                  AS total_revenue,
    ROUND(AVG(c.subtotal) / 100.0, 2)               AS avg_check
FROM checks c
WHERE c.restaurant_id = %(restaurant_id)s
  AND c.business_date BETWEEN %(start_date)s AND %(end_date)s
GROUP BY DATE_TRUNC('week', c.business_date)
ORDER BY week_start
"""

# ---------------------------------------------------------------------------
# Customer Segmentation
# ---------------------------------------------------------------------------

BY_PARTY_SIZE = """
SELECT
    c.party_size_category,
    COUNT(*)                                         AS check_count,
    SUM(c.guest_count)                               AS total_guests,
    ROUND(SUM(c.subtotal) / 100.0, 2)               AS gross_sales,
    ROUND(AVG(c.subtotal) / 100.0, 2)               AS avg_check,
    ROUND(AVG(c.check_avg_per_guest) / 100.0, 2)    AS avg_per_guest,
    ROUND(AVG(c.tip_percentage)::numeric, 1)         AS avg_tip_pct
FROM checks c
WHERE c.restaurant_id = %(restaurant_id)s
  AND c.business_date BETWEEN %(start_date)s AND %(end_date)s
  AND c.party_size_category IS NOT NULL
GROUP BY c.party_size_category
ORDER BY SUM(c.subtotal) DESC
"""

BY_REVENUE_CENTER = """
SELECT
    c.revenue_center,
    COUNT(*)                                         AS check_count,
    SUM(c.guest_count)                               AS total_guests,
    ROUND(SUM(c.subtotal) / 100.0, 2)               AS gross_sales,
    ROUND(AVG(c.subtotal) / 100.0, 2)               AS avg_check,
    ROUND(AVG(c.tip_percentage)::numeric, 1)         AS avg_tip_pct
FROM checks c
WHERE c.restaurant_id = %(restaurant_id)s
  AND c.business_date BETWEEN %(start_date)s AND %(end_date)s
  AND c.revenue_center IS NOT NULL
GROUP BY c.revenue_center
ORDER BY SUM(c.subtotal) DESC
"""

# ---------------------------------------------------------------------------
# Price History
# ---------------------------------------------------------------------------

PRICE_HISTORY = """
SELECT
    mp.item_name,
    ROUND(mp.unit_price / 100.0, 2)                 AS unit_price,
    mp.first_seen_date,
    mp.last_seen_date,
    mp.observation_count
FROM menu_item_prices mp
WHERE mp.restaurant_id = %(restaurant_id)s
  AND mp.item_name ILIKE %(item_pattern)s
ORDER BY mp.item_name, mp.first_seen_date
"""

# ---------------------------------------------------------------------------
# Period Comparison
# ---------------------------------------------------------------------------

COMPARE_PERIODS = """
WITH p1 AS (
    SELECT
        COUNT(*) AS check_count, SUM(guest_count) AS guests,
        ROUND(SUM(subtotal) / 100.0, 2) AS gross_sales,
        ROUND(SUM(tip) / 100.0, 2) AS tips,
        ROUND(AVG(subtotal) / 100.0, 2) AS avg_check,
        ROUND(AVG(tip_percentage)::numeric, 1) AS avg_tip_pct
    FROM checks
    WHERE restaurant_id = %(restaurant_id)s
      AND business_date BETWEEN %(p1_start)s AND %(p1_end)s
),
p2 AS (
    SELECT
        COUNT(*) AS check_count, SUM(guest_count) AS guests,
        ROUND(SUM(subtotal) / 100.0, 2) AS gross_sales,
        ROUND(SUM(tip) / 100.0, 2) AS tips,
        ROUND(AVG(subtotal) / 100.0, 2) AS avg_check,
        ROUND(AVG(tip_percentage)::numeric, 1) AS avg_tip_pct
    FROM checks
    WHERE restaurant_id = %(restaurant_id)s
      AND business_date BETWEEN %(p2_start)s AND %(p2_end)s
)
SELECT
    p1.check_count AS p1_checks, p2.check_count AS p2_checks,
    p1.guests AS p1_guests, p2.guests AS p2_guests,
    p1.gross_sales AS p1_sales, p2.gross_sales AS p2_sales,
    p1.tips AS p1_tips, p2.tips AS p2_tips,
    p1.avg_check AS p1_avg_check, p2.avg_check AS p2_avg_check,
    p1.avg_tip_pct AS p1_avg_tip_pct, p2.avg_tip_pct AS p2_avg_tip_pct,
    CASE WHEN p1.gross_sales > 0
         THEN ROUND(((p2.gross_sales - p1.gross_sales) / p1.gross_sales * 100)::numeric, 1)
         ELSE NULL END AS sales_change_pct
FROM p1, p2
"""
