"""Database schema DDL for the restaurant analytics platform.

Creates all dimension tables, fact tables, indexes, and materialized views.
This is the single source of truth for the database schema.
"""

from __future__ import annotations

from typing import Any

# ---------------------------------------------------------------------------
# Table DDL
# ---------------------------------------------------------------------------

DIMENSION_TABLES = """
-- Dimension: restaurants
CREATE TABLE IF NOT EXISTS restaurants (
    restaurant_id   SERIAL PRIMARY KEY,
    name            TEXT UNIQUE NOT NULL,
    city            TEXT,
    state           TEXT,
    timezone        TEXT NOT NULL DEFAULT 'America/New_York',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Dimension: revenue_centers
CREATE TABLE IF NOT EXISTS revenue_centers (
    revenue_center_id SERIAL PRIMARY KEY,
    restaurant_id     INTEGER NOT NULL REFERENCES restaurants(restaurant_id),
    name              TEXT NOT NULL,
    UNIQUE (restaurant_id, name)
);

-- Dimension: servers
CREATE TABLE IF NOT EXISTS servers (
    server_id       SERIAL PRIMARY KEY,
    restaurant_id   INTEGER NOT NULL REFERENCES restaurants(restaurant_id),
    name            TEXT NOT NULL,
    first_seen_at   DATE,
    last_seen_at    DATE,
    UNIQUE (restaurant_id, name)
);

-- Dimension: menu_items
CREATE TABLE IF NOT EXISTS menu_items (
    menu_item_id    SERIAL PRIMARY KEY,
    restaurant_id   INTEGER NOT NULL REFERENCES restaurants(restaurant_id),
    item_name       TEXT NOT NULL,
    menu_group      TEXT,
    menu            TEXT,
    category        TEXT,
    is_food         BOOLEAN DEFAULT FALSE,
    is_beverage     BOOLEAN DEFAULT FALSE,
    is_alcohol      BOOLEAN DEFAULT FALSE,
    first_seen_at   DATE,
    last_seen_at    DATE,
    UNIQUE (restaurant_id, item_name)
);
"""

FACT_TABLES = """
-- Fact: checks (one row per check)
-- All monetary columns stored as integer cents ($52.81 = 5281)
CREATE TABLE IF NOT EXISTS checks (
    check_id                BIGSERIAL PRIMARY KEY,
    restaurant_id           INTEGER NOT NULL REFERENCES restaurants(restaurant_id),
    payment_id              TEXT NOT NULL,
    check_number            INTEGER,
    business_date           DATE NOT NULL,
    time_opened             TIMESTAMPTZ,
    time_closed             TIMESTAMPTZ,
    turnover_minutes        NUMERIC(8,1),
    server_id               INTEGER REFERENCES servers(server_id),
    revenue_center_id       INTEGER REFERENCES revenue_centers(revenue_center_id),
    server_name             TEXT,
    revenue_center          TEXT,
    table_name              TEXT,
    tab_name                TEXT,
    guest_count             INTEGER,
    subtotal                BIGINT,
    discount                BIGINT,
    tax                     BIGINT,
    tip                     BIGINT,
    gratuity                BIGINT,
    total                   BIGINT,
    hour_opened             SMALLINT,
    meal_period             TEXT,
    day_of_week             SMALLINT,
    is_weekend              BOOLEAN,
    party_size_category     TEXT,
    tip_percentage          NUMERIC(8,2),
    check_avg_per_guest     BIGINT,
    has_discount            BOOLEAN DEFAULT FALSE,
    has_void                BOOLEAN DEFAULT FALSE,
    source                  TEXT,
    order_number            INTEGER,
    extracted_at            TIMESTAMPTZ,
    raw_data                JSONB,
    loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (restaurant_id, payment_id)
);

-- Fact: check_items (one row per line item)
-- Monetary columns in cents; quantity stays NUMERIC
CREATE TABLE IF NOT EXISTS check_items (
    check_item_id           BIGSERIAL PRIMARY KEY,
    check_id                BIGINT NOT NULL REFERENCES checks(check_id) ON DELETE CASCADE,
    restaurant_id           INTEGER NOT NULL REFERENCES restaurants(restaurant_id),
    menu_item_id            INTEGER REFERENCES menu_items(menu_item_id),
    item_index              INTEGER NOT NULL,
    item_name               TEXT,
    modifiers               TEXT,
    quantity                NUMERIC(8,2),
    unit_price              BIGINT,
    discount                BIGINT,
    line_total              BIGINT,
    line_tax                BIGINT,
    line_total_with_tax     BIGINT,
    voided                  BOOLEAN DEFAULT FALSE,
    void_reason             TEXT,
    UNIQUE (check_id, item_index)
);

-- Fact: check_payments (one row per payment, monetary in cents)
CREATE TABLE IF NOT EXISTS check_payments (
    check_payment_id        BIGSERIAL PRIMARY KEY,
    check_id                BIGINT NOT NULL REFERENCES checks(check_id) ON DELETE CASCADE,
    restaurant_id           INTEGER NOT NULL REFERENCES restaurants(restaurant_id),
    payment_index           INTEGER NOT NULL,
    payment_type            TEXT,
    payment_date            TIMESTAMPTZ,
    amount                  BIGINT,
    tip                     BIGINT,
    gratuity                BIGINT,
    total                   BIGINT,
    refund                  BIGINT,
    status                  TEXT,
    card_type               TEXT,
    card_last_4             TEXT,
    UNIQUE (check_id, payment_index)
);

-- Fact: check_discounts (one row per discount)
CREATE TABLE IF NOT EXISTS check_discounts (
    check_discount_id       BIGSERIAL PRIMARY KEY,
    check_id                BIGINT NOT NULL REFERENCES checks(check_id) ON DELETE CASCADE,
    restaurant_id           INTEGER NOT NULL REFERENCES restaurants(restaurant_id),
    discount_index          INTEGER NOT NULL,
    discount_name           TEXT,
    amount                  BIGINT,
    applied_date            TIMESTAMPTZ,
    approver                TEXT,
    reason                  TEXT,
    comment                 TEXT,
    UNIQUE (check_id, discount_index)
);

-- Fact: menu_item_prices (price tracking over time)
CREATE TABLE IF NOT EXISTS menu_item_prices (
    price_id                BIGSERIAL PRIMARY KEY,
    restaurant_id           INTEGER NOT NULL REFERENCES restaurants(restaurant_id),
    menu_item_id            INTEGER REFERENCES menu_items(menu_item_id),
    item_name               TEXT NOT NULL,
    unit_price              BIGINT NOT NULL,
    first_seen_date         DATE,
    last_seen_date          DATE,
    observation_count       INTEGER DEFAULT 1,
    UNIQUE (restaurant_id, item_name, unit_price)
);

-- Fact: menu_item_daily_summary (from Toast's daily summary)
CREATE TABLE IF NOT EXISTS menu_item_daily_summary (
    summary_id              BIGSERIAL PRIMARY KEY,
    restaurant_id           INTEGER NOT NULL REFERENCES restaurants(restaurant_id),
    business_date           DATE NOT NULL,
    menu_item_id            INTEGER REFERENCES menu_items(menu_item_id),
    item_name               TEXT NOT NULL,
    menu_group              TEXT,
    menu                    TEXT,
    item_qty                INTEGER,
    net_amount              BIGINT,
    UNIQUE (restaurant_id, business_date, item_name)
);

-- ETL tracking
CREATE TABLE IF NOT EXISTS etl_load_log (
    load_id                 BIGSERIAL PRIMARY KEY,
    restaurant_id           INTEGER REFERENCES restaurants(restaurant_id),
    business_date           DATE NOT NULL,
    source_file             TEXT NOT NULL,
    checks_loaded           INTEGER,
    items_loaded            INTEGER,
    started_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at            TIMESTAMPTZ,
    status                  TEXT NOT NULL DEFAULT 'running',
    error_message           TEXT,
    UNIQUE (restaurant_id, business_date, source_file)
);
"""

INDEXES = """
-- checks indexes
CREATE INDEX IF NOT EXISTS idx_checks_business_date
    ON checks (business_date);
CREATE INDEX IF NOT EXISTS idx_checks_restaurant_date
    ON checks (restaurant_id, business_date);
CREATE INDEX IF NOT EXISTS idx_checks_server_id
    ON checks (server_id);
CREATE INDEX IF NOT EXISTS idx_checks_revenue_center_id
    ON checks (revenue_center_id);
CREATE INDEX IF NOT EXISTS idx_checks_meal_period
    ON checks (meal_period);
CREATE INDEX IF NOT EXISTS idx_checks_hour_opened
    ON checks (hour_opened);
CREATE INDEX IF NOT EXISTS idx_checks_day_of_week
    ON checks (day_of_week);
CREATE INDEX IF NOT EXISTS idx_checks_has_discount
    ON checks (has_discount) WHERE has_discount = TRUE;

-- check_items indexes
CREATE INDEX IF NOT EXISTS idx_check_items_check_id
    ON check_items (check_id);
CREATE INDEX IF NOT EXISTS idx_check_items_menu_item_id
    ON check_items (menu_item_id);
CREATE INDEX IF NOT EXISTS idx_check_items_voided
    ON check_items (voided) WHERE voided = TRUE;

-- check_payments indexes
CREATE INDEX IF NOT EXISTS idx_check_payments_check_id
    ON check_payments (check_id);
CREATE INDEX IF NOT EXISTS idx_check_payments_status
    ON check_payments (status);

-- check_discounts indexes
CREATE INDEX IF NOT EXISTS idx_check_discounts_check_id
    ON check_discounts (check_id);
"""

MATERIALIZED_VIEWS = """
-- Daily sales summary (all monetary values in cents)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_sales AS
SELECT
    c.restaurant_id,
    c.business_date,
    c.meal_period,
    c.revenue_center,
    COUNT(*)                                    AS check_count,
    SUM(c.guest_count)                          AS total_guests,
    ROUND(AVG(c.guest_count)::numeric, 1)       AS avg_party_size,
    SUM(c.subtotal)                             AS gross_sales_cents,
    SUM(c.discount)                             AS total_discounts_cents,
    SUM(c.tip)                                  AS total_tips_cents,
    SUM(c.total)                                AS total_revenue_cents,
    ROUND(AVG(c.subtotal)::numeric, 0)          AS avg_check_cents,
    ROUND(AVG(c.check_avg_per_guest)::numeric, 0) AS avg_per_guest_cents,
    ROUND(AVG(c.tip_percentage)::numeric, 1)    AS avg_tip_pct,
    ROUND(AVG(c.turnover_minutes)::numeric, 1)  AS avg_turnover_min
FROM checks c
GROUP BY c.restaurant_id, c.business_date, c.meal_period, c.revenue_center;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_sales_pk
    ON mv_daily_sales (restaurant_id, business_date, meal_period, revenue_center);

-- Server performance (weekly, monetary in cents)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_server_performance AS
SELECT
    c.restaurant_id,
    c.server_id,
    c.server_name,
    DATE_TRUNC('week', c.business_date)::date   AS week_start,
    COUNT(*)                                     AS check_count,
    SUM(c.subtotal)                              AS gross_sales_cents,
    ROUND(AVG(c.subtotal)::numeric, 0)           AS avg_check_cents,
    ROUND(AVG(c.tip_percentage)::numeric, 1)     AS avg_tip_pct,
    SUM(c.tip)                                   AS total_tips_cents
FROM checks c
WHERE c.server_id IS NOT NULL
GROUP BY c.restaurant_id, c.server_id, c.server_name, DATE_TRUNC('week', c.business_date);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_server_perf_pk
    ON mv_server_performance (restaurant_id, server_id, week_start);

-- Menu item weekly summary (monetary in cents)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_menu_item_weekly AS
SELECT
    ci.restaurant_id,
    ci.menu_item_id,
    ci.item_name,
    DATE_TRUNC('week', c.business_date)::date    AS week_start,
    SUM(ci.quantity)                              AS total_qty,
    SUM(ci.line_total)                            AS total_revenue_cents,
    ROUND(AVG(ci.unit_price)::numeric, 0)         AS avg_unit_price_cents,
    SUM(CASE WHEN ci.voided THEN ci.quantity ELSE 0 END) AS voided_qty
FROM check_items ci
JOIN checks c ON c.check_id = ci.check_id
GROUP BY ci.restaurant_id, ci.menu_item_id, ci.item_name, DATE_TRUNC('week', c.business_date);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_menu_item_weekly_pk
    ON mv_menu_item_weekly (restaurant_id, menu_item_id, week_start);
"""


def create_schema(conn: Any) -> None:
    """Create all tables, indexes, and materialized views."""
    with conn.cursor() as cur:
        cur.execute(DIMENSION_TABLES)
        cur.execute(FACT_TABLES)
        cur.execute(INDEXES)
    conn.commit()
    # Materialized views created separately (they may fail on empty tables)
    with conn.cursor() as cur:
        cur.execute(MATERIALIZED_VIEWS)
    conn.commit()


def refresh_materialized_views(conn: Any) -> None:
    """Refresh all materialized views concurrently if possible."""
    views = ["mv_daily_sales", "mv_server_performance", "mv_menu_item_weekly"]
    for view in views:
        try:
            with conn.cursor() as cur:
                cur.execute(f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view}")
            conn.commit()
        except Exception:
            conn.rollback()
            try:
                with conn.cursor() as cur:
                    cur.execute(f"REFRESH MATERIALIZED VIEW {view}")
                conn.commit()
            except Exception:
                conn.rollback()


def drop_all(conn: Any) -> None:
    """Drop all tables and views (for development/testing only)."""
    with conn.cursor() as cur:
        cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_menu_item_weekly CASCADE")
        cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_server_performance CASCADE")
        cur.execute("DROP MATERIALIZED VIEW IF EXISTS mv_daily_sales CASCADE")
        cur.execute("DROP TABLE IF EXISTS etl_load_log CASCADE")
        cur.execute("DROP TABLE IF EXISTS menu_item_daily_summary CASCADE")
        cur.execute("DROP TABLE IF EXISTS menu_item_prices CASCADE")
        cur.execute("DROP TABLE IF EXISTS check_discounts CASCADE")
        cur.execute("DROP TABLE IF EXISTS check_payments CASCADE")
        cur.execute("DROP TABLE IF EXISTS check_items CASCADE")
        cur.execute("DROP TABLE IF EXISTS checks CASCADE")
        cur.execute("DROP TABLE IF EXISTS menu_items CASCADE")
        cur.execute("DROP TABLE IF EXISTS servers CASCADE")
        cur.execute("DROP TABLE IF EXISTS revenue_centers CASCADE")
        cur.execute("DROP TABLE IF EXISTS restaurants CASCADE")
    conn.commit()


if __name__ == "__main__":
    import argparse
    import os

    parser = argparse.ArgumentParser(description="Manage restaurant analytics schema")
    parser.add_argument("action", choices=["create", "drop", "recreate", "refresh"])
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"))
    args = parser.parse_args()

    if not args.database_url:
        print("DATABASE_URL is required (--database-url or env var)")
        raise SystemExit(1)

    import psycopg

    with psycopg.connect(args.database_url) as conn:
        if args.action == "create":
            create_schema(conn)
            print("Schema created.")
        elif args.action == "drop":
            drop_all(conn)
            print("Schema dropped.")
        elif args.action == "recreate":
            drop_all(conn)
            create_schema(conn)
            print("Schema recreated.")
        elif args.action == "refresh":
            refresh_materialized_views(conn)
            print("Materialized views refreshed.")
