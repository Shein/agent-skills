# Restaurant Analytics Platform Plan

## Context

Quality Italian NYC generates ~100K checks/year through their Toast POS system. We have a full year of extracted data (2025) as daily JSON files (~337MB total). The goal is to build an analytics platform that:
- Stores data in a structured database for fast querying
- Supports daily incremental loading + future multi-restaurant expansion
- Powers a conversational bot (via OpenClaw) so non-technical team members can query restaurant data from WhatsApp/Telegram/Discord
- Enables price optimization, server performance tracking, customer segmentation, and anomaly detection

## Database Choice: PostgreSQL

**Why PostgreSQL** (over alternatives):
- Already running locally, existing psycopg wiring in the codebase
- ~100K checks/year is modest - PostgreSQL handles this with room for 50+ restaurants
- Materialized views give us pre-computed analytics with sub-second query times
- JSONB column preserves raw data as insurance for future field extraction
- No operational overhead of ClickHouse/dedicated OLAP for this scale

**Optional future enhancement**: Export to Parquet + DuckDB for heavy ad-hoc analysis (not needed now)

---

## Phase 1: Database Schema & Setup

### New directory structure
```
toast-check-extractor/
  analytics/
    __init__.py
    config.py              -- DB connection config
    schema.py              -- All DDL, migration runner
    transforms.py          -- Date parsing, meal period classification, party size bucketing
    loader.py              -- Main ETL: JSON -> PostgreSQL (single-file loader)
    backfill.py            -- Load all historical JSON files
    daily_load.py          -- Incremental daily loader
    validate.py            -- Post-load data quality checks
    bot/
      __init__.py
      skill.py             -- OpenClaw AgentSkill definition
      tools.py             -- Query functions the bot can call
      queries.py           -- Pre-built SQL query templates
      reports.py           -- Daily/weekly report generation
```

### Schema Design

**Dimension tables** (slowly changing, auto-populated during loading):

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `dim_restaurants` | Multi-restaurant support | restaurant_id, name, city, timezone |
| `dim_revenue_centers` | Dining Room, Upstairs Bar, etc. | revenue_center_id, restaurant_id, name |
| `dim_servers` | Server lookup with tenure tracking | server_id, restaurant_id, name, first/last_seen |
| `dim_menu_items` | Menu catalog with classifications | menu_item_id, item_name, menu_group, menu, category, is_food/beverage/alcohol |
| `dim_dates` | Pre-populated date dimension | date_key, day_name, is_weekend, is_holiday |

**Fact tables** (append-only, one row per transaction):

| Table | Purpose | Key columns |
|-------|---------|-------------|
| `fact_checks` | One row per check | check_id, restaurant_id, payment_id, business_date, time_opened/closed, server_id, revenue_center_id, guest_count, subtotal/tax/tip/gratuity/discount/total, **derived**: hour_opened, meal_period, day_of_week, is_weekend, party_size_category, tip_percentage, check_avg_per_guest, has_discount, has_void, raw_data (JSONB) |
| `fact_check_items` | One row per line item | check_item_id, check_id, menu_item_id, item_name, modifiers, quantity, unit_price, discount, line_total, voided |
| `fact_check_payments` | One row per payment | check_payment_id, check_id, payment_type, amount, tip, status, card_type, card_last_4 |
| `fact_check_discounts` | One row per discount | check_discount_id, check_id, discount_name, amount, approver, reason |
| `fact_menu_item_prices` | Price tracking over time | item_name, unit_price, first_seen_date, last_seen_date |
| `fact_menu_item_daily_summary` | Daily menu performance | business_date, item_name, item_qty, net_amount |

**ETL tracking:**

| Table | Purpose |
|-------|---------|
| `etl_load_log` | Tracks which files have been loaded, check counts, status |

**Key indexes**: business_date, restaurant+date composite, server_id, revenue_center_id, meal_period, hour_opened, day_of_week, guest_count, menu_item_id

### Materialized Views (refreshed after each load)

1. **`mv_daily_sales`** - Daily totals by meal_period + revenue_center (check count, guests, revenue, avg check, avg tip %, avg turnover)
2. **`mv_server_performance`** - Weekly server rankings (checks, revenue, avg check, avg tip %)
3. **`mv_menu_item_weekly`** - Weekly item trends (qty sold, revenue, avg price, void rate)

### Key derived columns computed during load

| Column | Logic |
|--------|-------|
| `meal_period` | Brunch (weekend <3pm), Lunch (<3pm), Afternoon (3-5pm), Dinner (5-10pm), Late Night (10pm+) |
| `party_size_category` | Solo, Couple, Small Group (3-4), Large Group (5-8), Party (9+) |
| `tip_percentage` | tip / subtotal * 100 |
| `check_avg_per_guest` | subtotal / guest_count |
| `hour_opened` | Extracted from parsed time_opened |

---

## Phase 2: Data Loading Pipeline

### Transform logic (`transforms.py`)
- Parse Toast datetime strings (`"1/1/25, 11:19 AM"`) into timezone-aware datetimes (America/New_York)
- Parse currency strings (`"$3,392.00"`) from menu summaries
- Classify meal periods, party sizes
- Compute tip percentages and per-guest averages

### Loader logic (`loader.py`)
- Processes one daily JSON file at a time
- Auto-upserts dimension records (servers, revenue centers, menu items) on first encounter
- Computes all derived columns
- Tracks price changes in `fact_menu_item_prices`
- Uses `ON CONFLICT` for idempotent re-loads (safe to re-run)
- Stores full raw JSON in `raw_data` column as insurance

### Backfill (`backfill.py`)
- Walks all `output/2025-*/2025-*.json` files (365 files)
- Loads each sequentially, logging progress
- Expected runtime: ~5-15 minutes for full year

### Daily incremental (`daily_load.py`)
- Checks `etl_load_log` for last loaded date
- Scans `output/` for unloaded files
- Loads and refreshes materialized views
- Can be triggered by cron or after extraction completes

### Validation (`validate.py`)
- Check count matches source JSON
- Payment totals reconcile within tolerance
- No duplicate payment_ids
- Flag checks missing critical fields

---

## Phase 3: Conversational Bot via OpenClaw

### Why OpenClaw
- Natively supports WhatsApp, Telegram, Discord, Slack, Signal - exactly what we need for non-technical users
- Model-agnostic - can use Claude, GPT, or local models
- AgentSkills system lets us package our analytics as a reusable skill
- Can also automate cron jobs (daily extraction + loading + report generation)
- 145K+ GitHub stars, active ecosystem

### Architecture
```
WhatsApp/Telegram/Discord
         |
    OpenClaw Agent
         |
    AgentSkill: "restaurant-analytics"
         |
    Query Functions (read-only PostgreSQL)
         |
    Claude/GPT formats response with insights
```

### AgentSkill: `restaurant-analytics`
Package our analytics as an OpenClaw skill with these capabilities:

| Tool | Purpose |
|------|---------|
| `daily_summary(date_range)` | Revenue, checks, avg check, comparison to prior period |
| `server_leaderboard(date_range)` | Rank servers by revenue, tips, check count |
| `menu_item_performance(date_range, item?)` | Item trends, top sellers, biggest movers |
| `discount_analysis(date_range)` | Comp/discount breakdown by type and approver |
| `time_analysis(date_range, group_by)` | Revenue by hour/day/week patterns |
| `customer_segmentation(date_range)` | Breakdown by party size, meal period, revenue center |
| `price_history(item_name)` | Track price changes and impact on sales |
| `compare_periods(period1, period2)` | Period-over-period comparison |
| `run_sql_query(sql)` | Escape hatch for ad-hoc queries (SELECT only, read-only user, 10s timeout, 200 row limit) |

### Automated Reports
- **Daily report** (triggered after daily load): Revenue summary, top items, server highlights, anomalies, comparison to same day last week
- **Weekly report** (Monday morning): Week-over-week trends, server leaderboard, menu movers, day-of-week patterns
- Reports delivered as formatted messages to a designated channel

### Example interaction
```
User (WhatsApp): How did Saturday compare to last Saturday?
Bot: Saturday Feb 14 was strong (Valentine's effect):
     Revenue: $48,200 vs $32,100 (+50.2%)
     Checks: 312 vs 245, avg check $154 vs $131
     Top movers: Lobster Pasta +85%, Champagne by glass +120%
```

---

## Phase 4: Replace Old Schema

Remove the existing schema in `toast_skill_runner.py` (lines 214-286) and SQL export function (lines 289-433). Replace with imports from the new `analytics/` package. The extractor continues writing JSON files as before - the analytics pipeline reads from those files independently.

---

## Implementation Order

1. **Schema + config** - Create `analytics/` package, DDL in `schema.py`, run against local PostgreSQL
2. **Transforms + loader** - Build the ETL pipeline, test on a single day's file
3. **Backfill** - Load all 365 days, validate totals
4. **Materialized views** - Create views, verify query performance
5. **Bot tools** - Build query functions in `tools.py` and `queries.py`
6. **OpenClaw skill** - Package as AgentSkill, test via CLI first
7. **Connect messaging** - Wire up WhatsApp/Telegram/Discord via OpenClaw
8. **Automated reports** - Daily/weekly cron jobs via OpenClaw
9. **Remove old schema** - Clean up `toast_skill_runner.py`

## Verification

- After backfill: spot-check 5-10 random days by comparing DB totals to source JSON
- Query each materialized view and verify results make sense
- Test bot with 20+ representative questions covering all tool types
- Run a full daily cycle: extract -> load -> refresh views -> generate report
- Have a non-technical user test the WhatsApp/Telegram interface

## Key Files to Modify
- `scripts/toast_skill_runner.py` - Remove old schema (lines 214-433), replace with new analytics imports
- `scripts/run_range.py` - Add post-extraction hook to trigger daily_load.py

## Key Files to Create
- `analytics/schema.py` - All DDL
- `analytics/transforms.py` - Parsing and classification
- `analytics/loader.py` - JSON-to-PostgreSQL ETL
- `analytics/backfill.py` - Historical load
- `analytics/daily_load.py` - Incremental loader
- `analytics/validate.py` - Data quality checks
- `analytics/bot/skill.py` - OpenClaw AgentSkill
- `analytics/bot/tools.py` - Query functions
- `analytics/bot/queries.py` - SQL templates
- `analytics/bot/reports.py` - Report generation
