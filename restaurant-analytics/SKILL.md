# Restaurant Analytics

Read-only analytics and conversational query layer for restaurant data stored in PostgreSQL, loaded by `toast-check-extractor`.

## What this skill does

- Answers natural-language questions about restaurant performance (revenue, checks, servers, menu items, discounts, trends)
- Generates daily and weekly reports
- Provides pre-built query tools for common analytics tasks
- Integrates with OpenClaw for multi-channel messaging (WhatsApp, Telegram, Discord)

## Database

This skill is a **read-only consumer** of the PostgreSQL database. All data is loaded by `toast-check-extractor`. Never writes to the database.

### Key tables
- `checks` - One row per check with derived fields (meal_period, party_size_category, tip_percentage, etc.)
- `check_items` - Line items with menu_item_id linkage
- `check_payments` - Payment details
- `check_discounts` - Discount/comp details
- `servers` - Server dimension with first/last seen dates
- `menu_items` - Menu item dimension with category classification
- `menu_item_prices` - Price change tracking
- `menu_item_daily_summary` - Daily item sales from Toast summary

### Materialized views (pre-aggregated for speed)
- `mv_daily_sales` - By date/meal_period/revenue_center
- `mv_server_performance` - By server/week
- `mv_menu_item_weekly` - By menu_item/week

## Restaurant context

- **Restaurant**: Quality Italian, New York City
- **Timezone**: America/New_York
- **Revenue centers**: Dining Room, Upstairs Bar, Downstairs Bar, Banquets
- **Meal periods**: Brunch (weekend <3pm), Lunch (weekday <3pm), Afternoon (3-5pm), Dinner (5-10pm), Late Night (10pm+)
- **Data range**: 2025-01-01 onward (~100K checks/year)

## Bot tools

| Tool | What it does |
|------|-------------|
| `daily_summary(date_range)` | Revenue, check count, avg check, vs prior period |
| `server_leaderboard(date_range)` | Rank servers by revenue, tips, checks |
| `menu_item_performance(date_range, item?)` | Item trends, top sellers, movers |
| `discount_analysis(date_range)` | Comp/discount breakdown by type and approver |
| `time_analysis(date_range, group_by)` | Revenue by hour/day/week |
| `customer_segmentation(date_range)` | By party size, meal period, revenue center |
| `price_history(item_name)` | Price changes over time with sales impact |
| `compare_periods(period1, period2)` | Period-over-period comparison |
| `run_sql_query(sql)` | SELECT-only escape hatch (10s timeout, 200 row limit) |

## Usage

```bash
# Set DATABASE_URL env var or pass --database-url
export DATABASE_URL="postgresql://user:pass@localhost:5432/restaurant_analytics"

# Test a query
python scripts/bot/tools.py daily_summary --start 2025-01-01 --end 2025-01-31

# Generate daily report
python scripts/bot/reports.py daily --date 2025-06-15
```
