# TODO: Fix Multi-Check Order Extraction

## Problem

The Toast Order Details page shows **one block per check** (payment), not one block per order. When a table splits their bill into multiple checks, the page renders separate blocks for each check — each with its own `payment_id`, its own subset of items, and its own subtotal.

Our scraper (`scripts/toast_extract.py`) paginates through these blocks and extracts one record per block. This works correctly for single-check orders (the vast majority). But for multi-check orders, each check block only contains **that check's portion** of the order — a subset of items, a partial subtotal, and split quantities (e.g., `qty=0.5` for an item shared across checks).

The reference CSV files (Toast's "Order Details" export) report at the **order level**: one row per order with the **full combined amount** across all checks. This creates a systematic discrepancy between our database and the CSV ground truth.

## Impact

- **9,987 multi-check orders** in 2025 (≈14% of all orders)
- **1,678 of those** have a measurable subtotal mismatch vs the CSV
- **$405,521** total undercounted revenue (≈90% of all discrepancy)
- Affects **360 of 365 days** — every day except 5 exact-match days
- Typical daily error: 1–3% of revenue, but can spike to 10–30%+ on days with many large split-check parties

## Root Cause

The Order Details page (`#sales-order-details`) renders each check as a separate `.order-border` block. Each block has:
- Its own `payment_id` (extracted from `form[action*='reopencheck?id=']` or `.order-detail-meta-id`)
- Its own item table (only items assigned to that check, potentially with split quantities)
- Its own summary totals (subtotal, tax, tip, total — for that check only)
- A shared `Order #` header (`#order-summary-header` → "Order #N")

Currently, `extract_order_detail_blocks()` (line 1124) iterates over all `.order-border` nodes and creates one record per block. The `crawl_metadata()` function (line 2277) paginates through these blocks and deduplicates by `payment_id`. Each check is stored as an independent row in the `checks` table.

**The problem**: there is no logic to associate multiple checks belonging to the same order, and the `Order #` from each block is stored as-is. When the loader processes these, it upserts into `checks` keyed by `(restaurant_id, payment_id)`. For a 2-check order, the first check's data goes in, then the second check's data goes in with the same `order_number` — but the UNIQUE constraint is on `payment_id`, not `order_number`, so both rows coexist. However, the scraper's pagination (20 blocks per page) only captures a subset of checks for some orders when the second check lands on a different page or is simply missed.

In practice, for many multi-check orders the scraper only captures **one** of the checks (the first one it encounters), and that check's subtotal represents only a portion of the order total.

## What Needs To Change

### Option A: Link checks within orders (recommended)

1. **In `extract_order_detail_blocks()`** (line 1124): Already extracts `Order #` from `#order-summary-header`. This is correct — all checks from the same order share the same `Order #`.

2. **In `crawl_metadata()` / `merge_metadata()`**: After pagination completes, group all extracted records by `Order #` + `business_date`. For each order, verify that the number of extracted checks matches the expected count. The expected count can be inferred from the number of distinct `payment_id`s sharing the same `Order #`.

3. **In the loader** (`scripts/loader.py`): No schema change needed — the `checks` table already supports multiple rows per `order_number`. The loader already upserts on `(restaurant_id, payment_id)` so each check gets its own row. The issue is purely that not all checks are being extracted, not that they can't be stored.

4. **Verification**: After extraction, compare the sum of `subtotal` across all checks for a given `order_number` against the order-level total. The Order Details block header sometimes shows the order total — extract it if available.

### Option B: Detect and flag incomplete orders

If full extraction of all checks proves unreliable, add a `is_partial` or `missing_checks` flag to the `checks` table so downstream analytics can exclude or adjust for incomplete orders.

## Key Code Locations

| File | Line | Function | Role |
|------|------|----------|------|
| `scripts/toast_extract.py` | 1124 | `extract_order_detail_blocks()` | Parses each `.order-border` DOM block into a record |
| `scripts/toast_extract.py` | 2277 | `crawl_metadata()` | Paginates through Order Details, collects all blocks |
| `scripts/toast_extract.py` | 2323 | (inside `crawl_metadata`) | Deduplicates by `payment_id`, builds record list |
| `scripts/toast_extract.py` | 2897 | `map_detail_payload()` | Maps raw DOM data to structured check fields |
| `scripts/toast_extract.py` | 95–140 | `DEFAULT_SELECTORS["order_details"]` | CSS selectors for order blocks, pagination, etc. |
| `scripts/loader.py` | 1 | `load_daily_file()` | Loads JSON into DB; upserts on `(restaurant_id, payment_id)` |
| `scripts/transforms.py` | — | various | Computes derived fields from check data |

## Example Multi-Check Orders

Below are 25 real examples. Each shows the CSV (ground truth) order-level row alongside the single check the DB captured. The `diff` column shows uncaptured revenue.

### Example 1 — 2-check order, DB captured check with $0 subtotal (fully comped check)
- **Date**: 2025-01-01, **Order #35**, Table DB3
- **CSV**: checks=`35, 46`, amount=$83.50, discount=$61.00, total=$90.92
- **DB check 35**: payment_id=`200000049694436239`, subtotal=$0.00, discount=$61.00
- **Missing**: $83.50 — DB got the comped check (subtotal zeroed out); check 46 has the actual revenue

### Example 2 — 2-check order, DB captured fully comped check
- **Date**: 2025-01-01, **Order #73**, Table 202
- **CSV**: checks=`73, 180`, amount=$491.00, discount=$860.50, total=$634.58
- **DB check 73**: payment_id=`200000049700674282`, subtotal=$0.00, discount=$860.50
- **Missing**: $491.00 — all line_total values are $0 on the captured check

### Example 3 — 3-check order, DB only captured one check
- **Date**: 2025-01-01, **Order #74**, Table 101
- **CSV**: checks=`74, 123, 126`, amount=$200.50, discount=$24.00, total=$260.25
- **DB check 74**: payment_id=`200000049700735403`, subtotal=$132.00, discount=$0.00
- **Missing**: $68.50 — checks 123 and 126 not captured

### Example 4 — 2-check order, DB captured partial
- **Date**: 2025-01-01, **Order #125**, Table 121
- **CSV**: checks=`125, 170`, amount=$944.00, discount=$15.00, total=$1,256.84
- **DB check 125**: payment_id=`200000049704227444`, subtotal=$805.00
- **Missing**: $139.00 — check 170 not captured

### Example 5 — 2-check order with split quantities (qty=0.5)
- **Date**: 2025-01-01, **Order #133**, Table 101
- **CSV**: checks=`133, 186`, amount=$160.00, total=$207.34
- **DB check 133**: payment_id=`200000049704861582`, subtotal=$89.00
- **Missing**: $71.00 — items show qty=0.5 (Caesar Salad, Filet Classic, Lobster Pasta), confirming a 2-way split

### Example 6 — 3-check order
- **Date**: 2025-02-01, **Order #2**, Table 121
- **CSV**: checks=`2, 20, 31`, amount=$504.75, discount=$24.00, total=$660.99
- **DB check 2**: payment_id=`200000050655306281`, subtotal=$375.25
- **Missing**: $129.50

### Example 7 — 2-check order
- **Date**: 2025-02-01, **Order #279**, Table 137
- **CSV**: checks=`279, 290`, amount=$225.00, total=$294.96
- **DB check 279**: payment_id=`200000050694621605`, subtotal=$113.00
- **Missing**: $112.00 — nearly perfect 50/50 split

### Example 8 — 2-check order, DB captured comped check
- **Date**: 2025-02-02, **Order #136**, Table 112
- **CSV**: checks=`127, 136`, amount=$335.00, discount=$68.00, total=$484.74
- **DB check 127**: payment_id=`200000050727130864`, subtotal=$0.00, discount=$68.00
- **Missing**: $335.00 — entire order revenue missing

### Example 9 — 2-check large party order
- **Date**: 2025-03-01, **Order #78**, Table 410, 17 guests
- **CSV**: checks=`78, 104`, amount=$2,204.00, total=$2,950.65
- **DB check 78**: payment_id=`200000051674021516`, subtotal=$1,501.00
- **Missing**: $703.00

### Example 10 — 3-check order
- **Date**: 2025-03-01, **Order #106**, Table 306
- **CSV**: checks=`106, 181, 342`, amount=$917.00, discount=$24.00, total=$1,179.13
- **DB check 106**: payment_id=`200000051675992174`, subtotal=$323.00
- **Missing**: $594.00 — DB has only ~35% of order total

### Example 11 — 2-check order
- **Date**: 2025-03-01, **Order #190**, Table 309
- **CSV**: checks=`190, 268`, amount=$351.00, total=$382.16
- **DB check 190**: payment_id=`200000051685003120`, subtotal=$152.00
- **Missing**: $199.00

### Example 12 — 3-check order
- **Date**: 2025-04-01, **Order #55**, Table 123
- **CSV**: checks=`55, 150, 201`, amount=$1,596.00, discount=$12.00, total=$2,097.68
- **DB check 55**: payment_id=`200000052957360461`, subtotal=$1,166.00
- **Missing**: $430.00

### Example 13 — 2-check order, DB captured $0 check
- **Date**: 2025-04-01, **Order #173**, Table 105
- **CSV**: checks=`158, 173`, amount=$249.00, total=$326.11
- **DB check 158**: payment_id=`200000052965089364`, subtotal=$0.00
- **Missing**: $249.00 — entire order revenue missing

### Example 14 — 3-check order
- **Date**: 2025-04-01, **Order #4**, Table 137
- **CSV**: checks=`4, 7, 23`, amount=$274.00, discount=$35.00, total=$338.32
- **DB check 4**: payment_id=`200000052944077437`, subtotal=$86.00
- **Missing**: $188.00

### Example 15 — 3-check order
- **Date**: 2025-05-01, **Order #157**, Table 112
- **CSV**: checks=`157, 199, 214`, amount=$641.00, discount=$39.00, total=$822.91
- **DB check 157**: payment_id=`200000054212605955`, subtotal=$511.00
- **Missing**: $130.00

### Example 16 — 3-check order
- **Date**: 2025-05-01, **Order #111**, Table 113
- **CSV**: checks=`111, 186, 223`, amount=$504.00, discount=$27.00, total=$632.83
- **DB check 111**: payment_id=`200000054206744568`, subtotal=$381.00
- **Missing**: $123.00

### Example 17 — 2-check bar order
- **Date**: 2025-05-01, **Order #112**, Table B6 (Upstairs Bar)
- **CSV**: checks=`112, 139`, amount=$139.00, total=$180.34
- **DB check 112**: payment_id=`200000054206880453`, subtotal=$108.00
- **Missing**: $31.00

### Example 18 — 2-check order, DB captured $0 check
- **Date**: 2025-06-01, **Order #185**, Table 402, 13 guests
- **CSV**: checks=`172, 185`, amount=$1,019.00, discount=$59.00, total=$1,330.45
- **DB check 172**: payment_id=`200000055569351361`, subtotal=$0.00, discount=$59.00
- **Missing**: $1,019.00 — entire order revenue missing

### Example 19 — 3-check order
- **Date**: 2025-06-01, **Order #93**, Table 109
- **CSV**: checks=`93, 175, 199`, amount=$562.00, discount=$102.00, total=$671.90
- **DB check 93**: payment_id=`200000055562804119`, subtotal=$262.00
- **Missing**: $300.00

### Example 20 — 3-check order
- **Date**: 2025-06-01, **Order #209**, Table 121
- **CSV**: checks=`209, 245, 246`, amount=$346.00, total=$450.80
- **DB check 209**: payment_id=`200000055571633333`, subtotal=$76.00
- **Missing**: $270.00 — DB has only 22% of order total

### Example 21 — 3-check large order
- **Date**: 2025-07-01, **Order #142**, Table 302
- **CSV**: checks=`142, 209, 213`, amount=$1,447.00, discount=$61.00, total=$1,895.44
- **DB check 142**: payment_id=`200000056825211588`, subtotal=$652.00
- **Missing**: $795.00

### Example 22 — 2-check order
- **Date**: 2025-07-01, **Order #104**, Table 306, 8 guests
- **CSV**: checks=`104, 147`, amount=$369.00, total=$478.75
- **DB check 104**: payment_id=`200000056821348265`, subtotal=$166.00
- **Missing**: $203.00

### Example 23 — 2-check order
- **Date**: 2025-07-01, **Order #107**, Table 101
- **CSV**: checks=`107, 172`, amount=$313.00, total=$409.83
- **DB check 107**: payment_id=`200000056821745065`, subtotal=$191.00
- **Missing**: $122.00

### Example 24 — 2-check order
- **Date**: 2025-08-01, **Order #74**, Table 403
- **CSV**: checks=`74, 131`, amount=$719.00, discount=$29.00, total=$938.73
- **DB check 74**: payment_id=`200000058077897045`, subtotal=$224.00
- **Missing**: $495.00

### Example 25 — 3-check order
- **Date**: 2025-09-01, **Order #8**, Table O3 (Outdoor)
- **CSV**: checks=`8, 12, 27`, amount=$531.00, discount=$23.00, total=$688.13
- **DB check 8**: payment_id=`200000059318284556`, subtotal=$293.00
- **Missing**: $238.00

## Verification Queries

After implementing a fix, run these to verify:

```sql
-- Count orders with multiple checks (should increase after fix)
SELECT COUNT(DISTINCT order_number) as multi_check_orders
FROM (
    SELECT order_number, COUNT(*) as n
    FROM checks
    WHERE business_date BETWEEN '2025-01-01' AND '2025-01-31'
    GROUP BY order_number
    HAVING COUNT(*) > 1
) sub;

-- Compare daily subtotals against CSV reference
-- (use the Python comparison script from the gap analysis)

-- Check for orders where DB total < 50% of expected (from CSV cross-reference)
```

Compare the DB daily subtotals against the CSV files in `restaurant-analytics/references/OrderDetails_2025_*.csv` (deduplicate December — the Nov file covers Nov+Dec and overlaps with the Dec file). The CSV `Amount` column is the order-level subtotal. After a correct fix, single-check orders should still match exactly (99.8% do today) and multi-check order totals should converge with the CSV.
