# Quality Italian - Sales & Customer Pattern Analysis

**Database:** `postgres://localhost:5433/agm`
**Data Range:** Jan 2025 - Jan 2026 (13 months)
**Scale:** 71,714 checks | $20.6M subtotal revenue | $26.6M total with tax/tip

---

## Key Findings

### 1. Revenue Rhythm
| Month | Revenue | Avg Check | Checks |
|-------|---------|-----------|--------|
| Nov '25 | $2.01M | $318 | 6,306 |
| Dec '25 | $2.24M | $321 | 6,972 |
| Jul '25 | $1.21M | $274 | 4,426 |

Holiday season (Nov-Dec) is 30% above summer lows. **Idea: model weather/event correlation** -- do specific NYC events (Fashion Week, restaurant week, holidays) cause measurable spikes?

### 2. The "Tuesday Paradox"
Tuesday has the **highest average check ($353)** despite being mid-week, while Saturday has the **lowest ($233)**. Weekdays average $308/check vs weekends $242. Weekend guests spend ~$6 less per person.

**Idea:** Weekenders are likely tourists/casual diners who order less ambitiously. **Analyze menu mix by day-of-week** to see if specific high-margin items skew toward weekdays (business dinners ordering steaks vs. weekend brunch crowds).

### 3. Late Night is a Gold Mine for Tips
Late night tips average **29.8%** (vs 22.5% dinner, 19.6% brunch). The 11pm hour hits **43% avg tip**. These are likely bar-heavy, lower-subtotal checks where generous tipping culture kicks in.

**Idea:** Track late-night revenue per labor dollar -- are you paying enough staff to capture this high-margin window, or are you overstaffed during slower afternoon periods?

### 4. The Comp Problem: $710K in "Customer Appreciation"
33,288 checks got a "Customer Appreciation" 100% comp totaling **$710K** -- that's **3.4% of total subtotal revenue** given away. Miguel Carreto Pena alone approved $202K in comps.

**Ideas:**
- **Comp rate by server** -- are certain servers giving away more food? Is it correlated with their tip percentage?
- **Comp timing** -- do comps cluster around certain hours or days? Are they strategic (encouraging repeat visits) or reactive (fixing problems)?
- **Problems discount** is only $34K -- the comp line is 21x larger, suggesting most comps are proactive relationship-building, not error recovery

### 5. Party Economics
| Size | Rev/Min | Avg Check | Avg Minutes |
|------|---------|-----------|-------------|
| Party (10+) | **$39.08** | $2,551 | 138 min |
| Solo | $6.53 | $103 | 48 min |
| Couple | $2.75 | $172 | 75 min |

Parties generate **14x the revenue per minute** vs couples. But couples are 28,266 checks (39% of volume).

**Idea:** Revenue-per-square-foot optimization -- model how many couples you'd need to turn away to seat one banquet party, and whether the trade-off is worth it.

### 6. Banquet Seasonality
December banquets: **64 events, $568K, avg 38.5 guests** per event. August: only 29 events, $148K. Banquets are **19% of total revenue** from just 609 checks.

**Idea:** Build a banquet revenue forecast model. If you could convert even 2-3 more events per slow month (Jul-Aug-Sep), that's potentially $15-20K each.

### 7. Server Performance Spread is Enormous
| Server | Avg/Guest | Checks | Tip % |
|--------|-----------|--------|-------|
| Gardie Craggetti | $101.65 | 1,962 | 21.8% |
| Sergei Krivitski | $88.94 | 3,456 | 22.7% |
| Kara Keitt | (bottom) | 1,915 | 20.1% |

Top servers extract **$12-13 more per guest** than bottom performers. Over 3,000 covers, that's ~$36K+ in annual revenue difference per server.

**Ideas:**
- **Upsell fingerprint** -- what specific items do top-performing servers sell more of? Do they push appetizers, desserts, or premium entrees?
- **Sergei's secret**: lowest discount rate (6.6%) but high tip%. He's selling without comping.

### 8. The Caesar Salad is the Anchor
Caesar Salad appears in **6 of the top 15 item pairings** -- it's ordered with nearly everything. It's the universal starter.

**Ideas:**
- **Market basket analysis**: Build association rules beyond pairs. What's the typical 3-4 item "meal architecture"? (Starter -> Pasta -> Side -> Dessert)
- **Ricotta Honey (dessert)** pairs heavily with pastas -- **dessert attach rate by entree** could reveal which dishes leave room for dessert vs. which are too filling

### 9. Beverage Attach Rate is Surprisingly Low
Only **41% of dinner checks** include any beverage at all (by item flag). Alcohol attach is tagged at only 0.5%, which likely means the `is_alcohol` flag needs cleanup, but the beverage number is telling.

**Idea:** Every 1% increase in beverage attach rate = ~535 more beverage sales at dinner alone. If avg beverage is $15, that's $8K in incremental revenue. **Which servers have the highest beverage attach rate?**

### 10. Discount Checks Sit Longer
Discounted checks average **104.6 minutes** vs 69.8 for non-discounted, and tip **26.7%** vs 22.0%. These are likely VIP/regular tables that linger.

**Idea:** Are comped tables actually profitable when factoring in table-time cost? A comp'd table sitting 35 minutes longer is potentially one lost turn at $285 avg check.

---

## Creative Analysis Ideas Worth Building

### A. Revenue Weather Map
Heatmap of revenue by hour x day-of-week, layered with staffing data to find under/over-staffed gaps.

### B. Server Item DNA -- COMPLETED

> Full analysis in `server-item-dna.png`. Methodology and findings below.

**Methodology:** Dining Room Dinner only, 500+ checks minimum, servers with avg party < 4.5 excluded (removes large-group specialists). Shift distributions verified as matching (~50% early dinner / 40% mid / 10% late). Top 7 vs Bottom 7 by avg revenue per guest. ~10,700 checks per tier, 3.5 avg party both groups.

**Headline: Top servers extract $105.09/guest vs $94.33 — an 11.4% gap ($10.76/guest).**

#### Where the Gap Comes From (Items per Guest)

| Category | Top 7 | Bottom 7 | Delta |
|----------|-------|----------|-------|
| Starters | 0.399 | 0.365 | +9% |
| Entrees | 0.657 | 0.612 | +7% |
| **Sides** | **0.204** | **0.180** | **+13%** |
| Desserts | 0.118 | 0.116 | +2% |
| Wine BTG | 0.299 | 0.264 | +13% |
| Cocktails | 0.158 | 0.172 | -8% |
| Spirits (neat/rocks) | 0.265 | 0.250 | +6% |
| **Prem. Water** | **0.139** | **0.109** | **+28%** |
| After-Dinner | 0.135 | 0.129 | +5% |

#### Check Attach Rates (% of checks with at least one)

| Category | Top 7 | Bottom 7 | Delta |
|----------|-------|----------|-------|
| Starter | 66.4% | 64.2% | +2.2pp |
| **Side** | **46.5%** | **42.6%** | **+3.9pp** |
| Dessert | 31.5% | 30.9% | +0.6pp |
| Wine BTG | 45.6% | 43.8% | +1.8pp |
| Cocktail | 28.1% | 30.2% | -2.1pp |
| Spirit | 35.7% | 34.8% | +0.9pp |
| **Prem. Water** | **27.1%** | **23.3%** | **+3.8pp** |
| After-Dinner | 22.7% | 22.0% | +0.7pp |

#### Top Items Where Top Servers Outsell Bottom (per 1,000 guests, revenue-weighted)

| Item | Price | Top 7 | Bot 7 | Gap |
|------|-------|-------|-------|-----|
| Brussels Sprouts | $18 | 67.1 | 51.4 | +31% |
| Tuscan Fries | $17 | 63.1 | 50.3 | +25% |
| Pellegrino | $12 | 70.3 | 53.4 | +32% |
| Panna Water | $12 | 67.0 | 53.0 | +26% |
| Veal Milanese | $49 | 28.1 | 19.8 | +42% |
| Branzino | $48 | 36.2 | 28.3 | +28% |
| Filet Classic | $62 | 48.5 | 41.8 | +16% |
| GL Faust Cab | $35 | 17.2 | 10.3 | +68% |
| Yellowtail Crudo | $25 | 28.9 | 20.9 | +38% |
| Spinach | $16 | 34.7 | 23.6 | +47% |

#### Items Bottom Servers Sell More Of

| Item | Price | Top 7 | Bot 7 | Gap |
|------|-------|-------|-------|-----|
| Espresso Martini | $20 | 37.2 | 44.5 | +19% |
| Shirley Temple | $6 | 16.6 | 23.2 | +39% |
| Meatballs APP | $23 | 20.6 | 27.1 | +31% |
| Pocket Dial | $19 | 16.0 | 20.6 | +28% |
| Lemonade | $6 | 9.1 | 12.9 | +42% |
| Jr. Chicken Fingers | $21 | 3.0 | 5.6 | +87% |
| Jr. Kid's Filet | $29 | 3.3 | 5.0 | +51% |
| Pasta Vodka | $26 | 4.8 | 7.9 | +64% |

#### Actionable Recommendations

1. **Train the "Side Suggestion"**: Top servers sell 31% more Brussels Sprouts and 25% more Tuscan Fries. At ~$17, sides are the lowest-friction upsell. Script a "would you like a side for the table?" prompt into service training.

2. **Premium Water as Opening Move**: Top servers get water on 27% of checks vs 23%. Suggesting Pellegrino/Panna at $12 sets the spend-tone early and is pure margin with zero kitchen effort.

3. **Protein Ladder Coaching**: Top servers steer guests toward Veal Milanese (+42%), Branzino (+28%), and Filet (+16%) over cheaper entrees. Build a "suggest up" training module for entree recommendations.

4. **Cocktail vs. Spirit Pivot**: Bottom servers sell more Espresso Martinis and cocktails; top servers push spirits neat and wine by the glass instead -- higher price point, faster service, better margin. Train on wine/spirit pairing language.

5. **Family Table Strategy**: Bottom servers sell 87% more kids' items. When families are seated, coach servers to still push premium for the adults -- the kid's menu doesn't have to drag down the whole check.

#### Performance by Revenue Center

| Center | Tier | Checks | Avg Party | $/Guest | Gap | Tip% | Turn (min) |
|--------|------|--------|-----------|---------|-----|------|------------|
| Dining Room | TOP | 13,993 | 3.4 | $95.28 | **+$8.75 (+10.1%)** | 21.6% | 89.8 |
| Dining Room | BOT | 14,638 | 3.4 | $86.53 | | 21.4% | 86.5 |
| Outdoor Sidewalk | TOP | 283 | 2.3 | $87.63 | **+$17.02 (+24.1%)** | 19.1% | 76.4 |
| Outdoor Sidewalk | BOT | 397 | 2.3 | $70.60 | | 18.0% | 70.2 |
| Downstairs Bar | TOP | 64 | 2.6 | $76.85 | **+$17.72 (+30.0%)** | 18.9% | 70.4 |
| Downstairs Bar | BOT | 94 | 2.4 | $59.13 | | 18.7% | 58.1 |

> Banquets excluded — top servers work far more banquet shifts (308 vs 27) making comparison unreliable.

**Insight:** The gap is largest outside the dining room. Outdoor Sidewalk (+24%) and Downstairs Bar (+30%) show top servers carry their skill into lower-structure settings where there's no prix fixe or set menu to guide spend. These casual settings are the truest test of upsell ability.

#### Performance by Shift (Hour Opened)

| Shift | Tier | Checks | Avg Party | $/Guest | Gap | Tip% | Turn |
|-------|------|--------|-----------|---------|-----|------|------|
| Lunch (11a-2p) | TOP | 2,472 | 3.8 | $69.30 | **+$4.09 (+6.3%)** | 20.2% | 70.6 |
| Lunch (11a-2p) | BOT | 3,270 | 3.2 | $65.21 | | 21.4% | 68.4 |
| Early Dinner (5-6p) | TOP | 5,776 | 4.2 | $109.36 | **+$14.68 (+15.5%)** | 20.6% | 98.0 |
| Early Dinner (5-6p) | BOT | 5,440 | 3.6 | $94.67 | | 21.2% | 91.3 |
| Prime Dinner (7-8p) | TOP | 4,586 | 3.6 | $102.44 | **+$8.69 (+9.3%)** | 22.6% | 98.6 |
| Prime Dinner (7-8p) | BOT | 4,406 | 3.4 | $93.75 | | 21.6% | 97.1 |
| Late Dinner (9-10p) | TOP | 1,644 | 3.1 | $93.54 | **-$1.08 (-1.1%)** | 20.4% | 76.8 |
| Late Dinner (9-10p) | BOT | 1,728 | 2.9 | $94.62 | | 20.9% | 76.8 |

**Insights:**
- The gap is **widest at Early Dinner (+15.5%)** -- the 5-6pm seating is where top servers flex upselling power the most. Party sizes are also larger for top servers here (4.2 vs 3.6), suggesting they may get more prime table assignments.
- The gap **disappears at Late Dinner (9-10p)** -- essentially flat at -1.1%. Late diners know what they want and aren't as influenced by server suggestions. The upsell window has closed.
- **Lunch gap is narrow (+6.3%)** -- likely because lunch is faster-paced, lower-ticket, and guests have less appetite for add-ons.

#### Performance by Meal Period

| Meal | Tier | Checks | Avg Party | $/Guest | Gap | Tip% | Turn |
|------|------|--------|-----------|---------|-----|------|------|
| Dinner | TOP | 11,515 | 3.9 | $105.21 | **+$11.81 (+12.6%)** | 21.3% | 96.7 |
| Dinner | BOT | 11,010 | 3.5 | $93.40 | | 21.1% | 92.7 |
| Brunch | TOP | 579 | 3.8 | $77.87 | **+$11.05 (+16.5%)** | 18.4% | 73.7 |
| Brunch | BOT | 1,317 | 3.3 | $66.82 | | 20.9% | 71.7 |
| Lunch | TOP | 1,897 | 3.8 | $66.88 | **+$2.76 (+4.3%)** | 20.7% | 70.0 |
| Lunch | BOT | 1,955 | 3.1 | $64.12 | | 21.7% | 66.2 |
| Late Night | TOP | 531 | 2.8 | $88.76 | **-$21.35 (-19.4%)** | 24.2% | 62.7 |
| Late Night | BOT | 617 | 2.7 | $110.11 | | 24.2% | 64.3 |

**Insights:**
- **Brunch has the biggest percentage gap (+16.5%)** but top servers work far fewer brunch shifts (579 vs 1,317). When they do work brunch, they crush it -- suggesting brunch scheduling could be optimized by rotating top performers in.
- **Late Night flips: bottom servers win by $21/guest.** These servers may have more natural bar-side personalities or stronger late-night regular relationships. The "top dinner server" archetype doesn't translate to late night.
- **Dinner is the money shift** -- 12.6% gap on 11K+ checks per tier. This is where training investment yields the highest ROI.

#### Additional Recommendations from Shift/Center Analysis

6. **Optimize Brunch Scheduling**: Top servers generate +16.5% per guest at brunch but barely work it. Rotating even 2-3 top performers into weekend brunch could meaningfully lift that daypart.

7. **Outdoor Sidewalk as Training Ground**: The +24% gap at identical party sizes (2.3) makes the patio the purest measure of upsell skill. Use patio assignments for A/B testing new service scripts.

8. **Accept the Late Night Flip**: Don't force dinner-style upselling into the late-night shift. Bottom servers outperform here by $21/guest. Late night rewards a different skill set -- rapport, speed, and bar knowledge over menu navigation.

9. **Early Dinner is the Upsell Window**: The +15.5% gap at 5-6pm is the largest of any shift. This is when guests are fresh, hungry, and open to suggestions. Concentrate training and top-server scheduling here.

### C. Table Turn Optimization
Model optimal party mix to maximize revenue per seat-hour across the dining room. Trade-off analysis between large parties (high rev/min) vs. volume of smaller parties.

### D. Comp ROI Tracker
Do comped guests (matched by card_last_4) return more frequently? Is comping actually driving loyalty or just giving away margin?

### E. Menu Engineering Matrix
Plot items by popularity vs. profitability (contribution margin) to classify as:
- **Stars** -- high popularity, high margin (keep)
- **Plowhorses** -- high popularity, low margin (reprice)
- **Puzzles** -- low popularity, high margin (promote)
- **Dogs** -- low popularity, low margin (cut)

### F. "Dead Zone" Analysis
2-4pm Afternoon period has 2,119 checks with oddly high avg guest count (21.5) suggesting banquet-like activity. Is there untapped potential for a prix fixe or happy hour offering?

### G. Void Leakage
1,817 voids with no reason logged totaling $327K in cost. Who's voiding and why? Is there a pattern by server, time of day, or item?

### H. Price Elasticity Testing
The `menu_item_prices` table could reveal how volume changed when prices moved. Which items are price-sensitive vs. inelastic?

---

## Raw Data Reference

### Revenue Centers
| Center | Checks | Revenue | Avg Check | Avg Guests | Tip % |
|--------|--------|---------|-----------|------------|-------|
| Dining Room | 49,633 | $14.8M | $298 | 3.5 | 21.5% |
| Banquets | 609 | $3.9M | $6,368 | 29.6 | 1.0% |
| Upstairs Bar | 12,933 | $940K | $73 | 22.4 | 26.6% |
| Downstairs Bar | 6,544 | $709K | $108 | 15.0 | 24.8% |
| Outdoor Sidewalk | 1,975 | $314K | $159 | 2.3 | 18.7% |

### Top 15 Menu Items by Revenue
| Item | Orders | Revenue | Avg Price |
|------|--------|---------|-----------|
| Chicken Parm MC-D | 11,061 | $984K | $86 |
| Lobster Pasta MC-D | 12,542 | $794K | $53 |
| Black Truffle Cacio Pepe MC-D | 11,561 | $586K | $45 |
| Filet Classic - D | 5,552 | $410K | $63 |
| Caesar Salad- D | 14,528 | $362K | $22 |
| Chicken Parm APP- D | 3,925 | $359K | $86 |
| Fusilli alla Manzo MC - D | 8,438 | $336K | $34 |
| NY Strip Steak - D | 4,587 | $319K | $61 |
| Agnolotti Porterhouse MC-D | 6,091 | $302K | $44 |
| Paccheri Amatriciana MC-D | 6,409 | $239K | $34 |
| Bone-In Dry Aged Sirloin-D | 2,935 | $238K | $74 |
| Branzino- D | 4,180 | $225K | $48 |
| Mafaldine MC-D | 5,729 | $201K | $32 |
| Calamari Casino- D | 8,187 | $200K | $23 |
| Ricotta Honey - D | 7,839 | $174K | $21 |

### Top Item Pairings
| Item A | Item B | Co-occurrences |
|--------|--------|----------------|
| Lobster Pasta MC-D | Caesar Salad- D | 4,452 |
| Black Truffle Cacio Pepe MC-D | Caesar Salad- D | 4,259 |
| Chicken Parm MC-D | Caesar Salad- D | 4,215 |
| Black Truffle Cacio Pepe MC-D | Lobster Pasta MC-D | 3,302 |
| Calamari Casino- D | Lobster Pasta MC-D | 3,222 |

### Discount Breakdown
| Discount | Uses | Total | Avg |
|----------|------|-------|-----|
| Customer Appreciation (100%) | 33,288 | $710K | $21 |
| Open % Check (100%) | 315 | $80K | $256 |
| Employee Check (100%) | 271 | $46K | $169 |
| Problems Item (100%) | 1,014 | $34K | $33 |
| Marketing Comp | 52 | $19K | $361 |
| Investor 20 (20%) | 141 | $11K | $75 |
