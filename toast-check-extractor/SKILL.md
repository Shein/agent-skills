---
name: toast-check-extractor
description: Extract Toast POS check-level data from Toast Admin Sales reports with login automation, natural-language date resolution, resumable scraping, throttling-aware retries, and JSON or SQL export targets. Use when a user asks for Toast checks/orders/payments export for a date range.
---

# Toast Check Extraction
Use `scripts/toast_skill_runner.py` as the public entrypoint. It resolves date phrases, validates output mode/destination, runs scraping, and exports.

The scraper implementation is `scripts/toast_extract.py`.

## Required Inputs

- Dates:
  - Either concrete dates: `--start-date YYYY-MM-DD --end-date YYYY-MM-DD`
  - Or natural language: `--date-query "last week"` / `--date-query "last 7 days"`
- Output mode: `--format json` or `--format sql`
- Destination:
  - JSON mode: `--output-path path/to/file.json` (if omitted, defaults to `output/toast/checks.json`)
  - SQL mode: `--database-url postgresql://...` (must be present; otherwise abort)
- Credentials in `.env` (default `../.env`):
  - `TOAST_USERNAME=...`
  - `TOAST_PASSWORD=...`
  - Fallback keys: `USER` and `PASS`

If format/destination args are missing and the run is interactive, prompt for them.

## Install

```bash
pip install -r scripts/requirements.txt
python -m playwright install chromium
```

## Foreground Run (JSON)

```bash
python scripts/toast_skill_runner.py run \
  --date-query "last week" \
  --format json \
  --output-path output/toast/checks_last_week.json \
  --state-file output/toast_checks_state.json \
  --browser-channel chrome \
  --env-file ../.env \
  --workers 6
```

## Foreground Run (SQL)

```bash
python scripts/toast_skill_runner.py run \
  --start-date 2026-02-01 \
  --end-date 2026-02-07 \
  --format sql \
  --database-url postgresql://localhost:5432/toast \
  --state-file output/toast_checks_state.json \
  --env-file ../.env
```

## Background Run + Progress

Start in tmux:

```bash
python scripts/toast_skill_runner.py start-bg \
  --date-query "last 7 days" \
  --format json \
  --output-path output/toast/checks_last_7_days.json \
  --session-name toast-last-7
```

Check progress:

```bash
python scripts/toast_skill_runner.py status \
  --state-file output/toast_checks_state.json \
  --menu-summary-file output/toast_menu_item_summary.json \
  --progress-file output/toast_progress.json \
  --error-log-file output/toast_errors.jsonl \
  --session-name toast-last-7
```

## Workflow and Guarantees

- Keep Toast authentication persistent using `.toast_browser_profile/`.
- Crawl directly from `ORDER_DETAILS_URL` pages (including pagination) and persist parsed checks incrementally.
- Also crawl `Menu Item Summary` with pagination into `--menu-summary-file`.
- Resume safely by re-running with the same `--state-file`.
- Throttling/challenge handling:
  - Cloudflare challenge detection and wait
  - auth-block restart loop with cooldown
  - adaptive detail backoff (`output/toast_errors.jsonl` records failures)
- Progress ledger is written to `--progress-file`.
- Detail parsing includes validation checks:
  - `total ~= subtotal + tax + tip + gratuity - discount`
  - `line_total ~= quantity * unit_price`

## Notes for Agents

- Prefer config/selector updates before changing parser logic.
- If selector overrides are needed, add `references/toast_selectors.json` and pass it with `--config`.
- Use `scripts/toast_extract.py --skip-metadata` for retrying incomplete detail rows only.
