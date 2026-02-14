# Agent Skills

A collection of reusable automation skills designed to be invoked by AI coding agents (e.g. Claude Code).

## Skills

### toast-check-extractor

Automates extraction of check-level sales data from the Toast POS admin portal.

**Capabilities:**
- Scrapes payment metadata, order details, and menu item summaries from Toast Admin
- Handles Cloudflare challenges, authentication, pagination, and rate limiting
- Supports natural language date ranges (e.g. "last week", "yesterday")
- Exports to JSON files or directly to PostgreSQL
- Resumable — maintains state files for crash recovery
- Background execution via tmux

**Tech stack:** Python 3 · Playwright (Chromium) · asyncio · psycopg 3

**Quick start:**
```bash
cd toast-check-extractor/scripts
pip install -r requirements.txt
playwright install chromium
python toast_skill_runner.py --help
```

See [`toast-check-extractor/SKILL.md`](toast-check-extractor/SKILL.md) for full usage documentation.

## Structure

```
<skill-name>/
├── SKILL.md              # Skill contract & usage docs
├── scripts/              # Executable code
│   ├── requirements.txt  # Dependencies
│   └── ...
└── references/           # Sample data, test fixtures
```

## License

Private — all rights reserved.
