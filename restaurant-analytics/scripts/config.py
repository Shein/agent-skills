"""Database connection configuration for restaurant-analytics (read-only)."""

from __future__ import annotations

import os
from contextlib import contextmanager
from typing import Any, Generator


def get_database_url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable is required")
    return url


@contextmanager
def get_connection(database_url: str | None = None) -> Generator[Any, None, None]:
    """Get a read-only database connection."""
    import psycopg

    url = database_url or get_database_url()
    with psycopg.connect(url, options="-c default_transaction_read_only=on") as conn:
        yield conn
