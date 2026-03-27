"""
Shared utilities for Microsoft Fabric Energy Market Demo.
Provides timestamp generation in CET timezone relative to current time,
so demos can be re-run at any point.
"""

import os
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

CET = ZoneInfo("Europe/Warsaw")
UTC = timezone.utc


def now_cet() -> datetime:
    """Return current datetime in CET/CEST (Europe/Warsaw)."""
    return datetime.now(CET)


def now_utc() -> datetime:
    """Return current datetime in UTC."""
    return datetime.now(UTC)


def time_range_cet(hours_back: int = 24, interval_seconds: int = 10) -> list[datetime]:
    """
    Generate a list of CET timestamps from `hours_back` ago to now,
    spaced by `interval_seconds`.
    """
    end = now_cet().replace(microsecond=0)
    start = end - timedelta(hours=hours_back)
    timestamps = []
    current = start
    while current <= end:
        timestamps.append(current)
        current += timedelta(seconds=interval_seconds)
    return timestamps


def time_range_daily_cet(days_back: int = 730) -> list[datetime]:
    """
    Generate daily timestamps (midnight CET) for the last `days_back` days.
    """
    end = now_cet().replace(hour=0, minute=0, second=0, microsecond=0)
    return [end - timedelta(days=d) for d in range(days_back, -1, -1)]


def time_range_hourly_cet(days_back: int = 365) -> list[datetime]:
    """
    Generate hourly timestamps (CET) for the last `days_back` days.
    """
    end = now_cet().replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days_back)
    timestamps = []
    current = start
    while current <= end:
        timestamps.append(current)
        current += timedelta(hours=1)
    return timestamps


def format_ts(dt: datetime) -> str:
    """Format datetime as ISO 8601 string with timezone."""
    return dt.isoformat()


def format_ts_short(dt: datetime) -> str:
    """Format datetime as YYYY-MM-DD HH:MM:SS."""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def ensure_output_dir(script_path: str) -> str:
    """
    Ensure the 'data' directory exists relative to the calling script.
    Returns the absolute path to the data directory.
    """
    base_dir = os.path.dirname(os.path.abspath(script_path))
    data_dir = os.path.join(base_dir, "data")
    os.makedirs(data_dir, exist_ok=True)
    return data_dir


def add_shared_to_path():
    """Add the shared directory to Python path for imports."""
    shared_dir = os.path.dirname(os.path.abspath(__file__))
    repo_root = os.path.dirname(shared_dir)
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
