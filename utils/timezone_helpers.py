from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

EAST_AFRICA_TZ = ZoneInfo("Africa/Nairobi")


class EATDateTime(datetime):
    """Datetime wrapper that defaults naive "now" operations to East Africa Time."""

    @classmethod
    def now(cls, tz=None):
        tz = EAST_AFRICA_TZ if tz is None else tz
        return super().now(tz)

    @classmethod
    def utcnow(cls):
        # Preserve a naive return type for compatibility while using EAT clock.
        return super().now(EAST_AFRICA_TZ).replace(tzinfo=None)

    @classmethod
    def today(cls):
        return super().now(EAST_AFRICA_TZ).replace(tzinfo=None)

    @classmethod
    def fromtimestamp(cls, timestamp, tz=None):
        tz = EAST_AFRICA_TZ if tz is None else tz
        return super().fromtimestamp(timestamp, tz=tz)


def to_east_africa(value: Any) -> datetime | None:
    """Convert the provided value to an East Africa timezone-aware datetime."""
    if not value:
        return None
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value)
        except Exception:
            return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    try:
        return value.astimezone(EAST_AFRICA_TZ)
    except Exception:
        return None


def format_east_africa(value: Any, fmt: str = "%Y-%m-%d %H:%M") -> str:
    """Return the provided datetime formatted for East Africa in a 24-hour clock."""
    dt = to_east_africa(value)
    if dt:
        return dt.strftime(fmt)
    if isinstance(value, str):
        return value
    return ""


def east_africa_now() -> datetime:
    """Get the current time in the East Africa timezone."""
    return datetime.now(EAST_AFRICA_TZ)
