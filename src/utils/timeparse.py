from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import List, Optional, Tuple
from zoneinfo import ZoneInfo

STARBASE_TZ = ZoneInfo("America/Chicago")
UTC = timezone.utc

Window = Tuple[datetime, datetime]

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}
_MONTH_ABBR = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# "October 25 10:00 PM", "Oct. 25, 2026 6 a.m.", "6:00 PM" (date optionnelle)
_POINT_RE = re.compile(
    r"^(?:(?P<month>[A-Za-z]{3,9})\.?\s+(?P<day>\d{1,2})(?:st|nd|rd|th)?,?\s*(?:(?P<year>\d{4}),?\s*)?)?"
    r"(?:at\s+)?(?P<hour>\d{1,2})(?::(?P<minute>\d{2}))?\s*(?P<ampm>[ap])\.?\s*m\.?$",
    re.IGNORECASE,
)
_SPLIT_RE = re.compile(r"\s+(?:to|until|through)\s+|\s*(?:->|→|–|—)\s*|\s+-\s+", re.IGNORECASE)


def now_utc() -> datetime:
    return datetime.now(UTC)


def aware(dt) -> Optional[datetime]:
    """Mongo rend des datetimes naïfs (UTC) ; None si ce n'est pas un datetime."""
    if not isinstance(dt, datetime):
        return None
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt


def overlaps(window: Window, start: datetime, end: datetime) -> bool:
    return window[0] < end and start < window[1]


def _parse_point(text: str):
    m = _POINT_RE.match(text.strip())
    if not m:
        return None
    month = None
    if m.group("month"):
        month = _MONTHS.get(m.group("month")[:3].lower())
        if month is None:
            return None
    hour = int(m.group("hour")) % 12
    if m.group("ampm").lower() == "p":
        hour += 12
    return {
        "month": month,
        "day": int(m.group("day")) if m.group("day") else None,
        "year": int(m.group("year")) if m.group("year") else None,
        "hour": hour,
        "minute": int(m.group("minute") or 0),
    }


def _build(year: int, p: dict) -> Optional[datetime]:
    try:
        return datetime(year, p["month"], p["day"], p["hour"], p["minute"], tzinfo=STARBASE_TZ)
    except ValueError:
        return None


def parse_window(text: Optional[str], now: Optional[datetime] = None) -> Optional[Window]:
    """
    Parse a City of Starbase window such as 'Oct. 25 6:00 AM to Oct. 25 6:00 PM'
    into aware (start, end) datetimes, Starbase local time.
    The site gives no year: we take the one that puts the start closest to `now`.
    Returns None when the text is not understood (callers fall back to the raw string).
    """
    if not text:
        return None
    now = now or now_utc()
    parts = [p for p in _SPLIT_RE.split(str(text).strip()) if p and p.strip()]
    if len(parts) != 2:
        return None
    start_p, end_p = _parse_point(parts[0]), _parse_point(parts[1])
    if not start_p or not end_p or start_p["month"] is None:
        return None

    if start_p["year"]:
        start = _build(start_p["year"], start_p)
    else:
        candidates = [_build(y, start_p) for y in (now.year - 1, now.year, now.year + 1)]
        candidates = [c for c in candidates if c is not None]
        start = min(candidates, key=lambda c: abs((c - now).total_seconds())) if candidates else None
    if start is None:
        return None

    if end_p["month"] is None:
        # "6:00 AM to 6:00 PM" : fin le même jour, ou le lendemain si l'heure est passée
        end = start.replace(hour=end_p["hour"], minute=end_p["minute"])
        if end <= start:
            end += timedelta(days=1)
    else:
        end = _build(end_p["year"] or start.year, end_p)
        if end is not None and end < start and not end_p["year"]:
            end = _build(start.year + 1, end_p)
    if end is None or end <= start:
        return None
    return start, end


def parse_windows(texts: List[Optional[str]], now: Optional[datetime] = None) -> List[Window]:
    """Parse several window strings ('a | b' lists are split), dropping the ones not understood."""
    windows: List[Window] = []
    for text in texts:
        for part in str(text or "").split("|"):
            w = parse_window(part, now=now)
            if w:
                windows.append(w)
    return windows


def _fmt_clock(dt: datetime, twelve_hour: bool) -> str:
    if twelve_hour:
        hour = dt.hour % 12 or 12
        return f"{hour}:{dt.minute:02d} {'AM' if dt.hour < 12 else 'PM'}"
    return f"{dt.hour:02d}:{dt.minute:02d}"


def fmt_day(dt: datetime) -> str:
    return f"{_MONTH_ABBR[dt.month - 1]} {dt.day}"


def fmt_range(start: datetime, end: datetime, tz=STARBASE_TZ, twelve_hour: bool = True) -> str:
    """'Oct 25, 10:00 PM -> Oct 26, 4:30 AM' (the day is not repeated when it is the same)."""
    s, e = start.astimezone(tz), end.astimezone(tz)
    left = f"{fmt_day(s)}, {_fmt_clock(s, twelve_hour)}"
    right = _fmt_clock(e, twelve_hour) if s.date() == e.date() else f"{fmt_day(e)}, {_fmt_clock(e, twelve_hour)}"
    return f"{left} → {right}"


def fmt_local(window: Window) -> str:
    return fmt_range(window[0], window[1], tz=STARBASE_TZ, twelve_hour=True) + " CT"


def fmt_utc(window: Window) -> str:
    return fmt_range(window[0], window[1], tz=UTC, twelve_hour=False) + " UTC"


def fmt_point_utc(dt: datetime) -> str:
    d = dt.astimezone(UTC)
    return f"{fmt_day(d)}, {_fmt_clock(d, False)} UTC"


def fmt_point_local(dt: datetime) -> str:
    d = dt.astimezone(STARBASE_TZ)
    return f"{_fmt_clock(d, True)} CT"


def fmt_duration(delta: timedelta) -> str:
    """'2d 3h', '14h', '45 min' (rounded down, never negative)."""
    minutes = max(int(delta.total_seconds() // 60), 0)
    days, rem = divmod(minutes, 1440)
    hours, mins = divmod(rem, 60)
    if days:
        return f"{days}d {hours}h" if hours else f"{days}d"
    if hours:
        return f"{hours}h" if hours >= 6 or not mins else f"{hours}h {mins:02d}"
    return f"{mins} min"


def relative_label(window: Window, now: Optional[datetime] = None) -> str:
    """'starts in 14h' / 'in progress, ends in 2h' / 'ended'."""
    now = now or now_utc()
    start, end = window
    if now < start:
        return f"starts in {fmt_duration(start - now)}"
    if now < end:
        return f"in progress, ends in {fmt_duration(end - now)}"
    return "ended"
