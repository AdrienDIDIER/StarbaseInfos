from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests

from utils.cards import safe_render_card
from utils.notify import fit_lines, report_failure, report_success, tweet_len
from utils.timeparse import UTC, Window, aware, fmt_day, fmt_local, fmt_utc, now_utc, relative_label
from utils.tracking import sync_tracked
from utils.utils import get_database, get_api_twitter

logging.basicConfig(level=logging.INFO)

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0 Safari/537.36"
)
# Avis de navigation de la NGA (NAVAREA IV couvre le golfe) : les zones de danger des lancements y sont publiées
NGA_WARNINGS_URL = "https://msi.nga.mil/api/publications/broadcast-warn"
NAV_COLLECTION = "NavWarnings"

NAV_SOURCE_LABEL = "Source: NGA maritime safety info"
NAV_HASHTAGS = "#Starship #Starbase"

_NAV_AREA_LABELS = {"4": "NAVAREA IV", "12": "NAVAREA XII", "A": "HYDROLANT", "P": "HYDROPAC", "C": "HYDROARC"}
_MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]

_SPACE_RE = re.compile(r"ROCKET\s+LAUNCH|SPACE\s+DEBRIS|SPACE\s+OPERATIONS|SPACE\s+VEHICLE", re.IGNORECASE)
_STARBASE_AREA_RE = re.compile(r"BROWNSVILLE|BOCA\s+CHICA|STARBASE", re.IGNORECASE)
# 25-59.00N 097-07.00W ou 25-59N 097-07W
_COORD_RE = re.compile(r"(\d{2})-(\d{2}(?:\.\d+)?)\s*([NS])\s+(\d{3})-(\d{2}(?:\.\d+)?)\s*([EW])")
# 132245Z TO 140128Z NOV
_WINDOW_RE = re.compile(r"(\d{2})(\d{2})(\d{2})Z\s+TO\s+(\d{2})(\d{2})(\d{2})Z")
_MONTH_RE = re.compile(r"\b(" + "|".join(_MONTHS) + r")\b")

# Boîte autour de Starbase : côte sud du Texas et golfe proche
_LAT_RANGE = (24.5, 27.5)
_LON_RANGE = (-98.0, -94.5)


# ---------------------------
# HTTP
# ---------------------------

def fetch_warnings() -> List[Dict[str, Any]]:
    resp = requests.get(
        NGA_WARNINGS_URL,
        params={"status": "active", "output": "json"},
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        timeout=30,
    )
    resp.raise_for_status()
    warnings = resp.json().get("broadcast-warn")
    # Des centaines d'avis sont actifs en permanence : une liste vide est une panne
    if not isinstance(warnings, list) or not warnings:
        raise RuntimeError("NGA broadcast warnings list is empty or missing")
    return warnings


# ---------------------------
# Parsing
# ---------------------------

def parse_coordinates(text: str) -> List[Tuple[float, float]]:
    coords = []
    for lat_d, lat_m, ns, lon_d, lon_m, ew in _COORD_RE.findall(text):
        lat = (int(lat_d) + float(lat_m) / 60) * (1 if ns == "N" else -1)
        lon = (int(lon_d) + float(lon_m) / 60) * (1 if ew == "E" else -1)
        coords.append((lat, lon))
    return coords


def is_starbase_launch_warning(warning: Dict[str, Any]) -> bool:
    text = str(warning.get("text") or "")
    if not _SPACE_RE.search(text):
        return False
    if _STARBASE_AREA_RE.search(text):
        return True
    return any(
        _LAT_RANGE[0] <= lat <= _LAT_RANGE[1] and _LON_RANGE[0] <= lon <= _LON_RANGE[1]
        for lat, lon in parse_coordinates(text)
    )


def parse_warning_windows(text: str, year: int) -> List[Window]:
    """
    'ROCKET LAUNCHING 132245Z TO 140128Z NOV, ALTERNATE 142245Z TO 150128Z NOV' -> UTC windows.
    The month is the first one written after each DDHHMMZ pair.
    """
    windows: List[Window] = []
    for m in _WINDOW_RE.finditer(text):
        month_m = _MONTH_RE.search(text, m.end())
        if not month_m:
            continue
        month = _MONTHS.index(month_m.group(1)) + 1
        d1, h1, mi1, d2, h2, mi2 = (int(g) for g in m.groups())
        try:
            # Le mois écrit est celui de la fin : un début à jour plus grand est le mois précédent
            start_month, start_year = month, year
            if d1 > d2:
                start_month, start_year = (12, year - 1) if month == 1 else (month - 1, year)
            start = datetime(start_year, start_month, d1, h1, mi1, tzinfo=UTC)
            end = datetime(year, month, d2, h2, mi2, tzinfo=UTC)
        except ValueError:
            continue
        if end > start:
            windows.append((start, end))
    return sorted(windows)


def _activity(text: str) -> str:
    upper = text.upper()
    if "ROCKET LAUNCH" in upper:
        return "Rocket launching"
    if "SPACE DEBRIS" in upper:
        return "Space debris"
    return "Space operations"


def collect_starbase_warnings(warnings: List[Dict[str, Any]], now: datetime) -> Dict[str, Dict[str, Any]]:
    items: Dict[str, Dict[str, Any]] = {}
    for warning in warnings:
        if not is_starbase_launch_warning(warning):
            continue
        text = str(warning.get("text") or "")
        area = str(warning.get("navArea") or "").strip()
        number, year = warning.get("msgNumber"), int(warning.get("msgYear") or now.year)
        windows = parse_warning_windows(text, year if year > 100 else 2000 + year)
        key = f"{area}-{number}/{year}"
        items[key] = {
            "reference": f"{_NAV_AREA_LABELS.get(area, f'NAVAREA {area}')} {number}/{str(year)[-2:]}",
            "activity": _activity(text),
            "text": text,
            "windows": [[start, end] for start, end in windows],
            "begin": windows[0][0] if windows else None,
            "end": windows[-1][1] if windows else None,
        }
    return items


# ---------------------------
# Post helpers
# ---------------------------

_EVENT_HEAD = {
    "new": "⚓ NEW",
    "updated": "\U0001f504 UPDATED",
    "removed": "❌ CANCELLED",
}


def item_windows(item: Dict[str, Any]) -> List[Window]:
    return [(aware(start), aware(end)) for start, end in item.get("windows") or []]


def build_warning_post(
    event: str,
    item: Dict[str, Any],
    previous: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> Tuple[str, Optional[bytes]]:
    now = now or now_utc()
    windows = item_windows(item)
    pending = [w for w in windows if w[1] > now]
    reference = item.get("reference") or "NAVAREA IV"
    lines: List[tuple] = [
        (f"{_EVENT_HEAD[event]} · Marine hazard area off Starbase", 0),
        (f"{item.get('activity') or 'Space operations'} · {reference}", 0),
    ]
    rows: List[Tuple[str, str]] = [("Notice", f"{item.get('activity') or 'Space operations'} · {reference}")]

    if event == "removed":
        if pending:
            lines.append((f"Was: {fmt_utc(pending[0])}", 0))
            rows.append(("Was scheduled", fmt_utc(pending[0])))
        lines.append(("The notice is no longer active.", 0))
        lines.append(("Can mean the planned launch attempt has moved.", 5))
        rows.append(("Status", "Notice no longer active"))
    else:
        first = (pending or windows or [None])[0]
        if first:
            lines.append((f"\U0001f310 {fmt_utc(first)}", 0))
            lines.append((f"\U0001f552 {fmt_local(first)}", 2))
            rows.append(("UTC", fmt_utc(first)))
            rows.append(("Local time", fmt_local(first)))
            alternates = [w for w in (pending or windows) if w != first]
            if alternates:
                plural = "s" if len(alternates) > 1 else ""
                alt_line = f"+{len(alternates)} alternate window{plural}, through {fmt_day(alternates[-1][1])} UTC"
                lines.append((alt_line, 1))
                rows.append(("Alternates", alt_line.lstrip("+")))
            if now < first[1]:
                lines.append((f"⏳ {relative_label(first, now).capitalize()}", 4))
        else:
            lines.append(("Window: see the notice text.", 0))
        if event == "updated" and previous is not None:
            lines.append((f"Replaces {previous.get('reference')}", 1))
        elif event == "new":
            lines.append(("Hazard areas are posted a few days before a launch attempt.", 5))

    lines.append((NAV_SOURCE_LABEL, 1))
    lines.append((NAV_HASHTAGS, 3))

    accent = "cancelled" if event == "removed" else event
    card = safe_render_card(
        _EVENT_HEAD[event].split(" ", 1)[1], "Marine hazard area off Starbase",
        rows=rows[:5], accent=accent, footer=NAV_SOURCE_LABEL,
    )
    return fit_lines(lines), card


# ---------------------------
# Runner
# ---------------------------

def run_nav_warning_checks() -> None:
    db = get_database()
    api = get_api_twitter()
    now = now_utc()

    try:
        current = collect_starbase_warnings(fetch_warnings(), now)
    except Exception as e:
        report_failure(db, "navwarn-fetch", f"Cannot read NGA broadcast warnings: {e}", threshold=6)
        raise
    report_success(db, "navwarn-fetch")

    posted, failures = sync_tracked(
        db, NAV_COLLECTION, current, now,
        build_post=lambda event, item, previous: build_warning_post(event, item, previous, now=now),
        label="NAV", api=api,
    )
    logging.info(f"[NAV] Done. Listed: {len(current)}, Posted: {posted}, Failed: {failures}.")
    if failures:
        report_failure(db, "navwarn-post", f"{failures} marine notice post(s) failed, will retry on the next run.")
        raise RuntimeError(f"{failures} marine notice post(s) failed")
    report_success(db, "navwarn-post")


def preview(path: Optional[str] = None) -> None:
    """Print what would be posted for the warnings active right now. No Twitter or MongoDB call."""
    now = now_utc()
    if path:
        with open(path, encoding="utf-8") as f:
            warnings = json.load(f)["broadcast-warn"]
    else:
        warnings = fetch_warnings()
    current = collect_starbase_warnings(warnings, now)
    print(f"{len(warnings)} active warnings, {len(current)} launch hazard notice(s) near Starbase.\n")
    for key, item in current.items():
        text, _ = build_warning_post("new", item, now=now)
        print(f"--- {key} | {tweet_len(text)} chars\n{text}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Track NGA launch hazard notices near Starbase.")
    parser.add_argument("--preview", action="store_true", help="Print the posts instead of publishing.")
    parser.add_argument("--file", default=None, help="With --preview: read a saved API response.")
    args = parser.parse_args()
    try:
        preview(args.file) if args.preview else run_nav_warning_checks()
    except Exception as e:
        logging.error(f"[NAV][ERR] Failed to run nav warning checks: {e}")
        sys.exit(1)
