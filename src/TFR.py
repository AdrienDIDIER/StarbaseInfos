from __future__ import annotations

import argparse
import logging
import re
import sys
from datetime import datetime
from html import unescape
from typing import Any, Dict, List, Optional, Tuple

import requests

from utils.cards import safe_render_card
from utils.notify import fit_lines, report_failure, report_success, tweet_len
from utils.timeparse import UTC, aware, fmt_local, fmt_utc, now_utc, relative_label
from utils.tracking import sync_tracked
from utils.utils import get_database, get_api_twitter

logging.basicConfig(level=logging.INFO)

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0 Safari/537.36"
)
TFR_LIST_URL = "https://tfr.faa.gov/tfrapi/exportTfrList"
TFR_DETAIL_URL = "https://tfr.faa.gov/tfrapi/getWebText"
TFR_COLLECTION = "TFR"

TFR_SOURCE_LABEL = "Source: FAA"
TFR_HASHTAGS = "#Starship #Starbase"

# Les TFR "Security" permanents de Brownsville ne nous intéressent pas : seulement les opérations spatiales
_SPACE_TYPE_RE = re.compile(r"space\s+operations", re.IGNORECASE)
_STARBASE_AREA_RE = re.compile(r"brownsville|boca\s+chica|starbase|port\s+isabel|south\s+padre", re.IGNORECASE)
_DETAIL_DATE_RE = r"{label}\s*:\s*([A-Za-z]+ \d{{1,2}}, \d{{4}}) at (\d{{4}}) UTC"
_ALTITUDE_RE = re.compile(r"Altitude:\s*([^\n]+)", re.IGNORECASE)


# ---------------------------
# HTTP
# ---------------------------

def _get_json(url: str, params: Optional[dict] = None, timeout: int = 20) -> Any:
    resp = requests.get(url, params=params, headers={"User-Agent": USER_AGENT}, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def fetch_tfr_list() -> List[Dict[str, Any]]:
    data = _get_json(TFR_LIST_URL)
    # La FAA liste toujours des dizaines de TFR : une liste vide est une panne, pas une info
    if not isinstance(data, list) or not data:
        raise RuntimeError("FAA TFR list is empty or not a list")
    return data


def fetch_tfr_detail(notam_id: str) -> str:
    """Detail page of a TFR, flattened to text (one field per line)."""
    data = _get_json(TFR_DETAIL_URL, params={"notamId": notam_id})
    html = data[0]["text"] if isinstance(data, list) and data else ""
    text = unescape(re.sub(r"<[^>]+>", "\n", html))
    return re.sub(r"\s*\n\s*", "\n", text).strip()


# ---------------------------
# Parsing
# ---------------------------

def is_starbase_space_tfr(tfr: Dict[str, Any]) -> bool:
    return bool(
        _SPACE_TYPE_RE.search(str(tfr.get("type") or ""))
        and _STARBASE_AREA_RE.search(str(tfr.get("description") or ""))
    )


def _parse_detail_date(detail: str, label: str) -> Optional[datetime]:
    m = re.search(_DETAIL_DATE_RE.format(label=re.escape(label)), detail)
    if not m:
        return None
    try:
        return datetime.strptime(f"{m.group(1)} {m.group(2)}", "%B %d, %Y %H%M").replace(tzinfo=UTC)
    except ValueError:
        return None


def parse_tfr_detail(detail: str) -> Dict[str, Any]:
    """Extract begin / end (UTC) and the altitude line from the flattened detail text."""
    altitude = _ALTITUDE_RE.search(detail)
    return {
        "begin": _parse_detail_date(detail, "Beginning Date and Time"),
        "end": _parse_detail_date(detail, "Ending Date and Time"),
        "altitude": altitude.group(1).strip().rstrip(".") if altitude else None,
    }


def _location(description: Optional[str]) -> str:
    """'Brownsville, TX, Friday, November 13, 2026 through ...' -> 'Brownsville, TX'."""
    parts = [p.strip() for p in str(description or "").split(",")]
    return ", ".join(parts[:2]) if len(parts) >= 2 else (parts[0] or "Starbase area")


def collect_starbase_tfrs(tfr_list: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    items: Dict[str, Dict[str, Any]] = {}
    for tfr in tfr_list:
        if not is_starbase_space_tfr(tfr):
            continue
        notam_id = str(tfr.get("notam_id") or "").strip()
        if not notam_id:
            continue
        item = {
            "notam_id": notam_id,
            "type": tfr.get("type"),
            "description": tfr.get("description"),
            "location": _location(tfr.get("description")),
            "begin": None,
            "end": None,
            "altitude": None,
        }
        try:
            item.update(parse_tfr_detail(fetch_tfr_detail(notam_id)))
        except Exception as e:
            # Le détail est un plus : on annonce quand même le TFR avec la description de la liste
            logging.warning(f"[TFR] Could not read detail of {notam_id}: {e}")
        items[notam_id] = item
    return items


# ---------------------------
# Post helpers
# ---------------------------

_EVENT_HEAD = {
    "new": "✈️ NEW",
    "updated": "\U0001f504 UPDATED",
    "removed": "❌ REMOVED",
}


def _window(item: Dict[str, Any]) -> Optional[Tuple[datetime, datetime]]:
    begin, end = aware(item.get("begin")), aware(item.get("end"))
    return (begin, end) if begin and end and end > begin else None


def build_tfr_post(
    event: str,
    item: Dict[str, Any],
    previous: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> Tuple[str, Optional[bytes]]:
    now = now or now_utc()
    window = _window(item)
    notam = f"FDC {item['notam_id']}"
    lines: List[tuple] = [
        (f"{_EVENT_HEAD[event]} · Starbase airspace restriction (TFR)", 0),
        (f"Space operations · {item.get('location') or 'Starbase area'}", 0),
    ]
    rows: List[Tuple[str, str]] = [("Where", f"{item.get('location') or 'Starbase area'} · {notam}")]

    if event == "removed":
        if window:
            lines.append((f"Was: {fmt_utc(window)}", 0))
            rows.append(("Was scheduled", fmt_utc(window)))
        lines.append((f"{notam} is no longer on the FAA list.", 0))
        lines.append(("Can mean the planned test or launch attempt has moved.", 5))
        rows.append(("Status", "No longer on the FAA list"))
    else:
        if window:
            lines.append((f"\U0001f310 {fmt_utc(window)}", 0))
            lines.append((f"\U0001f552 {fmt_local(window)}", 2))
            if now < window[1]:
                lines.append((f"⏳ {relative_label(window, now).capitalize()}", 4))
            rows.append(("UTC", fmt_utc(window)))
            rows.append(("Local time", fmt_local(window)))
        else:
            # Pas de détail lisible : on donne la description brute de la FAA
            lines.append((str(item.get("description") or ""), 0))
        if item.get("altitude"):
            lines.append((f"Altitude: {item['altitude']}", 5))
            rows.append(("Altitude", item["altitude"]))
        if event == "updated" and previous is not None:
            lines.append((f"Replaces FDC {previous.get('notam_id')}", 1))
            prev_window = _window(previous)
            if prev_window:
                rows.append(("Previously", fmt_utc(prev_window)))
        lines.append((notam, 1))

    lines.append((TFR_SOURCE_LABEL, 1))
    lines.append((TFR_HASHTAGS, 3))

    accent = "cancelled" if event == "removed" else event
    card = safe_render_card(
        _EVENT_HEAD[event].split(" ", 1)[1], "Starbase airspace restriction",
        rows=rows[:5], accent=accent, footer=TFR_SOURCE_LABEL,
    )
    return fit_lines(lines), card


# ---------------------------
# Runner
# ---------------------------

def run_TFR_checks() -> None:
    db = get_database()
    api = get_api_twitter()
    now = now_utc()

    try:
        current = collect_starbase_tfrs(fetch_tfr_list())
    except Exception as e:
        report_failure(db, "tfr-fetch", f"Cannot read the FAA TFR list: {e}")
        raise
    report_success(db, "tfr-fetch")

    posted, failures = sync_tracked(
        db, TFR_COLLECTION, current, now,
        build_post=lambda event, item, previous: build_tfr_post(event, item, previous, now=now),
        label="TFR", api=api,
    )
    logging.info(f"[TFR] Done. Listed: {len(current)}, Posted: {posted}, Failed: {failures}.")
    if failures:
        report_failure(db, "tfr-post", f"{failures} TFR post(s) failed, will retry on the next run.")
        raise RuntimeError(f"{failures} TFR post(s) failed")
    report_success(db, "tfr-post")


def preview() -> None:
    """Print what would be posted for the TFRs listed right now. No Twitter or MongoDB call."""
    tfr_list = fetch_tfr_list()
    current = collect_starbase_tfrs(tfr_list)
    print(f"{len(tfr_list)} TFRs listed by the FAA, {len(current)} space-operations TFR(s) near Starbase.\n")
    for item in current.values():
        text, _ = build_tfr_post("new", item)
        print(f"--- {item['notam_id']} | {tweet_len(text)} chars\n{text}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Track FAA space-operations TFRs near Starbase.")
    parser.add_argument("--preview", action="store_true", help="Print the posts instead of publishing.")
    args = parser.parse_args()
    try:
        preview() if args.preview else run_TFR_checks()
    except Exception as e:
        logging.error(f"[TFR][ERR] Failed to run TFR checks: {e}")
        sys.exit(1)
