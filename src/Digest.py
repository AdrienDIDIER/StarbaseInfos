from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from Scraper_RC import get_active_closures, record_windows
from TFR import TFR_COLLECTION
from NavWarnings import NAV_COLLECTION, item_windows
from FlightTracker import fmt_net
from utils.cards import safe_render_card
from utils.launches import get_upcoming_starship_launches
from utils.notify import TWEET_CHAR_LIMIT, fit_lines, publish, tweet_len
from utils.timeparse import (
    STARBASE_TZ,
    aware,
    fmt_day,
    fmt_duration,
    fmt_local,
    fmt_point_utc,
    fmt_utc,
    now_utc,
)
from utils.tracking import get_active
from utils.utils import get_database, get_api_twitter

logging.basicConfig(level=logging.INFO)

DIGEST_COLLECTION = "Digest"
DIGEST_HASHTAGS = "#Starbase #Starship"
_WEEKDAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


# ---------------------------
# Lines
# ---------------------------

def _next_window(docs: List[Dict[str, Any]], now: datetime):
    """Earliest window not ended yet among the given closure docs."""
    windows = sorted(w for doc in docs for w in record_windows(doc, now) if w[1] > now)
    return windows[0] if windows else None


def beach_line(closures: List[Dict[str, Any]], now: datetime) -> str:
    beach = [c for c in closures if str(c.get("category") or "").lower() == "beach"]
    if not beach:
        return "no closure listed"
    window = _next_window(beach, now)
    return f"closure {fmt_local(window)}" if window else "closure listed"


def road_line(closures: List[Dict[str, Any]], now: datetime, compact: bool = False) -> str:
    roads = [c for c in closures if str(c.get("category") or "road").lower() != "beach"]
    if not roads:
        return "no delays listed"
    window = _next_window(roads, now)
    count = f"{len(roads)} delay{'s' if len(roads) > 1 else ''} listed"
    return f"{count}, next {fmt_local(window)}" if window and not compact else count


def tfr_line(tfrs: List[Dict[str, Any]]) -> str:
    if not tfrs:
        return "none"
    begin, end = aware(tfrs[0].get("begin")), aware(tfrs[0].get("end"))
    if begin and end:
        return fmt_utc((begin, end))
    return f"FDC {tfrs[0].get('notam_id')} active"


def marine_line(warnings: List[Dict[str, Any]], now: datetime) -> str:
    if not warnings:
        return "none"
    pending = sorted(w for doc in warnings for w in item_windows(doc) if w[1] > now)
    return fmt_utc(pending[0]) if pending else "notice active"


def launch_line(launch: Optional[Dict[str, Any]], now: datetime, compact: bool = False) -> str:
    if launch is None:
        return "no Starship launch dated yet"
    if not launch["firm"]:
        return f"{launch['label']}, date not firm yet"
    if compact:
        label = launch["label"].replace("Starship ", "")
        net = fmt_point_utc(launch["net"]) if launch.get("has_time") else fmt_day(launch["net"])
        return f"{label} NET {net}"
    return f"{launch['label']} NET {fmt_net(launch)}, T-{fmt_duration(launch['net'] - now)}"


def _digest_rows(closures, tfrs, warnings, launch, now: datetime, compact: bool = False) -> List[Tuple[str, str]]:
    return [
        ("Beach", beach_line(closures, now)),
        ("Road", road_line(closures, now, compact)),
        ("TFR" if compact else "Space-ops TFR", tfr_line(tfrs)),
        ("Marine" if compact else "Marine hazard area", marine_line(warnings, now)),
        ("Next launch", launch_line(launch, now, compact)),
    ]


def build_digest_post(
    closures: List[Dict[str, Any]],
    tfrs: List[Dict[str, Any]],
    warnings: List[Dict[str, Any]],
    launch: Optional[Dict[str, Any]],
    now: Optional[datetime] = None,
) -> Tuple[str, Optional[bytes]]:
    now = now or now_utc()
    local = now.astimezone(STARBASE_TZ)
    day = f"{_WEEKDAYS[local.weekday()]}, {fmt_day(local)}"
    rows = _digest_rows(closures, tfrs, warnings, launch, now)
    emojis = ["\U0001f3d6️", "\U0001f6a7", "✈️", "⚓", "\U0001f680"]

    def post_lines(post_rows: List[Tuple[str, str]]) -> List[tuple]:
        lines: List[tuple] = [(f"\U0001f4cb Starbase today · {day}", 0)]
        lines += [(f"{emoji} {label}: {value}", 0) for emoji, (label, value) in zip(emojis, post_rows)]
        return lines + [(DIGEST_HASHTAGS, 3)]

    lines = post_lines(rows)
    if tweet_len("\n".join(text for text, _ in lines[:-1])) > TWEET_CHAR_LIMIT:
        # Journée chargée : libellés courts, la carte garde le détail complet
        lines = post_lines(_digest_rows(closures, tfrs, warnings, launch, now, compact=True))

    card = safe_render_card(
        day, "Starbase today", rows=rows, accent="info",
        footer="Sources: City of Starbase, FAA, NGA, Launch Library 2",
    )
    return fit_lines(lines), card


# ---------------------------
# Runner
# ---------------------------

def run_digest() -> None:
    db = get_database()
    api = get_api_twitter()
    now = now_utc()

    # Un seul digest par jour (date locale de Starbase), même si la tâche est relancée
    day_key = now.astimezone(STARBASE_TZ).strftime("%Y-%m-%d")
    coll = db[DIGEST_COLLECTION]
    if coll.find_one({"day": day_key}):
        logging.info(f"[DIGEST] Already posted for {day_key}.")
        return

    try:
        launches = get_upcoming_starship_launches(db, now=now)
    except Exception as e:
        # Le digest reste utile sans la date de lancement
        logging.warning(f"[DIGEST] Launch date unavailable: {e}")
        launches = []

    text, card = build_digest_post(
        get_active_closures(db, now),
        get_active(db, TFR_COLLECTION, now),
        get_active(db, NAV_COLLECTION, now),
        launches[0] if launches else None,
        now=now,
    )
    tweet_id = publish(text, image=card, api=api)
    coll.update_one({"day": day_key}, {"$set": {"day": day_key, "tweet_id": tweet_id, "posted_at": now}}, upsert=True)
    logging.info(f"[DIGEST] Posted digest for {day_key} tweet_id={tweet_id}")


def preview(card_path: Optional[str] = None) -> None:
    """Print today's digest with empty closure/TFR/marine state. Reads the launch API only."""
    now = now_utc()
    try:
        launches = get_upcoming_starship_launches(None, now=now)
    except Exception as e:
        print(f"Launch date unavailable: {e}\n")
        launches = []
    text, card = build_digest_post([], [], [], launches[0] if launches else None, now=now)
    print(f"--- {tweet_len(text)} chars\n{text}")
    if card_path and card:
        with open(card_path, "wb") as f:
            f.write(card)
        print(f"\nCard written to {card_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Daily 'Starbase today' digest.")
    parser.add_argument("--preview", action="store_true", help="Print the post instead of publishing.")
    parser.add_argument("--card", default=None, help="With --preview: write the image card to this path.")
    args = parser.parse_args()
    try:
        preview(args.card) if args.preview else run_digest()
    except Exception as e:
        logging.error(f"[DIGEST][ERR] Failed to run digest: {e}")
        sys.exit(1)
