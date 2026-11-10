from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from Scraper_RC import get_active_closures, record_windows
from TFR import TFR_COLLECTION
from NavWarnings import NAV_COLLECTION, item_windows
from utils.cards import safe_render_card
from utils.launches import get_upcoming_starship_launches
from utils.notify import fit_lines, publish, tweet_len
from utils.timeparse import (
    aware,
    fmt_day,
    fmt_duration,
    fmt_point_local,
    fmt_point_utc,
    now_utc,
    overlaps,
)
from utils.tracking import get_active
from utils.utils import get_database, get_api_twitter

logging.basicConfig(level=logging.INFO)

TRACKER_COLLECTION = "FlightTracker"
TRACKER_HASHTAGS = "#Starship #Starbase"

# On ne suit un vol qu'à l'approche, quand les signaux officiels commencent à tomber
TRACK_WITHIN = timedelta(days=21)
# Un signal compte pour le vol s'il couvre la date visée à +/- 1 jour (les fenêtres glissent souvent)
SIGNAL_MARGIN = timedelta(hours=24)
# Pas plus d'un post de suivi par demi-heure, même si la date bouge plusieurs fois
MIN_POST_INTERVAL = timedelta(minutes=30)

SIGNAL_LABELS = {
    "beach": "Beach closure",
    "tfr": "Airspace TFR",
    "marine": "Marine hazard area",
}


# ---------------------------
# Signals
# ---------------------------

def _around(net: datetime) -> Tuple[datetime, datetime]:
    return net - SIGNAL_MARGIN, net + SIGNAL_MARGIN


def beach_signal(closures: List[Dict[str, Any]], net: datetime, now: datetime) -> Optional[str]:
    """Day of the beach closure window matching the launch date, None when there is none."""
    start, end = _around(net)
    for doc in closures:
        if str(doc.get("category") or "").lower() != "beach":
            continue
        for window in record_windows(doc, now):
            if overlaps(window, start, end):
                return fmt_day(window[0])
    return None


def tfr_signal(tfrs: List[Dict[str, Any]], net: datetime) -> Optional[str]:
    start, end = _around(net)
    for doc in tfrs:
        begin, stop = aware(doc.get("begin")), aware(doc.get("end"))
        if begin and stop:
            if overlaps((begin, stop), start, end):
                return f"FDC {doc.get('notam_id')}"
        else:
            # Dates illisibles : un TFR spatial actif à Starbase reste un signal
            return f"FDC {doc.get('notam_id')}"
    return None


def marine_signal(warnings: List[Dict[str, Any]], net: datetime) -> Optional[str]:
    start, end = _around(net)
    for doc in warnings:
        windows = item_windows(doc)
        if not windows or any(overlaps(w, start, end) for w in windows):
            return str(doc.get("reference") or "posted")
    return None


def collect_signals(db, launch: Dict[str, Any], now: datetime) -> Dict[str, Optional[str]]:
    net = launch["net"]
    return {
        "beach": beach_signal(get_active_closures(db, now), net, now),
        "tfr": tfr_signal(get_active(db, TFR_COLLECTION, now), net),
        "marine": marine_signal(get_active(db, NAV_COLLECTION, now), net),
    }


def pick_tracked_launch(launches: List[Dict[str, Any]], now: datetime) -> Optional[Dict[str, Any]]:
    """Next Starbase launch with a real date, close enough to be worth tracking."""
    for launch in launches:
        if launch["at_starbase"] and launch["firm"] and launch["net"] - now <= TRACK_WITHIN:
            return launch
    return None


# ---------------------------
# Post helpers
# ---------------------------

def fmt_net(launch: Dict[str, Any]) -> str:
    net = launch["net"]
    if launch.get("has_time"):
        return f"{fmt_point_utc(net)} ({fmt_point_local(net)})"
    return f"{fmt_day(net)} (time to be confirmed)"


def make_signature(launch: Dict[str, Any], signals: Dict[str, Optional[str]]) -> str:
    present = "".join("1" if signals.get(name) else "0" for name in SIGNAL_LABELS)
    return f"{launch['net']:%Y%m%d%H%M}|{launch.get('status')}|{present}"


def describe_change(
    launch: Dict[str, Any],
    signals: Dict[str, Optional[str]],
    state: Optional[Dict[str, Any]],
) -> str:
    """What changed since the previous tracker post, empty on the first post."""
    if not state:
        return ""
    previous_net = aware(state.get("net"))
    if previous_net and previous_net != launch["net"]:
        return f"\U0001f504 Date moved, was {fmt_point_utc(previous_net)}"
    previous_signals = state.get("signals") or {}
    for name, label in SIGNAL_LABELS.items():
        if signals.get(name) and not previous_signals.get(name):
            return f"\U0001f195 {label} posted"
        if previous_signals.get(name) and not signals.get(name):
            return f"⚠️ {label} withdrawn"
    if state.get("status") != launch.get("status") and launch.get("status_name"):
        return f"\U0001f504 Status now: {launch['status_name']}"
    return ""


def is_major_change(
    launch: Dict[str, Any],
    signals: Dict[str, Optional[str]],
    state: Optional[Dict[str, Any]],
) -> bool:
    """
    Major = first post, date moved, or a signal appeared / was withdrawn: worth a post of its own.
    A status change alone is minor and goes as a reply.
    """
    if not state:
        return True
    previous_net = aware(state.get("net"))
    if previous_net and previous_net != launch["net"]:
        return True
    previous_signals = state.get("signals") or {}
    return any(bool(signals.get(name)) != bool(previous_signals.get(name)) for name in SIGNAL_LABELS)


def build_tracker_post(
    launch: Dict[str, Any],
    signals: Dict[str, Optional[str]],
    state: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> Tuple[str, Optional[bytes]]:
    now = now or now_utc()
    count = sum(1 for name in SIGNAL_LABELS if signals.get(name))
    countdown = f"T-{fmt_duration(launch['net'] - now)}"

    lines: List[tuple] = [
        (f"\U0001f680 {launch['label']} · launch signals", 0),
        (describe_change(launch, signals, state), 1),
        (f"NET {fmt_net(launch)} · {countdown}", 0),
    ]
    checklist: List[Tuple[bool, str]] = []
    for name, label in SIGNAL_LABELS.items():
        detail = signals.get(name)
        if detail:
            lines.append((f"✅ {label}: {detail}", 0))
            checklist.append((True, f"{label}: {detail}"))
        else:
            lines.append((f"⬜ {label}: not posted yet", 0))
            checklist.append((False, f"{label}: not posted yet"))
    lines.append((f"{count}/{len(SIGNAL_LABELS)} official signals in", 2))
    if not state:
        lines.append(("Follow for updates.", 4))
    lines.append((TRACKER_HASHTAGS, 3))

    card = safe_render_card(
        countdown,
        f"{launch['label']} launch signals",
        rows=[("No earlier than", fmt_net(launch))],
        checklist=checklist,
        accent="go" if count == len(SIGNAL_LABELS) else "info",
        footer="Sources: City of Starbase, FAA, NGA, Launch Library 2",
    )
    return fit_lines(lines), card


# ---------------------------
# Runner
# ---------------------------

def run_flight_tracker() -> None:
    db = get_database()
    api = get_api_twitter()
    now = now_utc()

    launch = pick_tracked_launch(get_upcoming_starship_launches(db, now=now), now)
    if launch is None:
        logging.info("[TRACKER] No Starbase launch with a firm date in the tracking window.")
        return

    signals = collect_signals(db, launch, now)
    signature = make_signature(launch, signals)
    coll = db[TRACKER_COLLECTION]
    state = coll.find_one({"launch_id": launch["id"]})

    if state and state.get("signature") == signature:
        logging.info(f"[TRACKER] {launch['label']}: nothing changed.")
        return
    last_post = aware(state.get("posted_at")) if state else None
    if last_post and now - last_post < MIN_POST_INTERVAL:
        logging.info(f"[TRACKER] {launch['label']}: changed, but last post is too recent. Waiting.")
        return

    text, card = build_tracker_post(launch, signals, state, now=now)
    # Les réponses sont peu distribuées sur X : un changement important part en post normal,
    # seul un simple changement de statut répond au dernier post du vol
    major = is_major_change(launch, signals, state)
    reply_to = None if major else (state or {}).get("last_tweet_id")
    tweet_id = publish(text, image=card, reply_to=reply_to, api=api)
    coll.update_one(
        {"launch_id": launch["id"]},
        {"$set": {
            "launch_id": launch["id"],
            "label": launch["label"],
            "signature": signature,
            "net": launch["net"],
            "status": launch.get("status"),
            "signals": signals,
            "posted_at": now,
            "root_tweet_id": (state or {}).get("root_tweet_id") or tweet_id,
            "last_tweet_id": tweet_id or (state or {}).get("last_tweet_id"),
        }},
        upsert=True,
    )
    logging.info(f"[TRACKER] Posted {launch['label']} signature={signature} major={major} tweet_id={tweet_id}")


def preview() -> None:
    """Print the tracker post for the next launch. Reads the launch API only: signals are shown as missing."""
    now = now_utc()
    launches = get_upcoming_starship_launches(None, now=now)
    launch = pick_tracked_launch(launches, now)
    if launch is None:
        print(f"No Starbase launch with a firm date within {TRACK_WITHIN.days} days ({len(launches)} upcoming).")
        return
    text, _ = build_tracker_post(launch, {name: None for name in SIGNAL_LABELS}, now=now)
    print(f"--- {launch['name']} | {tweet_len(text)} chars\n{text}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Per-flight tracker of official launch signals.")
    parser.add_argument("--preview", action="store_true", help="Print the post instead of publishing.")
    args = parser.parse_args()
    try:
        preview() if args.preview else run_flight_tracker()
    except Exception as e:
        logging.error(f"[TRACKER][ERR] Failed to run flight tracker: {e}")
        sys.exit(1)
