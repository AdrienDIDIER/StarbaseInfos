from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from Digest import launch_line
from NavWarnings import NAV_COLLECTION
from TFR import TFR_COLLECTION
from utils.cards import safe_render_card
from utils.launches import get_upcoming_starship_launches
from utils.notify import fit_lines, publish, tweet_len
from utils.timeparse import STARBASE_TZ, aware, fmt_day, now_utc
from utils.utils import get_database, get_api_twitter

logging.basicConfig(level=logging.INFO)

WEEKLY_COLLECTION = "Weekly"
WEEKLY_HASHTAGS = "#Starbase #Starship"
WEEK = timedelta(days=7)
# En dessous de ce nombre d'événements sur 7 jours, la semaine ne mérite pas de récap : on ne poste rien
MIN_EVENTS = 3


# ---------------------------
# Stats
# ---------------------------

def _in_week(value: Any, start: datetime, end: datetime) -> bool:
    dt = aware(value)
    return dt is not None and start <= dt < end


def week_stats(
    closures: List[Dict[str, Any]],
    tfrs: List[Dict[str, Any]],
    warnings: List[Dict[str, Any]],
    start: datetime,
    end: datetime,
) -> Dict[str, int]:
    """Count what was announced / withdrawn between start and end. Pure function, no I/O."""
    stats = {
        "beach_new": 0, "beach_cancelled": 0,
        "road_new": 0, "road_cancelled": 0,
        "tfr_new": 0, "tfr_removed": 0,
        "marine_new": 0, "marine_removed": 0,
    }
    for doc in closures:
        kind = "beach" if str(doc.get("category") or "road").lower() == "beach" else "road"
        # Une fermeture reportée ou réécrite vit dans son successeur : on ne la compte qu'une fois
        if _in_week(doc.get("first_seen"), start, end) and not doc.get("superseded_by"):
            stats[f"{kind}_new"] += 1
        if _in_week(doc.get("cancelled_at"), start, end):
            stats[f"{kind}_cancelled"] += 1
    for name, docs in (("tfr", tfrs), ("marine", warnings)):
        for doc in docs:
            if _in_week(doc.get("first_seen"), start, end) and not doc.get("superseded_by"):
                stats[f"{name}_new"] += 1
            if _in_week(doc.get("removed_at"), start, end):
                stats[f"{name}_removed"] += 1
    return stats


def _count_line(posted: int, withdrawn: int, withdrawn_word: str) -> str:
    parts = []
    if posted:
        parts.append(f"{posted} posted")
    if withdrawn:
        parts.append(f"{withdrawn} {withdrawn_word}")
    return ", ".join(parts)


def build_weekly_post(
    stats: Dict[str, int],
    launch: Optional[Dict[str, Any]],
    start: datetime,
    end: datetime,
    now: Optional[datetime] = None,
) -> Optional[Tuple[str, Optional[bytes]]]:
    """(text, card) for the week, or None when too little happened to be worth a post."""
    now = now or now_utc()
    if sum(stats.values()) < MIN_EVENTS:
        return None

    last_day = (end - timedelta(seconds=1)).astimezone(STARBASE_TZ)
    period = f"{fmt_day(start.astimezone(STARBASE_TZ))} → {fmt_day(last_day)}"
    rows = [
        ("\U0001f3d6️", "Beach closures", _count_line(stats["beach_new"], stats["beach_cancelled"], "cancelled")),
        ("\U0001f6a7", "Road delays", _count_line(stats["road_new"], stats["road_cancelled"], "cancelled")),
        ("✈️", "Airspace TFRs", _count_line(stats["tfr_new"], stats["tfr_removed"], "removed")),
        ("⚓", "Marine hazard areas", _count_line(stats["marine_new"], stats["marine_removed"], "cancelled")),
    ]
    # Les lignes à zéro sont omises : le récap ne montre que ce qui s'est passé
    rows = [row for row in rows if row[2]]

    lines: List[tuple] = [(f"\U0001f4ca Starbase week · {period}", 0)]
    lines += [(f"{emoji} {label}: {value}", 0) for emoji, label, value in rows]
    card_rows = [(label, value) for _, label, value in rows]
    if launch is not None:
        lines.append((f"\U0001f680 Next: {launch_line(launch, now, compact=True)}", 1))
        card_rows.append(("Next launch", launch_line(launch, now)))
    lines.append((WEEKLY_HASHTAGS, 3))

    card = safe_render_card(
        period, "Starbase week in review", rows=card_rows[:5], accent="info",
        footer="Sources: City of Starbase, FAA, NGA, Launch Library 2",
    )
    return fit_lines(lines), card


# ---------------------------
# Runner
# ---------------------------

def run_weekly() -> None:
    db = get_database()
    api = get_api_twitter()
    now = now_utc()
    start = now - WEEK

    # Un seul récap par semaine ISO (date locale de Starbase), même si la tâche est relancée
    iso = now.astimezone(STARBASE_TZ).isocalendar()
    week_key = f"{iso[0]}-W{iso[1]:02d}"
    coll = db[WEEKLY_COLLECTION]
    if coll.find_one({"week": week_key}):
        logging.info(f"[WEEKLY] Already handled {week_key}.")
        return

    stats = week_stats(
        list(db["RoadClosure"].find({})),
        list(db[TFR_COLLECTION].find({})),
        list(db[NAV_COLLECTION].find({})),
        start, now,
    )
    try:
        launches = get_upcoming_starship_launches(db, now=now)
    except Exception as e:
        logging.warning(f"[WEEKLY] Launch date unavailable: {e}")
        launches = []

    post = build_weekly_post(stats, launches[0] if launches else None, start, now, now=now)
    if post is None:
        logging.info(f"[WEEKLY] Only {sum(stats.values())} event(s) this week (< {MIN_EVENTS}): no recap.")
        coll.update_one({"week": week_key}, {"$set": {"week": week_key, "skipped": True, "stats": stats}}, upsert=True)
        return

    text, card = post
    tweet_id = publish(text, image=card, api=api)
    coll.update_one(
        {"week": week_key},
        {"$set": {"week": week_key, "tweet_id": tweet_id, "posted_at": now, "stats": stats}},
        upsert=True,
    )
    logging.info(f"[WEEKLY] Posted recap for {week_key} tweet_id={tweet_id}")


def preview() -> None:
    """Print the recap for the last 7 days from the stored state. Reads Mongo and the launch API, posts nothing."""
    db = get_database()
    now = now_utc()
    stats = week_stats(
        list(db["RoadClosure"].find({})),
        list(db[TFR_COLLECTION].find({})),
        list(db[NAV_COLLECTION].find({})),
        now - WEEK, now,
    )
    try:
        launches = get_upcoming_starship_launches(None, now=now)
    except Exception as e:
        print(f"Launch date unavailable: {e}\n")
        launches = []
    post = build_weekly_post(stats, launches[0] if launches else None, now - WEEK, now, now=now)
    print(f"Stats: {stats}")
    if post is None:
        print(f"Fewer than {MIN_EVENTS} events this week: nothing would be posted.")
    else:
        print(f"--- {tweet_len(post[0])} chars\n{post[0]}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Weekly Starbase recap (skipped on quiet weeks).")
    parser.add_argument("--preview", action="store_true", help="Print the post instead of publishing.")
    args = parser.parse_args()
    try:
        preview() if args.preview else run_weekly()
    except Exception as e:
        logging.error(f"[WEEKLY][ERR] Failed to run weekly recap: {e}")
        sys.exit(1)
