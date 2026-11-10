"""
Print one example of every post the bot can publish, built from sample data.
No network, no MongoDB, no Twitter:  python -X utf8 src/preview_all.py [--cards DIR]
"""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta

from Digest import build_digest_post
from FlightTracker import build_tracker_post
from NavWarnings import build_warning_post
from Scraper_RC import build_post_card, build_post_text
from TFR import build_tfr_post
from Weekly import build_weekly_post
from utils.notify import tweet_len
from utils.timeparse import UTC

NOW = datetime(2026, 11, 10, 14, 0, tzinfo=UTC)
NET = datetime(2026, 11, 13, 22, 45, tzinfo=UTC)

BEACH = {
    "category": "beach", "status": "Beach closure", "description": "Boca Chica Beach",
    "date": "Nov. 13 8:00 AM to Nov. 13 8:00 PM", "backup_date": "Nov. 14 8:00 AM to Nov. 14 8:00 PM",
    "first_seen": NOW,
}
BEACH_MOVED = dict(BEACH, date="Nov. 14 8:00 AM to Nov. 14 8:00 PM", backup_date="Nov. 15 8:00 AM to Nov. 15 8:00 PM")
ROAD = {
    "category": "road", "status": "Road Delay", "description": "Production to Masseys",
    "date": "November 11 10:00 PM to November 12 4:30 AM", "backup_date": None, "first_seen": NOW,
}
TFR = {
    "notam_id": "6/7001", "location": "Brownsville, TX", "altitude": "From the surface up to unlimited",
    "begin": NET - timedelta(hours=1), "end": NET + timedelta(hours=3),
}
TFR_OLD = dict(TFR, notam_id="6/6990", begin=TFR["begin"] - timedelta(days=1), end=TFR["end"] - timedelta(days=1))
MARINE = {
    "reference": "NAVAREA IV 1187/26", "activity": "Rocket launching",
    "windows": [[NET + timedelta(days=d), NET + timedelta(days=d, hours=2, minutes=43)] for d in range(3)],
}
LAUNCH = {
    "id": "sample", "name": "Starship Flight 15", "label": "Starship Flight 15", "net": NET,
    "firm": True, "has_time": True, "status": "Go", "status_name": "Go for Launch", "at_starbase": True,
}
NO_SIGNALS = {"beach": None, "tfr": None, "marine": None}
WEEK_STATS = {
    "beach_new": 2, "beach_cancelled": 1, "road_new": 6, "road_cancelled": 0,
    "tfr_new": 1, "tfr_removed": 0, "marine_new": 1, "marine_removed": 0,
}


def examples():
    during = datetime(2026, 11, 13, 18, 0, tzinfo=UTC)
    for event, record, previous, now in (
        ("new", BEACH, None, NOW),
        ("updated", BEACH_MOVED, BEACH, NOW),
        ("cancelled", BEACH, None, NOW),
        ("lifted", BEACH, None, during),
        ("new", ROAD, None, NOW),
        ("cancelled", ROAD, None, NOW),
    ):
        yield (
            f"access-{record['category']}-{event}",
            build_post_text(event, record, previous=previous, now=now),
            build_post_card(event, record, previous=previous, now=now),
        )
    for event in ("new", "updated", "removed"):
        yield (f"tfr-{event}", *build_tfr_post(event, TFR, previous=TFR_OLD, now=NOW))
    for event in ("new", "updated", "removed"):
        yield (f"marine-{event}", *build_warning_post(event, MARINE, previous={"reference": "NAVAREA IV 1180/26"}, now=NOW))

    yield ("tracker-first-post", *build_tracker_post(LAUNCH, NO_SIGNALS, now=NOW))
    state = {"net": NET, "signals": NO_SIGNALS, "status": "Go"}
    yield ("tracker-signal-reply", *build_tracker_post(LAUNCH, dict(NO_SIGNALS, beach="Nov 13"), state, now=NOW))
    all_in = {"beach": "Nov 14", "tfr": "FDC 6/7001", "marine": "NAVAREA IV 1187/26"}
    moved = dict(LAUNCH, net=NET + timedelta(days=1))
    yield ("tracker-date-moved-reply", *build_tracker_post(moved, all_in, dict(state, signals=all_in), now=NOW))

    yield ("digest-quiet-day", *build_digest_post([], [], [], LAUNCH, now=NOW))
    yield ("digest-busy-day", *build_digest_post([BEACH, ROAD], [TFR], [MARINE], LAUNCH, now=NOW))
    yield ("weekly-recap", *build_weekly_post(WEEK_STATS, LAUNCH, NOW - timedelta(days=7), NOW, now=NOW))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Print an example of every post type.")
    parser.add_argument("--cards", default=None, help="Directory where the image cards are written.")
    args = parser.parse_args()
    if args.cards:
        os.makedirs(args.cards, exist_ok=True)
    for name, text, card in examples():
        print(f"=== {name} | {tweet_len(text)} chars\n{text}\n")
        if args.cards and card:
            with open(os.path.join(args.cards, f"{name}.png"), "wb") as f:
                f.write(card)
