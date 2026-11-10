from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple

from utils.notify import publish
from utils.timeparse import aware

# Passages consécutifs sans l'entrée avant de la considérer retirée de la source
MISSING_CONFIRM_RUNS = 3


def plan_tracked(
    current: Dict[str, Dict[str, Any]],
    stored: Dict[str, Dict[str, Any]],
    now: datetime,
    confirm_runs: int = MISSING_CONFIRM_RUNS,
) -> List[Dict[str, Any]]:
    """
    Compare the items a source lists now (by id) with the stored ones:
      new      never seen
      updated  new item appearing in the run where an active one vanished (reissued notice)
      removed  active item gone before its 'end': worth a post
      expired  active item gone after its 'end' (or with no known end): silent
      seen / missing  bookkeeping only
    Pure function, no I/O.
    """
    missing_active = [doc for key, doc in stored.items() if doc.get("active") and key not in current]
    replaced: List[int] = []
    actions: List[Dict[str, Any]] = []

    for key, item in current.items():
        if key in stored:
            actions.append({"type": "seen", "key": key, "item": item, "previous": stored[key]})
            continue
        previous = next((d for d in missing_active if id(d) not in replaced), None)
        if previous is not None:
            replaced.append(id(previous))
            actions.append({"type": "updated", "key": key, "item": item, "previous": previous})
        else:
            actions.append({"type": "new", "key": key, "item": item, "previous": None})

    for doc in missing_active:
        if id(doc) in replaced:
            continue
        key = doc["uniq_key"]
        missing_count = int(doc.get("missing_count") or 0) + 1
        end = aware(doc.get("end"))
        if missing_count < confirm_runs:
            actions.append({"type": "missing", "key": key, "item": doc, "missing_count": missing_count})
        elif end is not None and end > now:
            actions.append({"type": "removed", "key": key, "item": doc})
        else:
            actions.append({"type": "expired", "key": key, "item": doc})
    return actions


def sync_tracked(
    db,
    collection_name: str,
    current: Dict[str, Dict[str, Any]],
    now: datetime,
    build_post: Callable[[str, Dict[str, Any], Optional[Dict[str, Any]]], Tuple[str, Optional[bytes]]],
    label: str,
    api=None,
) -> Tuple[int, int]:
    """
    Run plan_tracked against a Mongo collection and post new / updated / removed items.
    `build_post(event, item, previous)` returns (text, image). State is only saved once the
    post went out, so a failed post is retried on the next run.
    """
    coll = db[collection_name]
    stored = {doc["uniq_key"]: doc for doc in coll.find({}) if doc.get("uniq_key")}
    posted = failures = 0

    def update(key: str, fields: Dict[str, Any]) -> None:
        coll.update_one({"uniq_key": key}, {"$set": {"uniq_key": key, **fields}}, upsert=True)

    for action in plan_tracked(current, stored, now):
        kind, key, item = action["type"], action["key"], action["item"]
        try:
            if kind == "seen":
                update(key, {**item, "active": True, "last_seen": now, "missing_count": 0})
            elif kind == "missing":
                update(key, {"missing_count": action["missing_count"]})
            elif kind == "expired":
                update(key, {"active": False, "missing_count": 0})
            elif kind in ("new", "updated"):
                previous = action["previous"]
                text, image = build_post(kind, item, previous)
                tweet_id = publish(text, image=image, api=api)
                update(key, {
                    **item,
                    "active": True,
                    "first_seen": now,
                    "last_seen": now,
                    "missing_count": 0,
                    "tweet_id": tweet_id,
                })
                if previous is not None:
                    update(previous["uniq_key"], {"active": False, "superseded_by": key})
                posted += 1
                logging.info(f"[{label}] Posted {kind} {key} tweet_id={tweet_id}")
            elif kind == "removed":
                text, image = build_post("removed", item, None)
                tweet_id = publish(text, image=image, api=api)
                update(key, {"active": False, "removed_at": now, "removed_tweet_id": tweet_id})
                posted += 1
                logging.info(f"[{label}] Posted removed {key} tweet_id={tweet_id}")
        except Exception as e:
            failures += 1
            logging.error(f"[{label}][ERR] Failed to apply {kind} {key}: {e}")
    return posted, failures


def get_active(db, collection_name: str, now: datetime) -> List[Dict[str, Any]]:
    """Active items whose 'end' is unknown or still in the future."""
    items = []
    for doc in db[collection_name].find({"active": True}):
        end = aware(doc.get("end"))
        if end is None or end > now:
            items.append(doc)
    return items
