from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import requests

from utils.timeparse import UTC, aware, now_utc

# Launch Library 2 (The Space Devs). Accès gratuit limité à 15 requêtes/heure : on met en cache dans Mongo.
LL2_UPCOMING_URL = "https://ll.thespacedevs.com/2.3.0/launches/upcoming/"
CACHE_COLLECTION = "Cache"
CACHE_KEY = "ll2-starship-upcoming"
CACHE_TTL = timedelta(minutes=20)

# Précisions LL2 pour lesquelles la date vaut la peine d'être annoncée
_FIRM_PRECISIONS = {"second", "minute", "hour", "day"}
_FLIGHT_RE = re.compile(r"Flight\s+(\d+)", re.IGNORECASE)


def _parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def simplify_launch(raw: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    net = _parse_iso(raw.get("net"))
    if net is None:
        return None
    name = str(raw.get("name") or "Starship")
    flight = _FLIGHT_RE.search(name)
    pad = raw.get("pad") or {}
    location = (pad.get("location") or {}).get("name") or ""
    precision = str((raw.get("net_precision") or {}).get("name") or "").lower()
    status = raw.get("status") or {}
    return {
        "id": raw.get("id"),
        "name": name,
        "label": f"Starship Flight {flight.group(1)}" if flight else name.split("|")[0].strip() or "Starship",
        "net": net,
        "precision": precision,
        "firm": precision in _FIRM_PRECISIONS,
        "has_time": precision in {"second", "minute", "hour"},
        "status": status.get("abbrev") or "",
        "status_name": status.get("name") or "",
        "pad": pad.get("name") or "",
        "location": location,
        "at_starbase": "starbase" in location.lower(),
    }


def _fetch_upcoming() -> List[Dict[str, Any]]:
    resp = requests.get(LL2_UPCOMING_URL, params={"search": "Starship", "limit": 10}, timeout=20)
    resp.raise_for_status()
    results = resp.json().get("results") or []
    launches = [simplify_launch(r) for r in results if str(r.get("name") or "").startswith("Starship")]
    return sorted((l for l in launches if l), key=lambda l: l["net"])


def get_upcoming_starship_launches(db=None, now: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Upcoming Starship launches, soonest first. Served from the Mongo cache when it is fresh."""
    now = now or now_utc()
    cached = None
    if db is not None:
        try:
            cached = db[CACHE_COLLECTION].find_one({"key": CACHE_KEY})
        except Exception as e:
            logging.warning(f"[LL2] Cache read failed: {e}")
    if cached and now - aware(cached["fetched_at"]) < CACHE_TTL:
        launches = cached["launches"]
    else:
        try:
            launches = _fetch_upcoming()
        except Exception as e:
            if not cached:
                raise
            # API en panne ou quota dépassé : mieux vaut des données un peu vieilles que rien
            logging.warning(f"[LL2] Fetch failed, using stale cache: {e}")
            launches = cached["launches"]
        else:
            if db is not None:
                try:
                    db[CACHE_COLLECTION].update_one(
                        {"key": CACHE_KEY},
                        {"$set": {"key": CACHE_KEY, "fetched_at": now, "launches": launches}},
                        upsert=True,
                    )
                except Exception as e:
                    logging.warning(f"[LL2] Cache write failed: {e}")
    for launch in launches:
        launch["net"] = aware(launch["net"])
    # L'endpoint "upcoming" garde les lancements des dernières 24 h
    return [l for l in launches if l["net"] > now]


def get_next_starship_launch(db=None, now: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
    launches = get_upcoming_starship_launches(db, now=now)
    return launches[0] if launches else None
