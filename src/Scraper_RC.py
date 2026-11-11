from __future__ import annotations

import re
import sys
import logging
from datetime import datetime, timedelta
from typing import Iterable, Optional, Tuple, List, Dict, Any

from utils.utils import get_database, get_api_twitter
from utils.notify import publish, report_failure, report_success, fit_lines
from utils.cards import safe_render_card
from utils.timeparse import (
    Window,
    aware,
    fmt_duration,
    fmt_local,
    fmt_utc,
    now_utc,
    parse_window,
    parse_windows,
    relative_label,
)
import pandas as pd
from bs4 import BeautifulSoup
import requests

logging.basicConfig(level=logging.INFO)


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0 Safari/537.36"
)
ACCESS_URL = "https://www.starbase.texas.gov/beach-road-access"
SNAPSHOT_COLLECTION = "PageSnapshots"
SNAPSHOT_MAX_CHARS = 500_000

# ---------------------------
# HTTP
# ---------------------------

def get_html(url: str, timeout: int = 20) -> str:
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.text


class PageStructureError(RuntimeError):
    """The access page no longer has the blocks we scrape (site redesign, error page...)."""


def check_page_structure(html: str) -> None:
    """
    Raise if the road/beach blocks are missing. Without this check a redesign would look
    like 'nothing listed' forever, and every active closure would be reported as cancelled.
    A block is either a list of notices (.cms-big-text) or the site's own "nothing listed"
    state (.w-dyn-empty: "Boca Chica Beach is open." / "No Road Delay.").
    """
    soup = BeautifulSoup(html, "html.parser")
    missing = [
        block
        for block in ("#road-closure", "#beach-closure")
        if soup.select_one(f"{block} .cms-big-text, {block} .w-dyn-empty") is None
    ]
    if missing:
        raise PageStructureError(f"Expected blocks not found on {ACCESS_URL}: {', '.join(missing)}")


# ---------------------------
# Parsing helpers
# ---------------------------

def _iter_description_date_pairs(text_block: str) -> Iterable[Tuple[Optional[str], str]]:
    """
    Yield (description | None, date_range_string) pairs.
    If a 'Date:' line appears without a preceding 'Description:', description is None.
    """
    lines = [ln.strip() for ln in text_block.splitlines() if ln.strip()]
    desc: Optional[str] = None
    for ln in lines:
        lower = ln.lower()
        if lower.startswith("description:"):
            desc = ln.split(":", 1)[1].strip() or None
        elif lower.startswith("date:"):
            date_str = ln.split(":", 1)[1].strip()
            yield (desc, date_str)
            desc = None


def _rich_text_lines(rich) -> List[str]:
    for br in rich.find_all("br"):
        br.replace_with("\n")
    return [ln.strip() for ln in rich.get_text("\n", strip=True).splitlines() if ln.strip()]


def _iter_label_value_pairs(lines: Iterable[str]) -> Iterable[Tuple[str, str]]:
    for line in lines:
        if ":" not in line:
            continue
        label, value = line.split(":", 1)
        value = value.strip()
        if value:
            yield label.strip(), value


def _is_empty_access_status(text: Optional[str]) -> bool:
    s = _norm(text)
    return not s or s.startswith("no ") or "no beach closures" in s or "no road delays" in s


def extract_road_updates(
    html: str,
    source_url: Optional[str] = None,
) -> pd.DataFrame:
    """
    Extract road closures/delays in simple columns: category, status, description, date, source_url.
    Only the road list is read: the scrolling banner of the site has carried test text ("test",
    "delay") and beach dates in the past, which were posted as road closures.
    """
    soup = BeautifulSoup(html, "html.parser")
    records: List[dict] = []

    for item in soup.select("#road-closure .cms-item-2"):
        status_el = item.select_one(".cms-big-text")
        status = status_el.get_text(strip=True) if status_el else "Road Update"
        if _is_empty_access_status(status):
            continue

        rich = item.select_one(".cms-small-text-api")
        if not rich:
            continue

        block_text = "\n".join(_rich_text_lines(rich))

        for description, date_str in _iter_description_date_pairs(block_text):
            final_desc = description if description is not None else "Unknown description"
            records.append(
                {
                    "category": "road",
                    "status": status,
                    "description": final_desc,
                    "date": date_str,
                    "backup_date": None,
                    "source_url": source_url,
                }
            )

    df = pd.DataFrame.from_records(records)
    if not df.empty:
        df = df.sort_values(["date"]).reset_index(drop=True)
    return df


_BACKUP_LABELS = {"backup", "alternate", "alternate date", "alternate dates"}


def extract_beach_access_status(
    html: str,
    source_url: Optional[str] = None,
) -> pd.DataFrame:
    """
    Extract the BEACH Access Status block as one event per closure ("Primary:" line).
    A backup window stays in the record of its primary to avoid posting two near-duplicates.
    """
    soup = BeautifulSoup(html, "html.parser")
    records: List[dict] = []

    for item in soup.select("#beach-closure .w-dyn-item"):
        status_el = item.select_one(".cms-big-text")
        raw_status = status_el.get_text(" ", strip=True) if status_el else "Beach Access Status"
        if _is_empty_access_status(raw_status):
            continue

        rich = item.select_one(".cms-small-text-api")
        if not rich:
            continue

        # Un bloc peut lister plusieurs fermetures : chaque "Primary:" en ouvre une nouvelle,
        # un "Backup:" se rattache à la fermeture qui le précède
        closures: List[Tuple[str, List[str]]] = []
        orphan_backups: List[str] = []
        for label, value in _iter_label_value_pairs(_rich_text_lines(rich)):
            label_norm = _norm(label)
            if label_norm == "primary" or ("date" in label_norm and not closures and label_norm not in _BACKUP_LABELS):
                closures.append((value, []))
            elif label_norm in _BACKUP_LABELS:
                (closures[-1][1] if closures else orphan_backups).append(value)

        if not closures and orphan_backups:
            closures.append((orphan_backups.pop(0), orphan_backups))
        elif closures and orphan_backups:
            closures[0][1][:0] = orphan_backups

        for primary, backup_dates in closures:
            records.append(
                {
                    "category": "beach",
                    "status": "Beach Closure" if "closure" in _norm(raw_status) else "Beach Access Status",
                    "description": raw_status.rstrip(".") or "Boca Chica Beach access",
                    "date": primary,
                    "backup_date": " | ".join(backup_dates) if backup_dates else None,
                    "source_url": source_url,
                }
            )

    df = pd.DataFrame.from_records(records)
    if not df.empty:
        df = df.sort_values(["date"]).reset_index(drop=True)
    return df


def extract_access_updates(
    html: str,
    source_url: Optional[str] = None,
) -> pd.DataFrame:
    """
    Extract high-signal beach closures first, then road updates.
    """
    frames = [
        extract_beach_access_status(html, source_url=source_url),
        extract_road_updates(html, source_url=source_url),
    ]
    frames = [df for df in frames if not df.empty]
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    priority = {"beach": 0, "road": 1}
    df["_priority"] = df["category"].map(priority).fillna(9)
    return df.sort_values(["_priority", "date", "description"]).drop(columns=["_priority"]).reset_index(drop=True)


# ---------------------------
# De-dup vs Mongo (date + description)
# ---------------------------

_WS_RE = re.compile(r"\s+")
_URL_RE = re.compile(r"https?://\S+", re.IGNORECASE)

def _norm(s: Optional[str]) -> str:
    """Minuscule + espaces compactés + trim; None -> ''."""
    if s is None:
        return ""
    return _WS_RE.sub(" ", str(s)).strip().lower()

def _doc_to_uniq_key(doc: Dict[str, Any]) -> Optional[str]:
    """
    Reconstruit la uniq_key d’un doc Mongo historique.
    Priorité :
      1) champ 'uniq_key' si présent
      2) sinon 'date' + 'description' (normalisées)
    """
    if doc.get("uniq_key"):
        return str(doc["uniq_key"])
    if "date" in doc and "description" in doc:
        return _make_uniq_key(
            doc.get("date"),
            doc.get("description"),
            category=doc.get("category"),
            backup_date=doc.get("backup_date"),
        )
    return None

def _load_stored_docs(db, collection_name: str = "RoadClosure") -> Dict[str, Dict[str, Any]]:
    """
    Charge les docs existants depuis Mongo, indexés par uniq_key.
    """
    docs: Dict[str, Dict[str, Any]] = {}
    for doc in db[collection_name].find({}):
        k = _doc_to_uniq_key(doc)
        if k:
            docs[k] = doc
    return docs


def _make_uniq_key(
    date_str: Optional[str],
    desc: Optional[str],
    category: Optional[str] = None,
    backup_date: Optional[str] = None,
) -> str:
    """Unique key based on normalized event fields."""
    parts: List[str] = []
    category_norm = _norm(category)
    if category_norm and category_norm != "road":
        parts.append(category_norm)
    parts.extend([_norm(date_str), _norm(desc)])
    if backup_date:
        parts.append(_norm(backup_date))
    return "::".join(parts)


def _optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    text = str(value).strip()
    return text or None


def records_with_keys(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """DataFrame scrapé -> liste de dicts avec leur uniq_key (doublons de page ignorés)."""
    records: Dict[str, Dict[str, Any]] = {}
    if df.empty:
        return []
    for _, row in df.iterrows():
        record = {
            "category": _optional_text(row.get("category")) or "road",
            "status": row["status"],
            "description": row["description"],
            "date": row["date"],
            "backup_date": _optional_text(row.get("backup_date")),
            "source_url": row.get("source_url"),
        }
        record["uniq_key"] = _make_uniq_key(
            record["date"],
            record["description"],
            category=record["category"],
            backup_date=record["backup_date"],
        )
        records.setdefault(record["uniq_key"], record)
    return list(records.values())


# ---------------------------
# Change detection (new / updated / cancelled)
# ---------------------------

# Passages consécutifs sans l'entrée avant de la considérer retirée (évite les pages vides transitoires)
MISSING_CONFIRM_RUNS = 3
# Une entrée inactive revue après ce délai est une nouvelle closure (le site ne donne pas l'année)
STALE_AFTER = timedelta(days=30)


def record_windows(record: Dict[str, Any], now: datetime) -> List[Window]:
    """Primary + backup windows of a record, parsed relative to when it was first seen."""
    ref = aware(record.get("first_seen")) or now
    return parse_windows([record.get("date"), record.get("backup_date")], now=ref)


def _is_stale(doc: Dict[str, Any], now: datetime) -> bool:
    if doc.get("active"):
        return False
    if doc.get("cancelled_at"):
        return True
    last_seen = aware(doc.get("last_seen"))
    return bool(last_seen and now - last_seen > STALE_AFTER)


def _has_ended(doc: Dict[str, Any], now: datetime) -> bool:
    windows = record_windows(doc, now)
    return bool(windows) and max(end for _, end in windows) <= now


def _same_kind(doc: Dict[str, Any], record: Dict[str, Any]) -> bool:
    """Same category, and same road for road delays (beach notices all describe the same beach)."""
    category = _norm(record.get("category") or "road")
    if _norm(doc.get("category") or "road") != category:
        return False
    return category == "beach" or _norm(doc.get("description")) == _norm(record.get("description"))


def plan_changes(
    current: List[Dict[str, Any]],
    stored: Dict[str, Dict[str, Any]],
    now: datetime,
) -> List[Dict[str, Any]]:
    """
    Compare the scraped records with the stored state and return the actions to run:
      new        record never seen (or seen long ago / cancelled then re-listed)
      updated    record replacing an active one of the same kind (same road, or beach) that just vanished
      rekeyed    same dates as an active one that just vanished, only written differently: silent
      cancelled  active record gone from the page before its window ended
      seen       record already known: refresh it, nothing to post
      missing    active record absent, not yet confirmed as removed
      expired    active record gone after its window ended: nothing to post
    Pure function: no I/O, so it can be tested without Mongo.
    """
    current_keys = {r["uniq_key"] for r in current}
    missing_active = [
        doc for key, doc in stored.items() if doc.get("active") and key not in current_keys
    ]
    replaced: List[int] = []
    actions: List[Dict[str, Any]] = []

    def candidates(record: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [
            d for d in missing_active
            if id(d) not in replaced and _same_kind(d, record) and not _has_ended(d, now)
        ]

    for record in current:
        doc = stored.get(record["uniq_key"])
        if doc is not None and not _is_stale(doc, now):
            actions.append({"type": "seen", "record": record, "previous": doc})
            continue
        windows = sorted(parse_windows([record.get("date"), record.get("backup_date")], now=now))
        # Mêmes dates écrites autrement ("Nov. 13" -> "Nov 13", "closure" -> "closures") : rien à poster
        same = next((d for d in candidates(record) if windows and sorted(record_windows(d, now)) == windows), None)
        if same is not None:
            replaced.append(id(same))
            actions.append({"type": "rekeyed", "record": record, "previous": same})
            continue
        previous = next(iter(candidates(record)), None)
        if previous is not None:
            replaced.append(id(previous))
            actions.append({"type": "updated", "record": record, "previous": previous})
        else:
            actions.append({"type": "new", "record": record, "previous": None})

    for doc in missing_active:
        if id(doc) in replaced:
            continue
        missing_count = int(doc.get("missing_count") or 0) + 1
        if missing_count < MISSING_CONFIRM_RUNS:
            actions.append({"type": "missing", "record": doc, "missing_count": missing_count})
            continue
        windows = record_windows(doc, now)
        if windows and max(end for _, end in windows) > now:
            in_progress = any(start <= now < end for start, end in windows)
            actions.append({"type": "cancelled", "record": doc, "in_progress": in_progress})
        else:
            # Fenêtre terminée, ou date illisible : on ne peut pas affirmer une annulation
            actions.append({"type": "expired", "record": doc})
    return actions


# ---------------------------
# Post helpers
# ---------------------------

RC_SOURCE_LABEL = "Source: City of Starbase"
RC_HASHTAGS = "#Starbase #SpaceX"

_EVENT_TAGS = {"new": "NEW", "updated": "UPDATED", "cancelled": "CANCELLED", "lifted": "LIFTED EARLY"}
_EVENT_EMOJI = {"updated": "\U0001f504", "cancelled": "❌", "lifted": "✅"}


def _clean_tweet_part(value: Optional[str], fallback: str = "") -> str:
    text = _URL_RE.sub("", str(value or ""))
    text = _WS_RE.sub(" ", text).strip(" \t\r\n-|")
    return text or fallback


def _status_kind(status: str) -> str:
    s = _norm(status)
    if any(word in s for word in ("closure", "closed", "fermeture", "fermee", "ferme")):
        return "closure"
    if any(word in s for word in ("delay", "restriction", "slowdown")):
        return "delay"
    return "update"


def _headline(record: Dict[str, Any]) -> str:
    if _norm(record.get("category")) == "beach":
        return "Boca Chica Beach closure"
    return f"Starbase road {_status_kind(record.get('status') or '')}"


def _format_description(description: Optional[str]) -> str:
    desc = _clean_tweet_part(description)
    if not desc or desc.lower() == "unknown description":
        return "Boca Chica / SH-4 access"
    return desc


def _context_line(record: Dict[str, Any]) -> str:
    """One line of interpretation; worded as likelihood because the city never states the reason."""
    if _norm(record.get("category")) == "beach":
        return "Beach closures usually signal testing or a launch attempt."
    desc = _norm(record.get("description"))
    if "massey" in desc:
        return "Likely vehicle transport to/from the Massey's test site."
    if "production" in desc and ("pad" in desc or "launch" in desc):
        return "Likely vehicle transport between the build site and the launch site."
    return ""


def _fmt_window_local(text: Optional[str], ref: datetime) -> str:
    """Local-time line for a raw window string, raw text when it cannot be parsed."""
    window = parse_window(text, now=ref)
    if window:
        return fmt_local(window)
    return f"{_clean_tweet_part(text, 'Schedule to be confirmed')} (local time)"


def _split_backups(backup_date: Optional[str]) -> List[str]:
    return [b.strip() for b in str(backup_date or "").split("|") if b.strip()]


def _pending_window(record: Dict[str, Any], now: datetime) -> Optional[Window]:
    """First window (primary or backup) that has not ended yet."""
    pending = sorted(w for w in record_windows(record, now) if w[1] > now)
    return pending[0] if pending else None


def _fmt_was(record: Dict[str, Any], now: datetime) -> str:
    """The window a removed record was still announcing."""
    pending = _pending_window(record, now)
    if pending:
        return fmt_local(pending)
    return _fmt_window_local(record.get("date"), aware(record.get("first_seen")) or now)


def build_post_text(
    event: str,
    record: Dict[str, Any],
    previous: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> str:
    """
    Single recognisable format for every access post:
    tag + headline, what, when (local then UTC), countdown, context, source, hashtags.
    Optional lines are dropped (least important first) when the post is too long.
    """
    now = now or now_utc()
    is_beach = _norm(record.get("category")) == "beach"
    ref = aware(record.get("first_seen")) or now
    emoji = _EVENT_EMOJI.get(event) or ("\U0001f3d6️" if is_beach else "\U0001f6a7")
    lines: List[tuple] = [(f"{emoji} {_EVENT_TAGS[event]} · {_headline(record)}", 0)]
    if not is_beach:
        lines.append((_format_description(record.get("description")), 0))

    primary = parse_window(record.get("date"), now=ref)
    backups = _split_backups(record.get("backup_date"))

    if event in ("cancelled", "lifted"):
        pending = _pending_window(record, now)
        lines.append((f"Was: {_fmt_was(record, now)}", 0))
        if event == "lifted" and pending:
            ahead = fmt_duration(pending[1] - now)
            lines.append((f"Removed from the city's list {ahead} before the scheduled end.", 1))
        else:
            lines.append(("Removed from the city's list before the window started.", 1))
        if is_beach:
            lines.append(("Can mean the planned test or launch attempt has moved.", 5))
    else:
        label = "Primary: " if is_beach and backups else ""
        lines.append((f"\U0001f552 {label}{_fmt_window_local(record.get('date'), ref)}", 0))
        for backup in backups:
            lines.append((f"\U0001f552 Backup: {_fmt_window_local(backup, ref)}", 1))
        if primary:
            lines.append((f"\U0001f310 {fmt_utc(primary)}", 2))
            if now < primary[1]:
                lines.append((f"⏳ {relative_label(primary, now).capitalize()}", 4))
        if event == "updated" and previous is not None:
            prev_ref = aware(previous.get("first_seen")) or now
            if _norm(previous.get("date")) != _norm(record.get("date")):
                lines.append((f"Was: {_fmt_window_local(previous.get('date'), prev_ref)}", 1))
            else:
                old_backups = _split_backups(previous.get("backup_date"))
                was = " | ".join(_fmt_window_local(b, prev_ref) for b in old_backups) or "none"
                lines.append((f"Backup was: {was}", 1))
        elif event == "new":
            lines.append((_context_line(record), 5))

    lines.append((RC_SOURCE_LABEL, 1))
    lines.append((RC_HASHTAGS, 3))
    return fit_lines(lines)


def build_post_card(
    event: str,
    record: Dict[str, Any],
    previous: Optional[Dict[str, Any]] = None,
    now: Optional[datetime] = None,
) -> Optional[bytes]:
    now = now or now_utc()
    is_beach = _norm(record.get("category")) == "beach"
    ref = aware(record.get("first_seen")) or now
    title = _headline(record) if is_beach else _format_description(record.get("description"))
    primary = parse_window(record.get("date"), now=ref)

    rows: List[Tuple[str, str]] = []
    if not is_beach:
        rows.append(("What", _headline(record)))
    if event in ("cancelled", "lifted"):
        rows.append(("Was scheduled", _fmt_was(record, now)))
        rows.append(("Status", "Removed from the City of Starbase list"))
    else:
        rows.append(("Local time", _fmt_window_local(record.get("date"), ref)))
        if primary:
            rows.append(("UTC", fmt_utc(primary)))
        for backup in _split_backups(record.get("backup_date")):
            rows.append(("Backup", _fmt_window_local(backup, ref)))
    if event == "updated" and previous is not None:
        rows.append(("Previously", _fmt_window_local(previous.get("date"), aware(previous.get("first_seen")) or now)))

    accent = "cancelled" if event in ("cancelled", "lifted") else event
    return safe_render_card(_EVENT_TAGS[event], title, rows=rows[:5], accent=accent, footer=RC_SOURCE_LABEL)


# ---------------------------
# Mongo : état des RC
# ---------------------------

def _update_rc(db, uniq_key: str, fields: Dict[str, Any], collection_name: str = "RoadClosure") -> None:
    db[collection_name].update_one({"uniq_key": uniq_key}, {"$set": {"uniq_key": uniq_key, **fields}}, upsert=True)


def _save_new_rc_to_mongo(
    db,
    record: Dict[str, Any],
    tweet_id: Optional[str],
    now: datetime,
    collection_name: str = "RoadClosure",
) -> None:
    """
    Upsert idempotent d'une RC (via uniq_key). On stocke également tweet_id si présent.
    """
    try:
        db[collection_name].create_index("uniq_key", unique=True)
    except Exception:
        pass

    _update_rc(
        db,
        record["uniq_key"],
        {
            "category": record.get("category", "road"),
            "status": record["status"],
            "description": record["description"],
            "date": record["date"],               # string telle que scrapée
            "backup_date": _optional_text(record.get("backup_date")),
            "source_url": record.get("source_url"),
            "tweet_id": tweet_id,
            "active": True,
            "first_seen": now,
            "last_seen": now,
            "missing_count": 0,
            "cancelled_at": None,
        },
        collection_name=collection_name,
    )


def get_active_closures(db, now: Optional[datetime] = None) -> List[Dict[str, Any]]:
    """Active closures whose window has not ended yet (used by the digest and flight tracker)."""
    now = now or now_utc()
    active = []
    for doc in db["RoadClosure"].find({"active": True}):
        windows = record_windows(doc, now)
        if not windows or max(end for _, end in windows) > now:
            active.append(doc)
    return active


# ---------------------------
# Runner
# ---------------------------

def apply_actions(db, api, actions: List[Dict[str, Any]], now: datetime) -> Tuple[int, int]:
    posted = 0
    failures = 0

    for action in actions:
        kind, record = action["type"], action["record"]
        key = record.get("uniq_key") or _doc_to_uniq_key(record)
        try:
            if kind == "seen":
                previous = action["previous"]
                fields: Dict[str, Any] = {"active": True, "last_seen": now, "missing_count": 0}
                if not previous.get("first_seen"):
                    fields["first_seen"] = now
                _update_rc(db, key, fields)
            elif kind == "missing":
                _update_rc(db, key, {"missing_count": action["missing_count"]})
            elif kind == "expired":
                _update_rc(db, key, {"active": False, "missing_count": 0})
            elif kind == "rekeyed":
                previous = action["previous"]
                _save_new_rc_to_mongo(db, record, tweet_id=previous.get("tweet_id"), now=now)
                _update_rc(db, key, {"first_seen": aware(previous.get("first_seen")) or now})
                _update_rc(db, _doc_to_uniq_key(previous), {"active": False, "superseded_by": key})
                logging.info(f"[RC] Same closure written differently, nothing posted: uniq_key={key}")
            elif kind in ("new", "updated"):
                previous = action.get("previous")
                tweet_id = publish(
                    build_post_text(kind, record, previous=previous, now=now),
                    image=build_post_card(kind, record, previous=previous, now=now),
                    api=api,
                )
                _save_new_rc_to_mongo(db, record, tweet_id=tweet_id, now=now)
                if previous is not None:
                    _update_rc(db, _doc_to_uniq_key(previous), {"active": False, "superseded_by": key})
                posted += 1
                logging.info(f"[RC] Posted {kind} category={record.get('category', 'road')} uniq_key={key} tweet_id={tweet_id}")
            elif kind == "cancelled":
                event = "lifted" if action.get("in_progress") else "cancelled"
                tweet_id = publish(
                    build_post_text(event, record, now=now),
                    image=build_post_card(event, record, now=now),
                    api=api,
                )
                _update_rc(db, key, {"active": False, "cancelled_at": now, "cancel_tweet_id": tweet_id})
                posted += 1
                logging.info(f"[RC] Posted {event} uniq_key={key} tweet_id={tweet_id}")
        except Exception as e:
            # Pas d'enregistrement en base : l'action sera retentée au prochain passage
            failures += 1
            logging.error(f"[RC][ERR] Failed to apply {kind} uniq_key={key}: {e}")
    return posted, failures


def save_page_snapshot(db, html: str, now: datetime) -> None:
    """Keep the last page that failed the structure check (one Mongo document), to see what the site served."""
    try:
        text = " ".join(BeautifulSoup(html, "html.parser").get_text(" ").split())
        logging.warning(f"[RC] Unexpected page ({len(html)} chars). Starts with: {text[:300]}")
        db[SNAPSHOT_COLLECTION].update_one(
            {"key": "rc-structure"},
            {"$set": {"key": "rc-structure", "url": ACCESS_URL, "saved_at": now, "html": html[:SNAPSHOT_MAX_CHARS]}},
            upsert=True,
        )
    except Exception as e:
        logging.warning(f"[RC] Could not save the page snapshot: {e}")


def run_RC_checks() -> None:
    db = get_database()
    api = get_api_twitter()
    now = now_utc()

    try:
        html = get_html(ACCESS_URL)
    except Exception as e:
        report_failure(db, "rc-fetch", f"Cannot fetch {ACCESS_URL}: {e}")
        raise
    try:
        check_page_structure(html)
    except PageStructureError:
        # Pas d'alerte ici : la page se vide parfois quelques minutes. L'alerte Airflow part
        # si ça dure (voir FAILURES_BEFORE_ALERT dans airflow_dags/alerting.py).
        save_page_snapshot(db, html, now)
        raise
    report_success(db, "rc-fetch")

    current = records_with_keys(extract_access_updates(html, source_url=ACCESS_URL))
    actions = plan_changes(current, _load_stored_docs(db, "RoadClosure"), now)

    posted, failures = apply_actions(db, api, actions, now)
    logging.info(f"[RC] Done. Listed: {len(current)}, Posted: {posted}, Failed: {failures}.")
    if failures:
        report_failure(db, "rc-post", f"{failures} access post(s) failed, see the task log.")
    else:
        report_success(db, "rc-post")


if __name__ == "__main__":
    try:
        run_RC_checks()
    except Exception as e:
        logging.error(f"[RC][ERR] Failed to run RC checks: {e}")
        # Code retour non nul : la tâche Airflow passe en échec au lieu d'un faux succès
        sys.exit(1)


