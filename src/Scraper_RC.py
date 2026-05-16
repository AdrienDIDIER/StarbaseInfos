from __future__ import annotations

import re
import logging
from typing import Iterable, Optional, Tuple, List, Set, Dict, Any

from utils.utils import get_database, get_api_twitter
import hashlib
import pandas as pd
from bs4 import BeautifulSoup
import requests

logging.basicConfig(level=logging.INFO)


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/127.0 Safari/537.36"
)
ACCESS_URL = "https://www.starbase.texas.gov/beach-road-access"

# ---------------------------
# HTTP
# ---------------------------

def get_html(url: str, timeout: int = 20) -> str:
    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml"}
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.text


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

    if not records:
        bar = soup.select_one(".notification-bar #rich-bar-message")
        if bar:
            text = bar.get_text(" ", strip=True)
            for part in re.split(r"\s*\|\s*", text):
                part = part.strip()
                if not part:
                    continue
                records.append(
                    {
                        "category": "road",
                        "status": "Road Delay",
                        "description": "Road Notification",
                        "date": part,
                        "backup_date": None,
                        "source_url": source_url,
                    }
                )

    df = pd.DataFrame.from_records(records)
    if not df.empty:
        df = df.sort_values(["date"]).reset_index(drop=True)
    return df


def extract_beach_access_status(
    html: str,
    source_url: Optional[str] = None,
) -> pd.DataFrame:
    """
    Extract the BEACH Access Status block as one high-signal event per closure notice.
    Primary and backup windows stay in the same record to avoid posting two near-duplicates.
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

        primary: Optional[str] = None
        backup_dates: List[str] = []
        for label, value in _iter_label_value_pairs(_rich_text_lines(rich)):
            label_norm = _norm(label)
            if label_norm == "primary":
                primary = value
            elif label_norm in {"backup", "alternate", "alternate date", "alternate dates"}:
                backup_dates.append(value)
            elif "date" in label_norm and primary is None:
                primary = value

        if not primary and backup_dates:
            primary = backup_dates.pop(0)
        if not primary:
            continue

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

def _get_existing_uniq_keys(db, collection_name: str = "RoadClosure") -> Set[str]:
    """
    Charge les uniq_key existantes depuis Mongo (schema minimal).
    """
    coll = db[collection_name]
    cursor = coll.find({}, {"_id": 1, "uniq_key": 1, "date": 1, "description": 1, "category": 1, "backup_date": 1})
    keys: Set[str] = set()
    for doc in cursor:
        k = _doc_to_uniq_key(doc)
        if k:
            keys.add(k)
    return keys


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


# ---------------------------
# Tweet helpers
# ---------------------------

RC_TWEET_CHAR_LIMIT = 270
RC_SOURCE_LABEL = "Source: City of Starbase"
RC_HASHTAGS = "#Starbase #SpaceX"


def _clean_tweet_part(value: Optional[str], fallback: str = "") -> str:
    text = _URL_RE.sub("", str(value or ""))
    text = _WS_RE.sub(" ", text).strip(" \t\r\n-|")
    return text or fallback


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


def _status_kind(status: str) -> str:
    s = _norm(status)
    if any(word in s for word in ("closure", "closed", "fermeture", "fermee", "ferme")):
        return "closure"
    if any(word in s for word in ("delay", "restriction", "slowdown")):
        return "delay"
    return "update"


def _emoji_for_status(status: str) -> str:
    kind = _status_kind(status)
    if kind == "closure":
        return "\U0001f6a7"
    if kind == "delay":
        return "\u26a0\ufe0f"
    return "\U0001f6a6"


def _headline_for_status(kind: str) -> str:
    if kind == "closure":
        return "Starbase road closure"
    if kind == "delay":
        return "Starbase road update"
    return "Starbase access update"


def _status_label(status: str) -> str:
    label = _clean_tweet_part(status, "Road update")
    if label.lower() in {"road closure", "closure", "closed"}:
        return "Road closure"
    if label.lower() in {"road delay", "delay"}:
        return "Road delay"
    if label.isupper() or label.islower():
        return label.title()
    return label


def _format_description(description: str) -> str:
    desc = _clean_tweet_part(description)
    if not desc or desc.lower() == "unknown description":
        return "Boca Chica / SH-4 access"
    if "boca chica beach closures" in desc.lower():
        return "Boca Chica Beach access"
    return re.sub(r"\bproduction\s+to\s+pad\b", "Production Site -> Pad", desc, flags=re.IGNORECASE)


def _format_date_window(date_str: str) -> str:
    date = _clean_tweet_part(date_str, "Schedule to be confirmed")
    date = re.sub(r"\b([A-Z][a-z]{2})\.\s*", r"\1 ", date)
    date = re.sub(r"\s+to\s+", " -> ", date, flags=re.IGNORECASE)
    return date


def _impact_line(kind: str, description: str, category: str = "road") -> str:
    if category == "beach":
        return "Impact: Boca Chica Beach access is scheduled to close."
    desc = description.lower()
    if "production" in desc and "pad" in desc:
        return "Impact: SpaceX traffic may affect SH-4 access."
    if kind == "closure":
        return "Impact: Boca Chica / SH-4 access may be restricted."
    if kind == "delay":
        return "Impact: expect possible delays around Boca Chica."
    return "Impact: verify Boca Chica / SH-4 access before heading out."


def _truncate(text: str, max_len: int) -> str:
    if max_len <= 0:
        return ""
    if len(text) <= max_len:
        return text
    if max_len <= 1:
        return "\u2026"
    return text[: max_len - 1].rstrip() + "\u2026"


def _stable_index(seed: str, modulo: int) -> int:
    """
    Select a deterministic template based on sha1(uniq_key).
    """
    h = hashlib.sha1(seed.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % modulo


def _render_tweet_with_fit(template: str, values: Dict[str, str]) -> str:
    for desc_len, date_len, backup_len in ((130, 105, 95), (110, 95, 85), (90, 85, 75), (70, 75, 65), (50, 65, 55)):
        candidate_values = dict(values)
        candidate_values["DESC"] = _truncate(values["DESC"], desc_len)
        candidate_values["DATE"] = _truncate(values["DATE"], date_len)
        candidate_values["BACKUP"] = _truncate(values.get("BACKUP", ""), backup_len)
        candidate_values["BACKUP_LINE"] = (
            f"Backup: {candidate_values['BACKUP']}\n"
            if candidate_values["BACKUP"]
            else ""
        )
        tweet = template.format(**candidate_values).strip()
        if len(tweet) <= RC_TWEET_CHAR_LIMIT:
            return tweet

    backup = _truncate(values.get("BACKUP", ""), 70)
    backup_line = f"Backup: {backup}\n" if backup else ""
    fallback = (
        "{EMOJI} {HEADLINE}\n"
        "Window: {DATE}\n"
        "{BACKUP_LINE}"
        "{DESC}\n"
        "{SOURCE}\n"
        "{TAGS}"
    ).format(
        EMOJI=values["EMOJI"],
        HEADLINE=values["HEADLINE"],
        DATE=_truncate(values["DATE"], 90),
        BACKUP_LINE=backup_line,
        DESC=_truncate(values["DESC"], 100),
        SOURCE=values["SOURCE"],
        TAGS=values["TAGS"],
    ).strip()
    return _truncate(fallback, RC_TWEET_CHAR_LIMIT)


def _build_tweet_text(
    status: str,
    description: str,
    date_str: str,
    uniq_key: str,
    category: str = "road",
    backup_date: Optional[str] = None,
) -> str:
    """
    Build a concise access update tweet with a clear hook, impact, source, and tags.
    The selected template stays deterministic so retries do not change the copy.
    """
    category = _norm(category) or "road"
    kind = "closure" if category == "beach" else _status_kind(status)
    desc = _format_description(description)
    date = _format_date_window(date_str)
    backup = _format_date_window(_optional_text(backup_date)) if _optional_text(backup_date) else ""

    values = {
        "EMOJI": _emoji_for_status(status),
        "HEADLINE": "Boca Chica Beach closure" if category == "beach" else _headline_for_status(kind),
        "STATUS": _status_label(status),
        "DESC": desc,
        "DATE": date,
        "BACKUP": backup,
        "BACKUP_LINE": "",
        "IMPACT": _impact_line(kind, desc, category=category),
        "SOURCE": RC_SOURCE_LABEL,
        "TAGS": RC_HASHTAGS,
    }

    if category == "beach":
        templates = [
            "{EMOJI} {HEADLINE}\n\nPrimary: {DATE}\n{BACKUP_LINE}Area: {DESC}\nTimes: local Starbase.\n\n{SOURCE}\n{TAGS}",
            "{EMOJI} Starbase beach access alert\n\nBeach closure window:\nPrimary: {DATE}\n{BACKUP_LINE}{IMPACT}\nTimes: local Starbase.\n\n{SOURCE}\n{TAGS}",
            "{EMOJI} Boca Chica Beach closure update\n\n{DESC}\nPrimary: {DATE}\n{BACKUP_LINE}Times: local Starbase.\n\n{SOURCE}\n{TAGS}",
            "{EMOJI} Boca Chica closure notice\n\nPrimary window: {DATE}\n{BACKUP_LINE}Beach access may be unavailable during this window.\n\n{SOURCE}\n{TAGS}",
            "{EMOJI} Starbase closure window posted\n\n{DESC}\nPrimary: {DATE}\n{BACKUP_LINE}Worth tracking for launch and static-fire activity.\n\n{SOURCE}\n{TAGS}",
            "{EMOJI} Access update for Boca Chica Beach\n\nClosure window: {DATE}\n{BACKUP_LINE}Times are local to Starbase.\n\n{SOURCE}\n{TAGS}",
            "{EMOJI} Beach access heads-up\n\nBoca Chica Beach closure listed for:\n{DATE}\n{BACKUP_LINE}{IMPACT}\n\n{SOURCE}\n{TAGS}",
        ]
    else:
        templates = [
            "{EMOJI} {HEADLINE}\n\n{STATUS}: {DESC}\nWindow: {DATE} (local Starbase time)\n{IMPACT}\n\n{SOURCE}\n{TAGS}",
            "{EMOJI} Boca Chica / Starbase\n\n{STATUS}: {DESC}\nWhen: {DATE} (local Starbase time)\n{IMPACT}\n\nTracking the next update.\n{TAGS}",
            "{EMOJI}p Starbase access watch\n\n{STATUS}\nArea: {DESC}\nWindow: {DATE} (local time)\n{IMPACT}\n\n{SOURCE}\n{TAGS}",
            "{EMOJI} {HEADLINE}\n{DESC}\n\n{DATE} (local Starbase time)\n{IMPACT}\n\n{SOURCE}\n{TAGS}",
            "{EMOJI} Road activity near Starbase\n\n{DESC}\nWindow: {DATE} (local Starbase time)\n{IMPACT}\n\n{SOURCE}\n{TAGS}",
            "{EMOJI} SH-4 / Boca Chica access note\n\n{STATUS}: {DESC}\nWindow: {DATE} (local time)\nPlan around possible access changes.\n\n{SOURCE}\n{TAGS}",
            "{EMOJI} Starbase road watch\n\nNew listed window: {DATE}\nArea: {DESC}\n{IMPACT}\n\n{SOURCE}\n{TAGS}",
            "{EMOJI} Boca Chica road update\n\n{DESC}\n{DATE} (local Starbase time)\nDrivers and launch-watchers should plan ahead.\n\n{SOURCE}\n{TAGS}",
            "{EMOJI} Access window update\n\n{STATUS}\n{DESC}\nWindow: {DATE}\n{IMPACT}\n\n{SOURCE}\n{TAGS}",
        ]

    seed = uniq_key or f"{status}|{description}|{date_str}"
    tmpl = templates[_stable_index(seed, len(templates))]
    return _render_tweet_with_fit(tmpl, values)


def _extract_tweet_id(resp: Any) -> Optional[str]:
    """
    Essaie d'extraire l'ID de tweet depuis divers formats de réponse possibles.
    """
    try:
        # Tweepy v2 style: resp.data["id"]
        if hasattr(resp, "data") and isinstance(resp.data, dict) and "id" in resp.data:
            return str(resp.data["id"])
        # dict direct
        if isinstance(resp, dict):
            if "data" in resp and isinstance(resp["data"], dict) and "id" in resp["data"]:
                return str(resp["data"]["id"])
            if "id" in resp:
                return str(resp["id"])
        # Objet simple avec attribut id
        if hasattr(resp, "id"):
            return str(resp.id)
    except Exception:
        pass
    return None


# ---------------------------
# Mongo upsert des nouvelles RC
# ---------------------------

def _save_new_rc_to_mongo(db, row: pd.Series, tweet_id: Optional[str], collection_name: str = "RoadClosure") -> None:
    """
    Upsert idempotent d'une RC (via uniq_key). On stocke également tweet_id si présent.
    """
    coll = db[collection_name]
    try:
        coll.create_index("uniq_key", unique=True)
    except Exception:
        pass

    doc = {
        "uniq_key": row["uniq_key"],
        "category": row.get("category", "road"),
        "status": row["status"],
        "description": row["description"],
        "date": row["date"],               # string telle que scrapée
        "backup_date": _optional_text(row.get("backup_date")),
        "source_url": row.get("source_url"),
        "tweet_id": tweet_id,
    }
    coll.update_one({"uniq_key": row["uniq_key"]}, {"$set": doc}, upsert=True)


# ---------------------------
# Runner
# ---------------------------

def run_RC_checks() -> None:
    db = get_database()
    api = get_api_twitter()

    url = ACCESS_URL
    html = get_html(url)
    df = extract_access_updates(html, source_url=url)

    if df.empty:
        logging.info("[RC] No beach closure or road update detected on the site.")
        return

    df = df.copy()
    df["uniq_key"] = [
        _make_uniq_key(
            row["date"],
            row["description"],
            category=row.get("category"),
            backup_date=_optional_text(row.get("backup_date")),
        )
        for _, row in df.iterrows()
    ]

    existing = _get_existing_uniq_keys(db, "RoadClosure")
    new_df = df[~df["uniq_key"].isin(existing)].copy().reset_index(drop=True)

    if new_df.empty:
        logging.info("[RC] Nothing new to save or tweet.")
        return

    tweeted = 0
    failures = 0

    for _, row in new_df.iterrows():
        try:
            tweet_text = _build_tweet_text(
                status=row["status"],
                description=row["description"],
                date_str=row["date"],
                uniq_key=row["uniq_key"],
                category=row.get("category", "road"),
                backup_date=_optional_text(row.get("backup_date")),
            )
            resp = api.create_tweet(text=tweet_text)
            tweet_id = _extract_tweet_id(resp)
            _save_new_rc_to_mongo(db, row, tweet_id=tweet_id)

            tweeted += 1
            logging.info(f"[RC] Tweeted category={row.get('category', 'road')} uniq_key={row['uniq_key']} tweet_id={tweet_id}")
        except Exception as e:
            failures += 1
            _save_new_rc_to_mongo(db, row, tweet_id=None)
            logging.error(f"[RC][ERR] Failed to tweet uniq_key={row['uniq_key']}: {e}")

    logging.info(f"[RC] Done. Tweeted: {tweeted}, Failed: {failures}.")


if __name__ == "__main__":
    try:
        run_RC_checks()
    except Exception as e:
        logging.error(f"[RC][ERR] Failed to run RC checks: {e}")
