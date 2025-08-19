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


def extract_road_updates(
    html: str,
    source_url: Optional[str] = None,
) -> pd.DataFrame:
    """
    Extrait les RC/Road Delays en colonnes simples: status, description, date (string), source_url.
    """
    soup = BeautifulSoup(html, "html.parser")
    records: List[dict] = []

    # Bloc principal
    for item in soup.select("#road-closure .cms-item-2"):
        status_el = item.select_one(".cms-big-text")
        status = status_el.get_text(strip=True) if status_el else "Road Update"

        rich = item.select_one(".cms-small-text-api")
        if not rich:
            continue

        for br in rich.find_all("br"):
            br.replace_with("\n")
        block_text = rich.get_text("\n", strip=True)

        for description, date_str in _iter_description_date_pairs(block_text):
            final_desc = description if description is not None else "Unknown description"
            records.append(
                {
                    "status": status,
                    "description": final_desc,
                    "date": date_str,
                    "source_url": source_url,
                }
            )

    # Fallback: notification bar (si pas de cms-item-2)
    if not records:
        bar = soup.select_one("#rich-bar-message")
        if bar:
            text = bar.get_text(" ", strip=True)
            for part in re.split(r"\s*\|\s*", text):
                part = part.strip()
                if not part:
                    continue
                records.append(
                    {
                        "status": "Road Delay",
                        "description": "Road Notification",
                        "date": part,
                        "source_url": source_url,
                    }
                )

    df = pd.DataFrame.from_records(records)
    if not df.empty:
        df = df.sort_values(["date"]).reset_index(drop=True)
    return df


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

def _make_uniq_key(date_str: Optional[str], desc: Optional[str]) -> str:
    """Clé d’unicité basée sur (date + description) normalisées."""
    return f"{_norm(date_str)}::{_norm(desc)}"

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
        return _make_uniq_key(doc.get("date"), doc.get("description"))
    return None

def _get_existing_uniq_keys(db, collection_name: str = "RoadClosure") -> Set[str]:
    """
    Charge les uniq_key existantes depuis Mongo (schema minimal).
    """
    coll = db[collection_name]
    cursor = coll.find({}, {"_id": 1, "uniq_key": 1, "date": 1, "description": 1})
    keys: Set[str] = set()
    for doc in cursor:
        k = _doc_to_uniq_key(doc)
        if k:
            keys.add(k)
    return keys


# ---------------------------
# Tweet helpers
# ---------------------------

def _emoji_for_status(status: str) -> str:
    s = (status or "").strip().lower()
    if "closure" in s:
        return "🛑"
    if "delay" in s:
        return "⚠️"
    return "🚦"

def _truncate(text: str, max_len: int) -> str:
    if len(text) <= max_len:
        return text
    if max_len <= 1:
        return "…"
    return text[: max_len - 1].rstrip() + "…"

def _stable_index(seed: str, modulo: int) -> int:
    """
    Sélection de template déterministe (pas random) basée sur sha1(uniq_key).
    """
    h = hashlib.sha1(seed.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % modulo

def _build_tweet_text(status: str, description: str, date_str: str, uniq_key: str) -> str:
    """
    Construit un tweet concis et "joli" en anglais (<= 280 chars), sans URL.
    Plusieurs gabarits sont utilisés de manière déterministe pour varier les posts.
    """
    emoji = _emoji_for_status(status)
    desc = description if description and description.lower() != "unknown description" else "No description provided"

    # Templates avec placeholders {EMOJI} {STATUS} {DESC} {DATE}
    templates = [
        # 1
        "{EMOJI} {STATUS} — {DESC}\nDate: {DATE} (local)\nDrive safe.",
        # 2
        "{EMOJI} Heads up: {STATUS}\n{DESC}\nWhen: {DATE}",
        # 3
        "{EMOJI} {STATUS}\n{DESC}\n{DATE} — plan accordingly.",
        # 5
        "{EMOJI} {STATUS} | {DATE}\n{DESC}\nExpect restrictions in the area.",
    ]

    # Choisir un template basé sur uniq_key (stable)
    idx = _stable_index(uniq_key or (status + date_str + desc), len(templates))
    tmpl = templates[idx]

    # On calcule la place restante pour {DESC} pour rester < 280 chars
    base_without_desc = tmpl.format(EMOJI=emoji, STATUS=status, DESC="", DATE=date_str)
    room_for_desc = 280 - len(base_without_desc)
    if room_for_desc < 0:
        room_for_desc = 0
    desc_final = _truncate(desc, room_for_desc)

    tweet = tmpl.format(EMOJI=emoji, STATUS=status, DESC=desc_final, DATE=date_str).strip()

    # Sécu supplémentaire
    if len(tweet) > 280:
        # Tronquer davantage la description si besoin
        overflow = len(tweet) - 280
        desc_final = _truncate(desc_final, max(0, len(desc_final) - overflow))
        tweet = tmpl.format(EMOJI=emoji, STATUS=status, DESC=desc_final, DATE=date_str).strip()

    return tweet

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
        "status": row["status"],
        "description": row["description"],
        "date": row["date"],               # string telle que scrapée
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

    URL = "https://cityofstarbase-texas.com/beach-road-access"
    html = get_html(URL)
    df = extract_road_updates(html, source_url=URL)

    if df.empty:
        logging.info("[RC] Aucune road closure/delay détectée sur le site.")
        return

    # Calcule la uniq_key (date + description)
    df = df.copy()
    df["uniq_key"] = [_make_uniq_key(d, desc) for d, desc in zip(df["date"], df["description"])]

    # Dédup avec la base
    existing = _get_existing_uniq_keys(db, "RoadClosure")
    new_df = df[~df["uniq_key"].isin(existing)].copy().reset_index(drop=True)

    if new_df.empty:
        logging.info("[RC] Rien de nouveau à enregistrer/tweeter (tout est déjà en base).")
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
            )
            resp = api.create_tweet(text=tweet_text)
            tweet_id = _extract_tweet_id(resp)
            _save_new_rc_to_mongo(db, row, tweet_id=tweet_id)

            tweeted += 1
            logging.info(f"[RC] Tweeted (uniq_key={row['uniq_key']}) → tweet_id={tweet_id}")
        except Exception as e:
            failures += 1
            # On loggue, on enregistre malgré tout la RC sans tweet_id pour éviter de la retweeter en boucle
            _save_new_rc_to_mongo(db, row, tweet_id=None)
            logging.error(f"[RC][ERR] Failed to tweet uniq_key={row['uniq_key']}: {e}")

    # out_csv = "rc_new_to_tweet.csv"
    # new_df.to_csv(out_csv, index=False)
    logging.info(f"[RC] Done. Tweeted: {tweeted}, Failed: {failures}.")


# Décommente si tu veux exécuter directement ce fichier :
if __name__ == "__main__":
    try:
        run_RC_checks()
    except Exception as e:
        logging.error(f"[RC][ERR] Failed to run RC checks: {e}")
