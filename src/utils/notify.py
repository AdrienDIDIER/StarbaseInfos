from __future__ import annotations

import io
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional

import requests
import tweepy

from utils.utils import get_api_twitter

TWEET_CHAR_LIMIT = 280
HTTP_TIMEOUT = 20
ALERT_THROTTLE_HOURS = 6

_TAG_RE = re.compile(r"#\w+")


def is_dry_run() -> bool:
    """DRY_RUN=1 : rien n'est publié, mais l'état est quand même enregistré (utile pour amorcer une nouvelle source)."""
    return os.getenv("DRY_RUN", "").strip().lower() in {"1", "true", "yes"}


def tweet_len(text: str) -> int:
    """Approximate X weighted length: emoji and wide characters count for 2."""
    return sum(1 if ord(ch) < 0x1100 else 2 for ch in text)


def fit_lines(lines: List[tuple], limit: int = TWEET_CHAR_LIMIT) -> str:
    """
    Build a post from (text, priority) lines, dropping the highest-priority-number lines
    first until it fits. Priority 0 lines are never dropped; empty texts are skipped.
    """
    kept = [(text, prio) for text, prio in lines if text]
    while True:
        text = "\n".join(t for t, _ in kept)
        if tweet_len(text) <= limit:
            return text
        droppable = [p for _, p in kept if p > 0]
        if not droppable:
            return _truncate_weighted(text, limit)
        worst = max(droppable)
        for i in range(len(kept) - 1, -1, -1):
            if kept[i][1] == worst:
                del kept[i]
                break


def _truncate_weighted(text: str, limit: int) -> str:
    out = text
    # L'ellipse compte pour 2
    while tweet_len(out) > limit - 2 and out:
        out = out[:-1]
    return out.rstrip() + "…"


def extract_tweet_id(resp: Any) -> Optional[str]:
    try:
        if hasattr(resp, "data") and isinstance(resp.data, dict) and "id" in resp.data:
            return str(resp.data["id"])
        if isinstance(resp, dict):
            if isinstance(resp.get("data"), dict) and "id" in resp["data"]:
                return str(resp["data"]["id"])
            if "id" in resp:
                return str(resp["id"])
    except Exception:
        pass
    return None


# ---------------------------
# X / Twitter (plateforme principale)
# ---------------------------

def _upload_media_x(image: bytes) -> Optional[int]:
    auth = tweepy.OAuth1UserHandler(
        os.getenv("TWITTER_API_KEY"),
        os.getenv("TWITTER_API_SECRET_KEY"),
        os.getenv("TWITTER_ACCESS_TOKEN"),
        os.getenv("TWITTER_ACCESS_TOKEN_SECRET"),
    )
    media = tweepy.API(auth).media_upload(filename="card.png", file=io.BytesIO(image))
    return media.media_id


def _post_x(api, text: str, image: Optional[bytes], reply_to: Optional[str]) -> Optional[str]:
    kwargs = {"text": text}
    if reply_to:
        kwargs["in_reply_to_tweet_id"] = reply_to
    if image:
        try:
            kwargs["media_ids"] = [_upload_media_x(image)]
        except Exception as e:
            # L'image est un bonus : on publie le texte seul plutôt que rien
            logging.warning(f"[NOTIFY] X media upload failed, posting text only: {e}")
    return extract_tweet_id(api.create_tweet(**kwargs))


# ---------------------------
# Plateformes secondaires (optionnelles, activées par variables d'env)
# ---------------------------

def _bluesky_facets(text: str) -> list:
    facets = []
    for m in _TAG_RE.finditer(text):
        start = len(text[: m.start()].encode("utf-8"))
        end = len(text[: m.end()].encode("utf-8"))
        facets.append({
            "index": {"byteStart": start, "byteEnd": end},
            "features": [{"$type": "app.bsky.richtext.facet#tag", "tag": m.group()[1:]}],
        })
    return facets


def _post_bluesky(text: str, image: Optional[bytes]) -> None:
    handle, password = os.getenv("BLUESKY_HANDLE"), os.getenv("BLUESKY_APP_PASSWORD")
    if not handle or not password:
        return
    host = os.getenv("BLUESKY_PDS_URL", "https://bsky.social").rstrip("/")
    session = requests.post(
        f"{host}/xrpc/com.atproto.server.createSession",
        json={"identifier": handle, "password": password},
        timeout=HTTP_TIMEOUT,
    )
    session.raise_for_status()
    session = session.json()
    headers = {"Authorization": f"Bearer {session['accessJwt']}"}

    record = {
        "$type": "app.bsky.feed.post",
        "text": text,
        "createdAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "langs": ["en"],
    }
    facets = _bluesky_facets(text)
    if facets:
        record["facets"] = facets
    if image:
        blob = requests.post(
            f"{host}/xrpc/com.atproto.repo.uploadBlob",
            headers={**headers, "Content-Type": "image/png"},
            data=image,
            timeout=HTTP_TIMEOUT,
        )
        blob.raise_for_status()
        record["embed"] = {
            "$type": "app.bsky.embed.images",
            "images": [{"alt": text[:900], "image": blob.json()["blob"]}],
        }
    resp = requests.post(
        f"{host}/xrpc/com.atproto.repo.createRecord",
        headers=headers,
        json={"repo": session["did"], "collection": "app.bsky.feed.post", "record": record},
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()


def _post_telegram(text: str, image: Optional[bytes], chat_id: Optional[str] = None) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        return
    base = f"https://api.telegram.org/bot{token}"
    if image and len(text) <= 1024:
        resp = requests.post(
            f"{base}/sendPhoto",
            data={"chat_id": chat_id, "caption": text},
            files={"photo": ("card.png", image, "image/png")},
            timeout=HTTP_TIMEOUT,
        )
    else:
        resp = requests.post(f"{base}/sendMessage", data={"chat_id": chat_id, "text": text}, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()


def _post_discord(text: str, image: Optional[bytes], webhook_url: Optional[str] = None) -> None:
    webhook_url = webhook_url or os.getenv("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        return
    if image:
        resp = requests.post(
            webhook_url,
            data={"content": text},
            files={"file": ("card.png", image, "image/png")},
            timeout=HTTP_TIMEOUT,
        )
    else:
        resp = requests.post(webhook_url, json={"content": text}, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()


# ---------------------------
# API publique
# ---------------------------

def publish(text: str, image: Optional[bytes] = None, reply_to: Optional[str] = None, api=None) -> Optional[str]:
    """
    Post on X (raises on failure so the caller can retry on its next run), then mirror
    best-effort on Bluesky / Telegram / Discord when they are configured.
    Returns the tweet id (None in dry-run).
    """
    if is_dry_run():
        logging.info(f"[NOTIFY][DRY_RUN] Would post ({tweet_len(text)} chars, image={bool(image)}):\n{text}")
        return None

    tweet_id = _post_x(api or get_api_twitter(), text, image, reply_to)

    for name, poster in (("bluesky", _post_bluesky), ("telegram", _post_telegram), ("discord", _post_discord)):
        try:
            poster(text, image)
        except Exception as e:
            logging.warning(f"[NOTIFY] {name} post failed: {e}")
    return tweet_id


def send_alert(db, key: str, message: str) -> None:
    """
    Operator alert (scraper broken, source down...). Goes to ALERT_DISCORD_WEBHOOK_URL and/or
    TELEGRAM_ALERT_CHAT_ID, never to the public account. Throttled per key via Mongo.
    """
    logging.error(f"[ALERT] {key}: {message}")
    now = datetime.now(timezone.utc)
    try:
        if db is not None:
            coll = db["Alerts"]
            last = coll.find_one({"key": key})
            if last and last.get("sent_at"):
                sent_at = last["sent_at"]
                if sent_at.tzinfo is None:
                    sent_at = sent_at.replace(tzinfo=timezone.utc)
                if now - sent_at < timedelta(hours=ALERT_THROTTLE_HOURS):
                    return
            coll.update_one({"key": key}, {"$set": {"key": key, "sent_at": now, "message": message}}, upsert=True)
    except Exception as e:
        logging.warning(f"[ALERT] Throttle check failed, sending anyway: {e}")

    text = f"⚠️ StarbaseInfos alert [{key}]\n{message}"
    webhook = os.getenv("ALERT_DISCORD_WEBHOOK_URL")
    alert_chat = os.getenv("TELEGRAM_ALERT_CHAT_ID")
    for send in (
        (lambda: _post_discord(text, None, webhook_url=webhook)) if webhook else None,
        (lambda: _post_telegram(text, None, chat_id=alert_chat)) if alert_chat else None,
        lambda: send_discord_dm(text),
    ):
        try:
            if send:
                send()
        except Exception as e:
            logging.warning(f"[ALERT] Failed to deliver alert: {e}")


def send_discord_dm(text: str) -> bool:
    """Private message from the bot DISCORD_BOT_TOKEN to the user DISCORD_ALERT_USER_ID. False when not configured."""
    token, user_id = os.getenv("DISCORD_BOT_TOKEN"), os.getenv("DISCORD_ALERT_USER_ID")
    if not token or not user_id:
        return False
    headers = {"Authorization": f"Bot {token}"}
    channel = requests.post(
        "https://discord.com/api/v10/users/@me/channels",
        headers=headers, json={"recipient_id": user_id}, timeout=HTTP_TIMEOUT,
    )
    channel.raise_for_status()
    requests.post(
        f"https://discord.com/api/v10/channels/{channel.json()['id']}/messages",
        headers=headers, json={"content": text[:1900]}, timeout=HTTP_TIMEOUT,
    ).raise_for_status()
    return True


def report_failure(db, key: str, message: str, threshold: int = 3) -> None:
    """Count consecutive failures for `key`; alert once `threshold` is reached (transient errors stay quiet)."""
    failures = threshold
    try:
        doc = db["Alerts"].find_one({"key": f"{key}:failures"}) or {}
        failures = int(doc.get("failures") or 0) + 1
        db["Alerts"].update_one(
            {"key": f"{key}:failures"}, {"$set": {"key": f"{key}:failures", "failures": failures}}, upsert=True
        )
    except Exception as e:
        logging.warning(f"[ALERT] Failure counter unavailable: {e}")
    if failures >= threshold:
        send_alert(db, key, f"{message} ({failures} consecutive failures)")


def report_success(db, key: str) -> None:
    try:
        doc = db["Alerts"].find_one({"key": f"{key}:failures"})
        if doc and doc.get("failures"):
            db["Alerts"].update_one({"key": f"{key}:failures"}, {"$set": {"failures": 0}})
    except Exception as e:
        logging.warning(f"[ALERT] Failure counter unavailable: {e}")
