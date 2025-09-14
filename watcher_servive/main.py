# watcher_service/main.py
import os, time, re, json, threading, signal, logging
import pytesseract, webcolors
import os, cv2, logging
from yt_dlp import YoutubeDL
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse, JSONResponse
import uvicorn

from vidgear.gears import CamGear
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler

from utils.color_detector import BackgroundColorDetector
from utils.utils import get_database, get_api_twitter, set_last_tweet, get_last_tweet

load_dotenv()

YOUTUBE_URL = os.getenv("YOUTUBE_URL", "https://www.youtube.com/watch?v=mhJRzQsLZGg")
DATA_DIR     = os.getenv("DATA_DIR", "/data")
LOG_PATH     = os.path.join(DATA_DIR, "nsf-watcher.log")
STATUS_PATH  = os.path.join(DATA_DIR, "status.json")
OCR_INTERVAL = float(os.getenv("OCR_INTERVAL_SEC", "5"))

# --- logging (stdout + fichier rotatif) ---
logger = logging.getLogger("nsf-watcher")
logger.setLevel(logging.INFO)
sh = logging.StreamHandler()
fh = RotatingFileHandler(LOG_PATH, maxBytes=5_000_000, backupCount=3)
fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
sh.setFormatter(fmt); fh.setFormatter(fmt)
logger.addHandler(sh); logger.addHandler(fh)

STATE = {
    "start_ts": time.time(),
    "last_frame_ts": 0.0,
    "last_ocr_ts": 0.0,
    "last_text": None,
    "stream_connected": False,
    "error_count": 0,
    "tweets_sent": 0,
}

def save_status():
    try:
        tmp = STATUS_PATH + ".tmp"
        with open(tmp, "w") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATUS_PATH)
    except Exception as e:
        logger.warning(f"Failed to save status: {e}")

def closest_colour(requested_colour):
    min_colours = {}
    for key, name in webcolors.CSS3_HEX_TO_NAMES.items():
        r_c, g_c, b_c = webcolors.hex_to_rgb(key)
        rd = (r_c - requested_colour[0]) ** 2
        gd = (g_c - requested_colour[1]) ** 2
        bd = (b_c - requested_colour[2]) ** 2
        min_colours[(rd + gd + bd)] = name
    return min_colours[min(min_colours.keys())]

def get_colour_name(requested_colour):
    try:
        closest_name, actual_name = webcolors.rgb_to_name(requested_colour)
    except Exception:
        closest_name = closest_colour(requested_colour)
        actual_name = None
    return actual_name, closest_name

def img_to_text(crop_frame):
    BackgroundColor = BackgroundColorDetector(crop_frame)
    _, closest_name = get_colour_name(BackgroundColor.detect())
    if closest_name == 'firebrick':
        text = str(pytesseract.image_to_string(crop_frame))
        return text.replace("-\n", "")
    return None

def _resolve_hls(url: str) -> tuple[str, dict] | tuple[None, None]:
    """
    Retourne (url_variant_hls, meta_format) en choisissant la meilleure rendition HLS.
    On trie par height puis tbr, et on respecte TARGET_HEIGHT si défini.
    """
    target_h = int(os.getenv("TARGET_HEIGHT", "1080") or "1080")

    ydl_opts = {
        "http_headers": {"User-Agent": "Mozilla/5.0", "Accept-Language": "fr-FR,fr;q=0.9"},
        "noplaylist": True,
        "cachedir": False,
        "extractor_args": {"youtube": {"player_client": ["mweb"]}},
        # cookies si besoin :
        "cookiefile": os.getenv("YT_COOKIES"),
        "quiet": True,
    }
    with YoutubeDL(ydl_opts) as y:
        info = y.extract_info(url, download=False)

    # candidates HLS avec hauteur connue
    fmts = [
        f for f in info.get("formats", [])
        if "m3u8" in (f.get("protocol") or "") and (f.get("height") or 0) > 0
    ]
    if not fmts:
        # fallback: tous m3u8, même sans height
        fmts = [f for f in info.get("formats", []) if "m3u8" in (f.get("protocol") or "")]
    if not fmts:
        return None, None

    # 1) ceux <= target_h, sinon 2) tous
    fmts_le = [f for f in fmts if (f.get("height") or 0) <= target_h]
    choose_from = fmts_le or fmts

    # trie par (height, tbr) décroissant
    def _key(f):
        return (f.get("height") or 0, f.get("tbr") or 0.0)
    best = sorted(choose_from, key=_key, reverse=True)[0]

    return best.get("url"), best

def open_stream():
    url = YOUTUBE_URL.strip()
    hls, meta = _resolve_hls(url)
    if not hls:
        raise RuntimeError("No HLS format found from yt_dlp")

    os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = (
        "reconnect;1|reconnect_streamed;1|reconnect_delay_max;15"
    )
    cap = cv2.VideoCapture(hls, cv2.CAP_FFMPEG)

    class _CapWrap:
        def __init__(self, c): self.c = c
        def read(self):
            ok, f = self.c.read()
            return f if ok else None
        def stop(self):
            try: self.c.release()
            except Exception: pass

    logging.info("Opened HLS via OpenCV/FFmpeg")
    return _CapWrap(cap)

STOP = False
def handle_signal(sig, frame):
    global STOP
    STOP = True
signal.signal(signal.SIGTERM, handle_signal)
signal.signal(signal.SIGINT, handle_signal)

def run_watcher():
    db = get_database()
    api = get_api_twitter()

    stream, backoff = None, 2
    last_sample_ts = 0

    while not STOP:
        try:
            if stream is None:
                logger.info("Opening stream…")
                logger.info("Starting watcher service with cookies=%s", bool(os.getenv("YT_COOKIES")))
                # Content file cookies if needed
                logger.info(open(os.getenv("YT_COOKIES") or "No cookies file").read() if os.getenv("YT_COOKIES") else "No cookies file")
                stream = open_stream()
                STATE["stream_connected"] = True
                save_status()

            frame = stream.read()
            if frame is None:
                raise RuntimeError("Stream frame is None (dropped)")

            now = time.time()
            STATE["last_frame_ts"] = now

            if now - last_sample_ts >= OCR_INTERVAL:
                h, w = frame.shape[:2]
                y1, y2 = int(0.92*h), int(1.00*h)
                x1, x2 = int(0.13*w), int(0.95*w)
                crop = frame[y1:y2, x1:x2]

                text = img_to_text(crop)
                STATE["last_ocr_ts"] = now

                if text and '@nasaspaceflight' not in text.lower():
                    text = "Infos @NASASpaceflight : \n" + text.replace("$", "S")
                    # anti-duplication (via ta Mongo util)
                    key = re.sub(r'[^\w\s]', '', text).lower()
                    if not get_last_tweet(db, key, "MONGO_DB_URL_TABLE_PT"):
                        try:
                            api.create_tweet(text="🇺🇸 " + text)
                            STATE["tweets_sent"] += 1
                        except Exception as e:
                            logger.warning(f"Twitter error: {e}")
                        set_last_tweet(db, key, "MONGO_DB_URL_TABLE_PT")

                    STATE["last_text"] = text
                    logger.info(f"OCR: {text[:120].replace(os.linesep, ' ')}...")

                last_sample_ts = now
                save_status()

            time.sleep(0.05)
            backoff = 2
        except Exception as e:
            STATE["error_count"] += 1
            STATE["stream_connected"] = False
            save_status()
            logger.exception(f"Stream error, reconnecting soon: {e}")
            if stream is not None:
                try: stream.stop()
                except Exception: pass
                stream = None
            time.sleep(backoff)
            backoff = min(backoff * 2, 300)

    # sortie propre
    if stream is not None:
        try: stream.stop()
        except Exception: pass
    logger.info("Watcher stopped.")

# ---------- API ----------
app = FastAPI()

@app.get("/health")
def health():
    now = time.time()
    ok = (now - STATE.get("last_frame_ts", 0) < 30) and STATE.get("stream_connected", False)
    return JSONResponse({
        "ok": ok,
        "uptime_sec": int(now - STATE["start_ts"]),
        "last_frame_ts": STATE["last_frame_ts"],
        "last_ocr_ts": STATE["last_ocr_ts"],
        "stream_connected": STATE["stream_connected"],
        "error_count": STATE["error_count"],
        "tweets_sent": STATE["tweets_sent"],
    })

@app.get("/last")
def last():
    return JSONResponse({
        "last_text": STATE["last_text"],
        "last_ocr_ts": STATE["last_ocr_ts"],
    })

@app.get("/logs", response_class=PlainTextResponse)
def logs(n: int = 200):
    try:
        with open(LOG_PATH, "r") as f:
            lines = f.readlines()[-n:]
        return "".join(lines)
    except FileNotFoundError:
        return ""

def main():
    os.makedirs(DATA_DIR, exist_ok=True)
    t = threading.Thread(target=run_watcher, daemon=True)
    t.start()
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))

if __name__ == "__main__":
    main()
