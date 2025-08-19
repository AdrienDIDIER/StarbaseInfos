import pytesseract
import webcolors
import re
import os
import logging
from utils.color_detector import BackgroundColorDetector
from utils.utils import get_database, get_api_twitter, set_last_tweet, get_last_tweet
from vidgear.gears import CamGear
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
# pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'



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
    except Exception as e:
        closest_name = closest_colour(requested_colour)
        actual_name = None
    return actual_name, closest_name


def img_to_text(crop_frame):

    BackgroundColor = BackgroundColorDetector(crop_frame)
    _, closest_name = get_colour_name(BackgroundColor.detect())
    
    if closest_name == 'firebrick':
        text = str(((pytesseract.image_to_string(crop_frame))))
        textEN = text.replace("-\n", "")
        return textEN
    else:
        return None


def getScreenNSF(url):
    ytdlp_params = {
        # ✅ évite le client TV/IOS qui déclenche "not available on this app"
        "extractor_args": {"youtube": {"player_client": ["android"]}},
        # cookies netscape si besoin (connexion/âge/région)
        "cookiefile": os.getenv("YT_COOKIES"),
        # headers "normaux"
        "http_headers": {"User-Agent": "Mozilla/5.0", "Accept-Language": "fr-FR,fr;q=0.9"},
        # ✅ pas de cache yt-dlp (équiv. à --rm-cache-dir au lancement)
        "cachedir": False,
        # autres options utiles
        "noplaylist": True,
    }

    options = {
        "STREAM_PARAMS": ytdlp_params,
        # petit délai pour laisser le backend accrocher le flux
        "time_delay": 2
    }

    logging.info(options)
    stream = CamGear(source=url, stream_mode=True, logging=True, **options).start() # YouTube Video URL as input
    frame = stream.read()
    crop_frame = frame[995:1080, 245:1820]
    ret = img_to_text(crop_frame)
    if ret==None or '@nasaspaceflight' in ret.lower():
        return None
    else:
        ret = ret.replace("$", "S")
        return "Infos @NASASpaceflight : \n" + ret


def check_NSF(api, db_client, text):
    if not get_last_tweet(db_client, re.sub(r'[^\w\s]', '', text).lower(), "MONGO_DB_URL_TABLE_PT"):
        print('Tweet NSF')
        try:
            api.create_tweet(text="🇺🇸 " + text)
        except Exception as e:
            print(e)
        set_last_tweet(db_client, re.sub(r'[^\w\s]', '', text).lower(), "MONGO_DB_URL_TABLE_PT")
    else:
        print('No Tweet NSF')


def run_NSF():
    db = get_database()
    api = get_api_twitter()
    
    textNSF = getScreenNSF("https://www.youtube.com/watch?v=mhJRzQsLZGg")
    logging.info(textNSF)
    if textNSF is not None:
        check_NSF(api, db, textNSF)
    else:
        logging.error('No Tweet NSF')
    return

if __name__ == "__main__":
    try:
        run_NSF()
    except Exception as e:
        logging.error(f"Error occurred: {e}")