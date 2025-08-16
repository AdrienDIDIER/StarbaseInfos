import requests
import pdf2image
import io
import os
import re
import pytesseract
import logging
from bs4 import BeautifulSoup
from utils.utils import get_database, get_api_twitter, upload_media
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def getMSIB():

    url = "http://msib.bocachica.com/"
    response = requests.get(url)
    if response.status_code != 200:
        logging.error("Error fetching page home")
    else:
        content = response.content

    # print(response.content)
    # print(response.content.decode(errors='replace'))
    # soup_page = BeautifulSoup(content, 'html.parser')
    # print(soup_page.prettify())
    # url_msib = soup_page.find("frame")['src']

    pdf_file = download_file("http://msib.bocachica.com/")
    text = pdf_to_img_to_text(pdf_file)
    return text, pdf_file 


def download_file(download_url):
    response = requests.get(download_url, verify=False)
    return response.content


def pdf_to_img_to_text(file):
    stream = pdf2image.convert_from_bytes(file)[0]
    # Recognize the text as string in image using pytesserct
    text = str(((pytesseract.image_to_string(stream))))
    text = text.replace("-\n", "").lower()
    return text


def set_last_msib(client, id, table):
    client[os.getenv(table)].replace_one(
        {"NOTAM":id}, {"last_issue_date":id}, upsert=True
    )
    return 


def get_last_msib(client, id, table):
    res = client[os.getenv(table)].find_one({"last_issue_date": id})
    if res is not None:
        return True
    return False


def check_MSIB(api, db_client, text, pdf_file):
    check_date = re.sub(r'[^\w\s]', '', text.split('issue date:')[1].split('spacex')[0]).lower().strip().replace(" ",'').replace('\n', ' ').replace('\r', '')
    to_tweet = 'New MSIB : http://msib.bocachica.com/'
    if not get_last_msib(db_client, check_date, "MONGO_DB_URL_TABLE_MSIB"):
        print('Tweet MSIB')
        try:
            img = pdf2image.convert_from_bytes(pdf_file, fmt='jpeg')[0]
            # Create a buffer to hold the bytes
            buf = io.BytesIO()
            # Save the image as jpeg to the buffer
            img.save(buf, 'jpeg')
            # Rewind the buffer's file pointer
            buf.seek(0)
            # Read the bytes from the buffer
            image_bytes = buf.read()
            media = upload_media(image_bytes)
            api.create_tweet(text = to_tweet, media_ids=[media])
        except Exception as e:
            print(e)
        set_last_msib(db_client, check_date, "MONGO_DB_URL_TABLE_MSIB")
    else:
        print('No Tweet MSIB')


def run_MSIB():
    db = get_database()
    api = get_api_twitter()

    textMSIB, pdf_file = getMSIB()
    if textMSIB is not None:
        check_MSIB(api, db, textMSIB, pdf_file)
    else:
        logging.error('No Tweet MSIB')

run_MSIB()