import requests
import os

from utils.utils import get_database, get_api_twitter, upload_media
from bs4 import BeautifulSoup


def set_last_licence(client, id, table):
    client[os.getenv(table)].replace_one(
        {"NOTAM":id}, {"NOTAM":id}, upsert=True
    )
    return 


def get_last_licence(client, id, table):
    res = client[os.getenv(table)].find_one({"NOTAM": id})
    if res is not None:
        return True
    return False

def check_TFR(api, db_client, proxy):
    print(f"Ajout licence licenceBoca BDD")

    url = f"https://drs.faa.gov/browse/excelExternalWindow/DRSDOCID173891218620231102140506.0001?modalOpened=true"
    data = requests.get(url).content
    soup = BeautifulSoup(data, 'html.parser')
    
    print(data)
    print(soup.prettify())
    exit()


db = get_database()
api = get_api_twitter()
check_TFR(api, db, None)