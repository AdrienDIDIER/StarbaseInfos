import os
import pandas as pd
import requests
import logging
from io import StringIO 
from datetime import datetime
from utils.utils import get_database, get_api_twitter
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_data_table_simple(url):

    x = requests.get(url)
    df = pd.read_html(StringIO(x.text))[0]
    df_tmp = pd.read_html(StringIO(x.text))[1]  
    
    df_tmp = df_tmp.rename(columns={"Unnamed: 0": "Type", "Temp. Delay Date": "Date", "Time of Delay": "DateTime"})
    df_tmp["Status"] = 'Transport Closure'
    df = df.rename(columns={"Unnamed: 0": "Type", "Temp. Closure Date": "Date", "Time of Closure": "DateTime", "Current Beach Status": "Status"}, errors="raise")
    df = pd.concat([df, df_tmp])

    df = df[~df["Date"].isna()]

    df["DateTime"] = df["DateTime"].str.replace(".", "", regex=False)
    df["DateTime"] = df["DateTime"].str.replace("am", "AM", regex=False)
    df["DateTime"] = df["DateTime"].str.replace("pm", "PM", regex=False)
    
    df['Type'] = df['Type'].fillna("Date")
    df['index'] = df['Date'] + " " + df['DateTime']
    return df


def get_data_old_date(client):
    db_infos = client[os.getenv('MONGO_DB_URL_TABLE')].find()
    data = pd.DataFrame(columns=['index', 'Flight'])
    for info in db_infos:
        if 'Flight' in info.keys():
            data.loc[len(data.index)] = [info['index'], info['Flight']]
    return data


def insert_new_road_closure(client, df):
    
    df = df[~df['Type'].isna()]
    
    data = get_data_old_date(client)
    df = df.merge(data, on='index', how='left')
    df['Flight'] = df['Flight'].fillna(-1)

    df_data_old = pd.DataFrame(list(client[os.getenv('MONGO_DB_URL_TABLE')].find()))

    list_id_new = []
    for row in df.to_dict(orient='records'):
        result = client[os.getenv('MONGO_DB_URL_TABLE')].replace_one(
            {'index': row.get('index')}, row, upsert=True
        )
        if result.upserted_id is not None:
            list_id_new.append(result.upserted_id)

    list_id_change = []
    df_data = pd.DataFrame(list(client[os.getenv('MONGO_DB_URL_TABLE')].find()))
    df_changes = pd.concat([df_data_old,df_data]).drop_duplicates(keep=False)
    for obj in df_changes['_id'].unique():
        if obj not in list_id_new:
            list_id_change.append(obj)
    return list_id_new, list_id_change


def get_rc_with_id(client, ids, created):
    rcs = client[os.getenv('MONGO_DB_URL_TABLE')].find({'_id' : {"$in": ids}})
    df = pd.DataFrame(list(rcs))
    if len(df) > 0:
        df['created'] = created
    return df


def get_rc_to_check(client):
    rcs = client[os.getenv('MONGO_DB_URL_TABLE')].find({'Flight': -1})
    result = []
    for rc in rcs:
        result.append(datetime.strftime(rc['Date'], "%Y-%m-%d"))
    return result


def tweet_road_closure_simple(api, df):

    message = []

    for _, row in df.iterrows():
        
        Date_start = row["Date"]
        Date_end = row["DateTime"]

        # STATUS
        if "Canceled" in row["Status"]:
            row["Status"] = "🚧 Road closure canceled"
        elif "Scheduled" in row["Status"]:
            row["Status"] = "🚧 Road closure scheduled"
        elif "Possible" in row["Status"]:
            row["Status"] = "🚧 Possible road closure"

        # TYPE
        if row["created"] is True:
            row["created"] = "🇺🇸 NEW RC : \n"
        else:
            row["created"] = "🇺🇸 RC UDPATE : \n"

        # FLIGHT
        if row["Flight"] == 0:
            row["Flight"] = "❌ NB : This is a non flight closure"
        elif row["Flight"] == 1:
            row["Flight"] = "✅ NB : This can be a flight closure"
        else:
            row["Flight"] = ""
        
        message.append(
            row["created"]+
            row["Type"] +
            ": " +
            row["Status"] +
            " from " +
            Date_start + " " + Date_end + 
            " Boca Chica Time \n" +
            row["Flight"]
        )

    for i in range(len(message)):
        try:
            api.create_tweet(text=message[i])
        except Exception as e:
            print(e)    
    return


def run_roads_closure():
    db = get_database()
    api = get_api_twitter()

    # GET ROAD closure
    df = get_data_table_simple("https://www.cameroncountytx.gov/spacex/")
    row_added, row_updated = insert_new_road_closure(db, df)

    # GET DATA OF NEW AND UPDATED ROAD closure
    df_created = get_rc_with_id(db, row_added, True)
    df_updated = get_rc_with_id(db, row_updated, False)
    df_to_tweet = pd.concat([df_created, df_updated])
    if len(df_to_tweet) > 0:
        logging.error(f"Update / Creation of {len(df_created) + len(df_updated)} RC.")
        # tweet_road_closure_without_api(driver, df_to_tweet)
        tweet_road_closure_simple(api, df_to_tweet)
    else:
        logging.error("No Tweet RC")

run_roads_closure()