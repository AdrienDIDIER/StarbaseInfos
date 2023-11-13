import tweepy
import os
import dotenv

from pymongo import MongoClient

dotenv.load_dotenv()


def get_database():
    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = os.getenv('MONGO_DB_URL')
    # Create a connection using MongoClient. You can import MongoClient or use pymongo.MongoClient
    client = MongoClient(CONNECTION_STRING)
    return client['bocachicaroadclosure']


def get_api_twitter():
    # Authenticate to Twitter
    client = tweepy.Client(
        consumer_key=os.getenv("TWITTER_API_KEY"), consumer_secret=os.getenv("TWITTER_API_SECRET_KEY"),
        access_token=os.getenv("TWITTER_ACCESS_TOKEN"), access_token_secret=os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
    )
    return client


def set_last_tweet(client, id, table):
    client[os.getenv(table)].replace_one(
        {"last_id":id}, {"last_id":id}, upsert=True
    )
    return 


def get_last_tweet(client, id, table):
    res = client[os.getenv(table)].find_one({"last_id": id})
    if res is not None:
        return True
    return False


def upload_media(img_data):
    tweepy_auth = tweepy.OAuth1UserHandler(
        "{}".format(os.environ.get("TWITTER_API_KEY")),
        "{}".format(os.environ.get("TWITTER_API_SECRET_KEY")),
        "{}".format(os.environ.get("TWITTER_ACCESS_TOKEN")),
        "{}".format(os.environ.get("TWITTER_ACCESS_TOKEN_SECRET")),
    )
    tweepy_api = tweepy.API(tweepy_auth)
    with open("test.jpg", "wb") as handler:
        handler.write(img_data)
    media = tweepy_api.media_upload("test.jpg")
    os.remove("test.jpg")
    return media.media_id