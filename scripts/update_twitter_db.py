from os import environ

from dotenv import load_dotenv
from tweepy import OAuthHandler
from tweepy import Stream

from common.database_access.db_connection import DbConnection
from config.config import DOTENV_FILE
from repo_path import REPO_PATH
from twitter_data.tweeter_streemer.listener import Listener

assert load_dotenv(DOTENV_FILE)
print(REPO_PATH)
print(DOTENV_FILE)
auth = OAuthHandler(environ.get("CUSTOMER_KEY"), environ.get("CUSTOMER_SECRET"))
auth.set_access_token(environ.get("ACCESS_TOKEN"), environ.get("ACCESS_TOKEN_SECRET"))

listener = Listener(DbConnection('twitter').connection)
twitterStream = Stream(auth, listener)

twitterStream.filter(track=[
    "Bitcoin",
    "BTC",
    "比特币",
    "crypto",
    "bitcoin",
    'btc',
])
