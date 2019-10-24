import json
from datetime import datetime

import unicodedata
from dotenv import load_dotenv
from tweepy.streaming import StreamListener

from common.helper.content import hash_strings
from config.config import DOTENV_FILE

assert load_dotenv(DOTENV_FILE)


class Listener(StreamListener):

    def __init__(self, connection):
        super().__init__()
        self.connection = connection
        self.cursor = self.connection.cursor()

    def on_data(self, data):
        try:
            all_data = json.loads(data)

            now = datetime.now()

            text = all_data["text"]
            text = unicodedata.normalize('NFC', text)

            author = all_data["user"]["screen_name"]

            content_id = hash_strings([author, text])
            tweet_id = hash_strings([now.strftime("%m/%d/%Y %H:%M:%S"), author, text])

            self.cursor.execute(
                "INSERT INTO tweets (tweet_id, content_id datetime, author, text) VALUES (%s,%s,%s,%s,%s)",
                (tweet_id, content_id, now, author, text))

            self.connection.commit()
            return True

        except Exception as e:
            self.on_error(e)

    def on_error(self, status):
        print(status)
