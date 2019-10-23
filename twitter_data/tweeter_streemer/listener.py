import json
from datetime import datetime

import unicodedata
from dotenv import load_dotenv
from tweepy.streaming import StreamListener

from config.config import DOTENV_FILE

assert load_dotenv(DOTENV_FILE)


class Listener(StreamListener):

    def __init__(self, connection):
        self.connection = connection
        self.cursor = self.connection.cursor()

    def on_data(self, data):
        try:
            all_data = json.loads(data)

            text = all_data["text"]
            text = unicodedata.normalize('NFC', text)
            author = all_data["user"]["screen_name"]
            self.cursor.execute("INSERT INTO tweets (datetime, author, text) VALUES (%s,%s,%s)",
                                (datetime.now(), author, text))

            self.connection.commit()

        except Exception as e:
            self.on_error(e)

        return True

    def on_error(self, status):
        print(status)
