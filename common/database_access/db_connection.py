from os import environ

import MySQLdb
from dotenv import load_dotenv

from config.config import DOTENV_FILE

DATABASE_TWITTER = "twitter"


class DbConnection:
    def __init__(self, database: str):
        assert load_dotenv(DOTENV_FILE)
        self.database = database

        if database == DATABASE_TWITTER:
            self.host = environ.get("TWITTER_HOST")
            self.db = environ.get("TWITTER_DB")
            self.user = environ.get("TWITTER_USER")
            self.password = environ.get("TWITTER_PASSWORD")

        else:
            raise ValueError("database {} does not exist".format(database))

        self.connection = MySQLdb.connect(self.host,
                                          self.user,
                                          self.password,
                                          self.db,
                                          use_unicode=True,
                                          charset="utf8")
