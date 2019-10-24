from os import environ

import MySQLdb
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from common.database_access.db_connection_singleton import DbConnectionSingleton
from config.config import DOTENV_FILE


class DbConnection(metaclass=DbConnectionSingleton):
    host = ""
    db = ""
    user = ""
    password = ""
    engine = ""
    session = ""

    CONNECTION_TWITTER = 'twitter'

    def __init__(self, database: str):
        assert load_dotenv(DOTENV_FILE), "can not load .env file"

        self.name = database
        self.get_connection()

    def get_connection(self):
        self.init_needed_parameters()

        # create pool engine
        self.engine = create_engine(self.find_dsn(), poolclass=NullPool)

        # PLEASE LEAVE THIS INSIDE. This is for debugging dudes..
        # self.engine = create_engine(self.find_dsn(), poolclass=NullPool, pool_pre_ping=True, echo=True)

        self.session = Session(bind=self.engine)
        self.connection = self.engine.connect()
        self.mysqldb_connection = MySQLdb.connect(self.host,
                                                  self.user,
                                                  self.password,
                                                  self.db,
                                                  use_unicode=True,
                                                  charset="utf8")

    def init_needed_parameters(self):
        if self.name == self.CONNECTION_TWITTER:
            self.host = environ.get("TWITTER_HOST")
            self.db = environ.get("TWITTER_DB")
            self.user = environ.get("TWITTER_USER")
            self.password = environ.get("TWITTER_PASSWORD")

    def find_dsn(self):
        return "mysql://{}:{}@{}/{}?charset={}".format(
            self.user, self.password, self.host, self.db, "utf8mb4"
        )
