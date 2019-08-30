from os import environ
import sqlalchemy as db
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker

DB_CONFIG_DEFAULT = {
    'drivername': 'postgres',
    'host': environ['RDS_HOST'],
    'port': environ['RDS_PORT'],
    'username': environ['RDS_USERNAME'],
    'password': environ['RDS_PASSWORD'],
    'database': environ['RDS_DB']
}


def config(args):
    copy = DB_CONFIG_DEFAULT.copy()
    copy.update(args)
    return copy


class DbClient:
    def __init__(self, conifg = config({})):
        self.engine = db.create_engine(URL(**conifg))
        self.connection = self.engine.connect()
        self.metadata = db.MetaData()

    def trash(self):
        self.connection.close()
        self.engine.dispose()

    def table_for(self, table_name, schema):
        return db.Table(table_name, self.metadata, autoload=True, autoload_with=self.engine, schema=schema)

    def session(self):
        return sessionmaker(bind=self.engine)()
