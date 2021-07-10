import os
import configparser
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine


class ServerDatabaseSQL:

    ROOT_DIR = Path(__file__).parents[1]
    CONFIG_DIR = os.path.abspath(os.path.join(ROOT_DIR, 'configs'))
    CONFIG_FILENAME = CONFIG_DIR + '/config.ini'

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(self.CONFIG_FILENAME)
        self.setting_connection()

    def setting_connection(self):
        x = {
            'user': self.config['SERVER_DATABASE_CREDENTIALS']['username'],
            'password': self.config['SERVER_DATABASE_CREDENTIALS']['password'],
            'host': self.config['SERVER_DATABASE_CREDENTIALS']['hostname'],
            'port': self.config['SERVER_DATABASE_CREDENTIALS']['port'],
            'db': self.config['SERVER_DATABASE_CREDENTIALS']['database']
        }
        driver = "mysql+pymysql:"
        conn = f"{driver}//{x['user']}:{x['password']}@{x['host']}/{x['db']}"
        return create_engine(conn)


class SQLConnection(ServerDatabaseSQL):

    def __init__(self):
        super().__init__()
        self.conn = self.setting_connection()

    def read_sql(self, table_name):
        query = f"""
                SELECT * FROM {table_name}
        """
        return pd.read_sql(sql=query, con=self.conn)

    def to_sql(self, df, table_name):
        df.to_sql(table_name=table_name, con=self.conn, if_exists='replace')


kl = SQLConnection()

a = kl.read_sql(table_name='contacts')