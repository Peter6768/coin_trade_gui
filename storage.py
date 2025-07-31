import os
import sqlite3

import utils

db_path = 'sqlite.db'

logger = utils.get_logger()


class DB:
    def __init__(self):
        self._conn = None
        self._cursor = None
        if not os.path.exists(db_path):
            logger.info('db file %s not exist, try to create.', db_path)
            try:
                conn = sqlite3.connect(db_path)
            except Exception as e:
                logger.exception('init db occur error: %s', e)
            else:
                conn.close()
        else:
            logger.info('db file %s already exist, skip init db', db_path)

    @property
    def conn(self):
        if not self._conn:
            self._conn = sqlite3.connect(db_path)
        return self._conn

    @conn.setter
    def conn(self, v):
        self._conn = v

    @property
    def cursor(self):
        if not self._cursor:
            self._cursor = self.conn.cursor()
        return self._cursor

    @cursor.setter
    def cursor(self, v):
        self._cursor = v

    def close(self):
        try:
            self.cursor.close()
            self.conn.close()
        except Exception as e:
            logger.exception('try to close db inst cursor or conn error: %s', e)

    def reload(self):
        self.conn = sqlite3.connect(db_path)
        self.cursor = self.conn.cursor()

    def execute(self, cmd):
        self.cursor.execute(cmd)
        self.conn.commit()

def create_table_wave_rate():
    cmd = '''
    create table if not exists wave_rate(
    id int primary key not null,
    coin_type text not null,
    begin_price real not null,
    max_price real not null,
    min_price real not null,
    last_price real not null,
    daily_wave real not null,
    wave_sum real,
    wave_coef real,
    wave_coef_year real,
    avg_wave real,
    daily_wave_rate real,
    wave_rate_year real);
    '''
    db_inst.execute(cmd)


def get_conn():
    return sqlite3.connect(db_path)


db_inst = DB()
