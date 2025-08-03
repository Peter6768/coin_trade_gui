import os
import sqlite3
import pprint

import pandas as pd

import utils
import data

db_path = 'sqlite.db'

logger = utils.get_logger()

wave_coef = 14
wave_coef_year = 19.1


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
        self.create_table_wave_rate()
        self.init_wave_data()

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
        try:
            self.cursor.execute(cmd)
        except Exception as e:
            logger.exception('cursor execute cmd %s occur error: %s', cmd, e)
            return []
        try:
            self.conn.commit()
        except Exception as e:
            logger.exception('conn commit cmd %s occur error: %s', cmd, e)
            return []
        return self.cursor.fetchall()

    def execute_df(self, cmd):
        return pd.read_sql(cmd, self.conn)

    def create_table_wave_rate(self):
        cmd = '''
        create table if not exists wave_rate(
        timestamp integer not null,
        coin_type text not null,
        begin_price real not null,
        max_price real not null,
        min_price real not null,
        last_price real not null,
        daily_wave real not null,
        wave_sum real,
        wave_coef real default 14,
        wave_coef_year real default 19.1,
        avg_wave real,
        daily_wave_rate real,
        wave_rate_year real);
        '''
        self.execute(cmd)

    def init_wave_data(self):
        row_num = self.execute('select count(*) from wave_rate;')[0][0]
        if row_num == 0:
            logger.info('table wave_rate is empty, try to initialize')
            logger.info('table wave_rate not empty. try to update new data')
            kline_data = data.get_kline_data()
            self.handle_kline_data(kline_data)
            cmd = ('insert into wave_rate (timestamp,begin_price,max_price,min_price,last_price,coin_type,daily_wave,wave_sum,avg_wave,daily_wave_rate,wave_rate_year) values ' +
                   ','.join([('(%s,%s,%s,%s,%s,"%s",%s,%s,%s,%s,%s)' % tuple(v)) for _, values in kline_data.items() for v in values]) +
                   ';')
            self.execute(cmd)

    def handle_kline_data(self, kline_data):
        # todo: use pandas, not use dict
        for coin_name in kline_data:
            values = kline_data[coin_name]
            values.sort(key=lambda x: int(x[0]))
            for index in range(len(values)):
                v = values[index]
                v[0] = int(v[0][:-3])   # timestamp
                v.append(coin_name)     # coin_type
                # daily_wave
                daily_wave = round(float(v[2]) - float(v[3]), ndigits=8)
                v.append(daily_wave)
                if index < 14:
                    v.extend(['null'] * 4)
                else:
                    wave_sum = sum([v_tmp[6] for v_tmp in values[index - 14: index]])
                    avg_wave = round(wave_sum / wave_coef, ndigits=8)
                    daily_wave_rate = round(avg_wave / float(v[4]) * 100, ndigits=8)
                    wave_rate_year = daily_wave_rate * wave_coef_year
                    v.extend([avg_wave, wave_sum, daily_wave_rate, wave_rate_year])


def get_conn():
    return db_inst.conn


db_inst = DB()
