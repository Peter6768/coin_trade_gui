import os
import sqlite3
import pprint
import datetime as dt

import pandas as pd

import utils
import data

db_path = 'sqlite.db'

logger = utils.get_logger()

wave_coef = 14
wave_coef_year = 19.1


class DB:
    def __init__(self):
        self.data_clean_timespan = 30
        self.wave_rate_col_name_map = {
            'timestamp': '日期',
            'coin_type': '币种名称',
            'begin_price': '开盘价',
            'max_price': '最高价',
            'min_price': '最低价',
            'last_price': '收盘价',
            'daily_wave': '每日波幅',
            'wave_sum': '14天波幅汇总',
            'wave_coef': '波动系数',
            'wave_coef_year': '年化波动系数',
            'avg_wave': '平均波幅',
            'daily_wave_rate': '每日波动率',
            'wave_rate_year': '年化波动率'
        }

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
        # self.init_wave_data()

    @staticmethod
    def execute(cmd):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(cmd)
        conn.commit()
        rst = cursor.fetchall()
        cursor.close()
        conn.close()
        return rst

    @staticmethod
    def execute_df(cmd):
        conn = sqlite3.connect(db_path)
        rst = pd.read_sql(cmd, conn)
        conn.close()
        return rst

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
            kline_data = data.get_kline_data()
            self.handle_kline_data(kline_data)
            cmd = ('insert into wave_rate (timestamp,begin_price,max_price,min_price,last_price,coin_type,daily_wave,avg_wave,wave_sum,daily_wave_rate,wave_rate_year) values ' +
                   ','.join([('(%s,%s,%s,%s,%s,"%s",%s,%s,%s,%s,%s)' % tuple(v)) for _, values in kline_data.items() for v in values]) +
                   ';')
            self.execute(cmd)
        else:
            logger.info('table wave_rate not empty. try to update new data')
            pass

    @staticmethod
    def handle_kline_data(kline_data):
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
                    avg_wave = round(wave_sum / wave_coef, ndigits=6)
                    daily_wave_rate = round(avg_wave / float(v[4]) * 100, ndigits=6)
                    wave_rate_year = round(daily_wave_rate * wave_coef_year, ndigits=6)
                    v.extend([avg_wave, wave_sum, daily_wave_rate, wave_rate_year])

    def get_newest_date(self):
        return self.execute('select max(timestamp) from wave_rate;')[0][0]

    def get_recent_wave_data(self, dayspan=3):
        end_date = self.get_newest_date()
        start_date = end_date - 24 * 3600 * dayspan
        df = self.execute_df('select timestamp, coin_type, wave_rate_year from wave_rate where timestamp >= %s and timestamp < %s' % (start_date, end_date))
        rst = []
        for timestamp in sorted([i.item() for i in df['timestamp'].unique()], reverse=True):
            sub_df = df[df['timestamp'] == timestamp].copy()
            sub_df.sort_values(by='wave_rate_year', ascending=False, inplace=True)
            sub_df['rank'] = range(1, len(sub_df) + 1)
            sub_df['timestamp'] = sub_df['timestamp'].map(lambda x: dt.datetime.fromtimestamp(x).strftime('%Y-%m-%d'))
            rst.append(sub_df)
        return pd.concat(rst)

    def clean_wave_rate_old_data(self):
        newest_date = self.execute('select max(timestamp) from wave_rate;')[0][0]
        date_clean = int(newest_date) - 24 * 3600 * self.data_clean_timespan
        self.execute('delete from wave_rate where timestamp < %s' % date_clean)
        logger.info('success clean wave rate data before date %s', dt.datetime.fromtimestamp(date_clean).strftime('%Y-%m-%d'))

    def export_data(self, export_data_vars):
        self.clean_wave_rate_old_data()
        if export_data_vars['年化波动率']:
            df = self.execute_df('select * from wave_rate;')
            max_date = df['timestamp'].max()
            table_name = '年华波动率数据%s.xlsx' % dt.datetime.now().strftime('%Y-%m-%d')
            if os.path.exists(table_name):
                logger.info('table %s already exist, try to delte and create a new one', table_name)
                os.remove(table_name)
            with pd.ExcelWriter(table_name, engine='xlsxwriter') as writer:
                def handle_output_format(sheet_name, df_inner):
                    worksheet = writer.sheets[sheet_name]
                    for col_num, value in enumerate(df_inner.columns.values):
                        worksheet.write(0, col_num, value, output_format)
                workbook = writer.book
                output_format = workbook.add_format({'bold': False, 'border': 0})
                wave_rate_rank_col_names = ['timestamp', 'wave_rate_year', 'coin_type']
                coin_rank_sheet = df[df['timestamp'] == max_date].copy()[wave_rate_rank_col_names]
                coin_rank_sheet.sort_values(by='wave_rate_year', ascending=False, inplace=True)
                coin_rank_sheet['rank'] = range(1, len(coin_rank_sheet) + 1)
                coin_rank_sheet['timestamp'] = dt.datetime.fromtimestamp(max_date).strftime('%Y-%m-%d')
                coin_rank_sheet.rename(columns={**{k: v for k, v in self.wave_rate_col_name_map.items() if k in wave_rate_rank_col_names}, 'rank': '年华波动率排名'}, inplace=True)
                coin_rank_sheet.to_excel(writer, sheet_name='年化波动率排名', index=False)
                handle_output_format('年化波动率排名', coin_rank_sheet)
                for coin_name in sorted(df['coin_type'].unique()):
                    coin_sheet = df[df['coin_type'] == coin_name].copy()
                    coin_sheet.sort_values(by='timestamp', ascending=True, inplace=True)
                    coin_sheet['timestamp'] = coin_sheet['timestamp'].map(lambda x: dt.datetime.fromtimestamp(x).strftime('%Y-%m-%d'))
                    coin_sheet.rename(columns={**self.wave_rate_col_name_map, 'rank': '排名'}, inplace=True)
                    coin_sheet.to_excel(writer, sheet_name=coin_name, index=False)
                    handle_output_format(coin_name, coin_sheet)
        if export_data_vars['币种止损数据']:
            pass

    def update_wave_rate_date(self):
        newest_timestamp = self.get_newest_date()
        newest_data = data.get_kline_data(newest_timestamp)


db_inst = DB()
