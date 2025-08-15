from os import path, remove
from sqlite3 import connect
import time
from datetime import datetime
from collections import deque
from threading import Lock

from pandas import read_sql, ExcelWriter
from tkinter import messagebox

import utils
import data

db_path = 'sqlite.db'

logger = utils.get_logger()

wave_coef = 14
wave_coef_year = 19.1


class DB:
    def __init__(self):
        self.data_clean_timespan = 90
        self.state = 'ok'
        self.mutex = Lock()
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

        if not path.exists(db_path):
            logger.info('db file %s not exist, try to create.', db_path)
            try:
                conn = connect(db_path)
            except Exception as e:
                logger.exception('init db occur error: %s', e)
            else:
                conn.close()
        else:
            logger.info('db file %s already exist, skip init db', db_path)
        self.create_table_wave_rate()
        self.create_table_ontime_kline()

    def execute(self, cmd):
        with self.mutex:
            conn = connect(db_path)
            cursor = conn.cursor()
            cursor.execute(cmd)
            conn.commit()
            rst = cursor.fetchall()
            cursor.close()
            conn.close()
        return rst

    @staticmethod
    def execute_df(cmd):
        conn = connect(db_path)
        rst = read_sql(cmd, conn)
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

    def create_table_ontime_kline(self):
        cmd = '''
        create table if not exists ontime_kline(
        coin_type text not null,
        timestamp integer not null,
        begin_price real not null,
        min_price real not null,
        max_price real not null,
        last_price real not null,
        today_max real not null,
        today_min real not null,
        dot_neg_num integer,
        dot_pos_num integer,
        dot_final integer,
        dot_key_value integer,
        dot_op_type text);
        '''
        self.execute(cmd)

    def insert_wave_rate_batch(self, d):
        cmd = ('insert into wave_rate (timestamp,begin_price,max_price,min_price,last_price,coin_type,daily_wave,avg_wave,wave_sum,daily_wave_rate,wave_rate_year) values ' +
               ','.join([('(%s,%s,%s,%s,%s,"%s",%s,%s,%s,%s,%s)' % tuple(v)) for _, values in d.items() for v in values]) + ';')
        self.execute(cmd)

    def init_wave_data(self):
        try:
            self.state = 'initializing'
            row_num = self.execute('select count(*) from wave_rate;')[0][0]
            if row_num == 0:
                logger.info('table wave_rate is empty, try to initialize')
                now = time.time()
                after_timestamp = int((now - now % 86400 - 8 * 3600) * 1000)
                kline_data = data.get_kline_data(after=after_timestamp)
                self.handle_kline_data(kline_data)
                self.insert_wave_rate_batch(kline_data)
            else:
                logger.info('table wave_rate not empty. try to update new data')
                pass
        except Exception as e:
            logger.exception('init wave_data error, reset state to ok: %s', e)
        finally:
            self.state = 'ok'

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
                if index < 13:
                    v.extend(['null'] * 4)
                else:
                    wave_sum = sum([v_tmp[6] for v_tmp in values[index - 13: index + 1]])
                    avg_wave = round(wave_sum / wave_coef, ndigits=8)
                    daily_wave_rate = round(avg_wave / max(float(v[4]), 1e-8) * 100, ndigits=8)
                    wave_rate_year = round(daily_wave_rate * wave_coef_year, ndigits=8)
                    v.extend([avg_wave, wave_sum, daily_wave_rate, wave_rate_year])

    def get_newest_date(self):
        return self.execute('select max(timestamp) from wave_rate;')[0][0]

    def get_recent_wave_data(self, dayspan=3):
        end_date = self.get_newest_date()
        start_date = end_date - 24 * 3600 * dayspan
        df = self.execute_df('select timestamp, coin_type, wave_rate_year from wave_rate where timestamp > %s and timestamp <= %s' % (start_date, end_date))
        rst = []
        for timestamp in sorted([i.item() for i in df['timestamp'].unique()], reverse=True):
            sub_df = df[df['timestamp'] == timestamp].copy()
            sub_df.sort_values(by='wave_rate_year', ascending=False, inplace=True)
            sub_df['rank'] = range(1, len(sub_df) + 1)
            sub_df['timestamp'] = sub_df['timestamp'].map(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d'))
            rst.append(sub_df)
        return rst

    def clean_wave_rate_old_data(self):
        if self.state == 'initializing':
            logger.info('data is initializing, return')
            return
        else:
            resp = self.execute('select max(timestamp) from wave_rate;')
            if len(resp) == 0 or len(resp[0]) == 0 or (not resp[0][0]):
                return
            newest_date = resp[0][0]
            date_clean = int(newest_date) - 24 * 3600 * self.data_clean_timespan
            if self.execute('select count(*) from wave_rate where timestamp<%s' % date_clean)[0][0] == 0:
                return
            logger.info('begin to clean old data before timestamp %s', date_clean)
            self.execute('delete from wave_rate where timestamp < %s' % date_clean)

    def export_data(self, export_data_vars, wave_date_combobox):
        if self.state == 'initializing':
            logger.error('data is initializing, cannot export data')
            messagebox.showinfo('提示', '正在初始化数据, 无法导出数据')
            return
        if export_data_vars['年化波动率']:
            wave_rate_date = wave_date_combobox.get()
            if not wave_rate_date:
                logger.error('export wave_rate data need fill wave rate date.')
                messagebox.showinfo('提示', '导出数据需要填写年化波动率日期')
                return
            self.clean_wave_rate_old_data()
            df = self.execute_df('select * from wave_rate;')
            target_date = datetime.strptime(wave_rate_date, '%Y-%m-%d')
            target_timestamp = target_date.timestamp()
            # max_date = df['timestamp'].max()
            table_name = '年华波动率数据%s.xlsx' % wave_rate_date
            if path.exists(table_name):
                logger.info('table %s already exist, try to delete and create a new one', table_name)
                try:
                    remove(table_name)
                except PermissionError:
                    logger.error('export data error, file is open, please close file and retry export data')
                    messagebox.showinfo('提示', '文件已打开, 无法导出文件, 请关闭文件后重新导出')
            with ExcelWriter(table_name, engine='xlsxwriter') as writer:
                def handle_output_format(sheet_name, df_inner):
                    worksheet = writer.sheets[sheet_name]
                    for col_num, value in enumerate(df_inner.columns.values):
                        worksheet.write(0, col_num, value, output_format)
                workbook = writer.book
                output_format = workbook.add_format({'bold': False, 'border': 0})
                wave_rate_rank_col_names = ['timestamp', 'wave_rate_year', 'coin_type']
                coin_rank_sheet = df[df['timestamp'] == target_timestamp].copy()[wave_rate_rank_col_names]
                coin_rank_sheet.sort_values(by='wave_rate_year', ascending=False, inplace=True)
                coin_rank_sheet['rank'] = range(1, len(coin_rank_sheet) + 1)
                coin_rank_sheet['timestamp'] = wave_rate_date
                coin_rank_sheet.rename(columns={**{k: v for k, v in self.wave_rate_col_name_map.items() if k in wave_rate_rank_col_names}, 'rank': '年华波动率排名'}, inplace=True)
                coin_rank_sheet.to_excel(writer, sheet_name='年化波动率排名', index=False)
                handle_output_format('年化波动率排名', coin_rank_sheet)
                for coin_name in sorted(df['coin_type'].unique()):
                    coin_sheet = df[df['coin_type'] == coin_name].copy()
                    coin_sheet.sort_values(by='timestamp', ascending=True, inplace=True)
                    coin_sheet['timestamp'] = coin_sheet['timestamp'].map(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d'))
                    coin_sheet.rename(columns={**self.wave_rate_col_name_map, 'rank': '排名'}, inplace=True)
                    coin_sheet.to_excel(writer, sheet_name=coin_name, index=False)
                    handle_output_format(coin_name, coin_sheet)
        if export_data_vars['币种止损数据']:
            pass
        messagebox.showinfo('提示', '数据导出完成, 文件存放在%s目录下' % path.realpath('.'))

    def update_wave_rate_data(self):
        count, sleep_time = 1, 30
        while count <= 5:
            if self.state == 'initialing':
                logger.info('storage is initializing, sleep %s and continue, retry time: %s', sleep_time, count)
                time.sleep(sleep_time)
                count += 1
            elif self.state == 'ok':
                break
        start_timestamp = self.get_newest_date()
        timestamp_delta = time.time() - start_timestamp
        if timestamp_delta > 24 * 3600 * self.data_clean_timespan:
            logger.info('newest timestamp older than 90 days, try to init wave data')
            self.init_wave_data()
        elif timestamp_delta > 24 * 3600 * 2:
            logger.info('begin to periodically update wave rate data')
            newest_data = data.get_kline_data(before=start_timestamp * 1000, after=(int(time.time()) - 86400) * 1000)
            old_data = self.execute_df('select * from wave_rate;')
            old_coin_names = old_data['coin_type'].unique()
            for coin_type in newest_data:
                values = newest_data[coin_type]
                if coin_type not in old_coin_names:
                    self.handle_kline_data({coin_type: values})
                else:
                    values.sort(key=lambda x: int(x[0]))
                    old_coin_data = old_data[old_data['coin_type'] == coin_type].sort_values(by='timestamp', ascending=True)
                    wave_sum_window = deque([i for i in old_coin_data['daily_wave'].iloc[-14:]], maxlen=14)
                    for index in range(len(values)):
                        v = values[index]
                        v[0] = int(v[0][:-3])
                        v.append(coin_type)
                        daily_wave = round(float(v[2]) - float(v[3]), ndigits=8)
                        v.append(daily_wave)
                        if len(wave_sum_window) < 13:
                            wave_sum_window.append(daily_wave)
                            continue
                        else:
                            wave_sum = sum(wave_sum_window)
                            avg_wave = round(wave_sum / wave_coef, ndigits=8)
                            daily_wave_rate = round(avg_wave / max(float(v[4]), 1e-8) * 100, ndigits=8)
                            wave_rate_year = round(daily_wave_rate * wave_coef_year, ndigits=8)
                            v.extend([avg_wave, wave_sum, daily_wave_rate, wave_rate_year])
                            wave_sum_window.append(daily_wave)
            self.insert_wave_rate_batch(newest_data)
        else:
            pass
        self.clean_wave_rate_old_data()


db_inst = DB()
