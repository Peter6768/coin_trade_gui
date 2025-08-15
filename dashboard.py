from threading import Thread, Event, Lock
import time
from datetime import datetime
from functools import partial
from configparser import ConfigParser
from collections import deque

from tkinter import Frame as Frame_tk, END, Tk, StringVar, BooleanVar
from tkinter.ttk import Frame as Frame_ttk, LabelFrame, Label, Radiobutton, Combobox, Button, Entry, Checkbutton, Treeview, Scrollbar, Style, Notebook
from tkinter.messagebox import showinfo

import data
import utils
import storage

logger = utils.get_logger()


class CollectDataThread:
    def __init__(self):
        self.stop_event = Event()
        self.coin_type = ''
        self.mutex = Lock()
        self.ontime_data_window = deque(maxlen=2)
        self.thread_start()

    @staticmethod
    def get_timestamp():
        now_timestamp = time.time()
        end_timestamp = int(now_timestamp - (now_timestamp % 300))
        begin_timestamp = end_timestamp - 300
        return begin_timestamp, end_timestamp

    def get_data(self):
        begin_timestamp, end_timestamp = self.get_timestamp()
        row_num = storage.db_inst.execute('select count(*) from wave_rate where timestamp>=%s and timestamp<%s' % (begin_timestamp, end_timestamp))[0][0]
        if row_num == 0:
            return data.get_one_coin_kline(self.coin_type, begin_timestamp, end_timestamp)
        elif row_num > 0:
            return []

    def get_peak_values(self, day_begin_timestamp, begin_timestamp, end_timestamp):
        today_records = storage.db_inst.execute_df('select today_max, today_min from ontime_kline where timestamp>=%s and timestamp<=%s', day_begin_timestamp, begin_timestamp)
        if len(today_records) > 0:
            return today_records.sort_values(by='timestamp', ascending=True).iloc[-1][['today_max', 'today_min']]

        return '', ''

    def thread_start(self):
        def thread_inner():
            # fill today data:
            while True:
                if not self.stop_event.is_set():
                    if self.coin_type and self.coin_type != '请选择币种':
                        now = time.time()
                        day_begin_timestamp = now - (now + 8 * 3600) % 86400
                        end_timestamp = now - now % 300
                        begin_timestamp = end_timestamp - 300
                        day_begin_timestamp = day_begin_timestamp if day_begin_timestamp < begin_timestamp else int(day_begin_timestamp) - 86400
                        resp = storage.db_inst.execute('select max(timestamp) from ontime_kline where coin_type=="%s";' % self.coin_type)
                        if resp[0][0] == begin_timestamp:
                            logger.info('ontime_kline data is newest, no need to fill')
                            break
                        update_begin_timestamp = max(resp[0][0] or 0, day_begin_timestamp - 1)
                        resp = storage.db_inst.execute('select today_max, today_min from ontime_kline where timestamp==%s and coin_type=="%s"' % (update_begin_timestamp, self.coin_type))
                        today_max, today_min = (0, 0) if not resp else resp[0]
                        logger.info('auto fill ontime_kline data from date %s to %s', update_begin_timestamp, begin_timestamp)

                        coin_data = data.get_one_coin_kline(self.coin_type, int(update_begin_timestamp) * 1000, int(begin_timestamp) * 1000 + 1)
                        if not coin_data:
                            logger.error('get coin data is None! return')
                            showinfo('提示', '无法回补币种%s5分钟k线数据, 请检查vpn是否开启, 并重启程序' % self.coin_type)
                            return

                        coin_data.sort(key=lambda x: int(x[0]))
                        for index in range(len(coin_data)):
                            coin_data[index] = coin_data[index][:5]
                            v = coin_data[index]
                            v[0] = int(v[0][:-3])
                            v.append(self.coin_type)
                            today_max = max(today_max, round(float(v[2]), ndigits=8))
                            if index == 0 and not today_min:
                                today_min = round(float(v[3]))
                            else:
                                today_min = min(today_min, round(float(v[3]), ndigits=8))
                            v.extend([today_max, today_min])
                            v.extend(['null'] * 5)
                        cmd = ('insert into ontime_kline (timestamp, begin_price, max_price, min_price, last_price, '
                               'coin_type, today_max, today_min, dot_neg_num, dot_pos_num, dot_final, dot_key_value, '
                               'dot_op_type) values ') + ','.join([('(%s,%s,%s,%s,%s,"%s",%s,%s,%s,%s,%s,%s,%s)' % tuple(i)) for i in coin_data])
                        storage.db_inst.execute(cmd)
                        break
                time.sleep(15)

            cmd = ('select coin_type, timestamp, begin_price, max_price, min_price, last_price, today_max, today_min,'
                   '(today_max - today_min) as today_delta, dot_neg_num, dot_pos_num, dot_final from ontime_kline '
                   'where coin_type=="%s"') % self.coin_type
            d = storage.db_inst.execute_df(cmd)
            d['timestamp'] = d['timestamp'].map(lambda x: datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M'))
            stop_loss_view(notebook_view, d.sort_values(by='timestamp', ascending=True))

            while True:
                if not self.stop_event.is_set():
                    if self.coin_type and self.coin_type != '请选择币种':
                        now = time.time()
                        end_timestamp = int(now - now % 300)
                        begin_timestamp = int(end_timestamp - 300)
                        newest_timestamp = storage.db_inst.execute('select max(timestamp) from ontime_kline where coin_type=="%s";' % self.coin_type)[0][0]
                        if newest_timestamp == begin_timestamp:
                            logger.info('ontime_kline data is newest, no need to update')
                        elif newest_timestamp + 300 == begin_timestamp:
                            today_max, today_min = storage.db_inst.execute('select today_max, today_min from ontime_kline where coin_type=="%s" and timestamp==%s' % (self.coin_type, newest_timestamp))[0]
                            coin_data = data.get_one_coin_kline(self.coin_type, begin_timestamp * 1000 - 1, end_timestamp * 1000)[0][:5]
                            coin_data[0] = int(coin_data[0][:-3])
                            coin_data.append(self.coin_type)
                            coin_data.extend([max(today_max, int(coin_data[2])), min(today_min, int(coin_data[3]))])
                            coin_data.extend(['null'] * 5)
                            cmd = ('insert into ontime_kline (timestamp, begin_price, max_price, min_price, '
                                   'last_price, coin_type, today_max, today_min, dot_neg_num, dot_pos_num, '
                                   'dot_final, dot_key_value, dot_op_type) values ') + '(%s,%s,%s,%s,%s,"%s",%s,%s,%s,%s,%s,%s,%s)' % tuple(coin_data)
                            storage.db_inst.execute(cmd)
                        self.update_dashboard()
                time.sleep(60)
        Thread(target=thread_inner, daemon=True).start()

    def update_dashboard(self):
        pass

    def collect_data_thread_edit(self, *args, **kwargs):
        utils.activate_widget(*args, **kwargs)

    def collect_data_thread_apply(self, *args, **kwargs):
        coin_type_widget = kwargs.get('coin_type', None)
        if coin_type_widget:
            v = coin_type_widget.get()
            if v == '请选择币种':
                pass
            else:
                self.coin_type = v
                action = collect_data_radio.get()
                if action == 'yes':
                    self.stop_event.clear()
                elif action == 'no':
                    self.stop_event.set()
            utils.disable_widget(kwargs.get('coin_type'))

        utils.disable_widget(*args)


def data_collect_panel(notebook):
    tab = Frame_ttk(notebook)
    data_collect_frame = LabelFrame(tab, text='数据采集综合面板', padding=[10 for _ in range(4)])
    data_collect_frame.pack(side='left')
    notebook.add(tab, text='数据采集综合面板')
    notebook.pack(fill='both', expand=True)

    # time
    def update_time():
        time_label.config(text='当前时间: ' + time.strftime('%Y-%m-%d %H:%M:%S'))
        time_label.after(1000, update_time)
    time_frame = Frame_tk(data_collect_frame)
    time_frame.pack(anchor='w', fill='x')
    time_label = Label(time_frame, text='当前时间')
    time_label.pack(side='left')
    update_time()

    # collect data
    collect_data_frame = Frame_tk(data_collect_frame)
    collect_data_frame.pack(anchor='w', fill='x')
    Label(collect_data_frame, text='行情采集').pack(side='left')
    collect_data_radio1 = Radiobutton(collect_data_frame, text='采集', variable=collect_data_radio, value='yes', state='disabled')
    collect_data_radio2 = Radiobutton(collect_data_frame, text='停止采集', variable=collect_data_radio, value='no', state='disabled')
    collect_data_coin_name = Combobox(collect_data_frame, state='disabled')
    collect_data_coin_name.set('请选择币种')
    collect_data_coin_name.bind('<Button-1>', partial(utils.load_ontime_coin_type_thread, collect_data_coin_name))
    collect_data_coin_name.pack(side='left')
    collect_data_set = Button(collect_data_frame, text='修改', command=lambda: collect_data_thread.collect_data_thread_edit(collect_data_radio1, collect_data_radio2, collect_data_apply, special={collect_data_coin_name: 'readonly'}))
    collect_data_apply = Button(collect_data_frame, text='应用', state='disabled', command=lambda: collect_data_thread.collect_data_thread_apply(collect_data_radio1, collect_data_radio2, collect_data_apply, coin_type=collect_data_coin_name))

    collect_data_radio1.pack(side='left')
    collect_data_radio2.pack(side='left')
    collect_data_set.pack(side='right')
    collect_data_apply.pack(side='right')

    manual_collect_frame = Frame_tk(data_collect_frame)
    manual_collect_frame.pack(anchor='w', fill='x')
    Label(manual_collect_frame, text='手动回补数据').pack(side='left')
    manual_collect_radio1 = Radiobutton(manual_collect_frame, text='开始回补', variable=manual_collect_radio, value='start', state='disabled')
    manual_collect_radio2 = Radiobutton(manual_collect_frame, text='停止回补', variable=manual_collect_radio, value='stop', state='disabled')
    manual_collect_set = Button(manual_collect_frame, text='修改', command=lambda: utils.activate_widget(manual_collect_radio1, manual_collect_radio2, manual_collect_apply))
    manual_collect_apply = Button(manual_collect_frame, text='应用', state='disabled', command=lambda: utils.disable_widget(manual_collect_radio1, manual_collect_radio2, manual_collect_apply))

    manual_collect_radio1.pack(side='left')
    manual_collect_radio2.pack(side='left')
    manual_collect_set.pack(side='right')
    manual_collect_apply.pack(side='right')

    fixed_threshold_frame = Frame_tk(data_collect_frame)
    fixed_threshold_frame.pack(anchor='w', fill='x')
    Label(fixed_threshold_frame, text='固定值破位低').pack(side='left')
    fixed_threshold_entry1 = Entry(fixed_threshold_frame, state='disabled')
    fixed_threshold_entry1.pack(side='left')
    Label(fixed_threshold_frame, text='固定值破位高').pack(side='left')
    fixed_threshold_entry2 = Entry(fixed_threshold_frame, state='disabled')
    fixed_threshold_entry2.pack(side='left')
    fixed_threshold_set = Button(fixed_threshold_frame, text='修改', command=lambda: utils.activate_widget(fixed_threshold_entry1, fixed_threshold_entry2, fixed_threshold_apply))
    fixed_threshold_set.pack(side='right')
    fixed_threshold_apply = Button(fixed_threshold_frame, text='应用', state='disabled', command=lambda: utils.disable_widget(fixed_threshold_entry1, fixed_threshold_entry2, fixed_threshold_apply))
    fixed_threshold_apply.pack(side='right')

    interval_threshold_frame = Frame_tk(data_collect_frame)
    interval_threshold_frame.pack(anchor='w', fill='x')
    Label(interval_threshold_frame, text='间隔止损报警位').pack(side='left')
    interval_threshold_entry = Entry(interval_threshold_frame, state='disabled')
    interval_threshold_entry.pack(side='left')
    interval_threshold_set = Button(interval_threshold_frame, text='修改', command=lambda: utils.activate_widget(interval_threshold_entry, interval_threshold_apply))
    interval_threshold_set.pack(side='right')
    interval_threshold_apply = Button(interval_threshold_frame, text='应用', state='disabled', command=lambda: utils.disable_widget(interval_threshold_entry, interval_threshold_apply))
    interval_threshold_apply.pack(side='right')

    moving_threshold_frame = Frame_tk(data_collect_frame)
    moving_threshold_frame.pack(anchor='w', fill='x')
    Label(moving_threshold_frame, text='移动止损报警位').pack(side='left')
    moving_threshold_entry = Entry(moving_threshold_frame, state='disabled')
    moving_threshold_entry.pack(side='left')
    moving_threshold_set = Button(moving_threshold_frame, text='修改', command=lambda: utils.activate_widget(moving_threshold_entry, moving_threshold_apply))
    moving_threshold_set.pack(side='right')
    moving_threshold_apply = Button(moving_threshold_frame, text='应用', state='disabled', command=lambda: utils.disable_widget(moving_threshold_entry, moving_threshold_apply))
    moving_threshold_apply.pack(side='right')

    email_frame = Frame_tk(data_collect_frame)
    email_frame.pack(anchor='w', fill='x')
    Label(email_frame, text='邮箱地址').pack(side='left')
    email_entry = Entry(email_frame, state='disabled')
    email_entry.pack(side='left')
    email_set = Button(email_frame, text='修改', command=lambda: utils.activate_widget(email_entry, email_apply))
    email_set.pack(side='right')
    email_apply = Button(email_frame, text='应用', state='disabled', command=lambda: utils.disable_widget(email_entry, email_apply))
    email_apply.pack(side='right')

    def dynamic_load_export_date(event):
        resp = storage.db_inst.execute_df('select distinct timestamp from wave_rate;')
        timestamp_options = sorted(resp['timestamp'].unique())[-7:]
        export_date_selection['values'] = [datetime.fromtimestamp(i).strftime('%Y-%m-%d') for i in timestamp_options]
    export_frame = Frame_tk(data_collect_frame)
    export_frame.pack(anchor='w', fill='x')
    Label(export_frame, text='导出数据excel').pack(side='left')
    for k, v in export_data_vars.items():
        Checkbutton(export_frame, variable=v, onvalue=True, offvalue=False, text=k).pack(side='left')
    Label(export_frame, text='年化波动率排名日期 ').pack(side='left')
    export_date_selection = Combobox(export_frame, state='readonly')
    export_date_selection.set('请选择日期')
    export_date_selection.bind("<Button-1>", dynamic_load_export_date)
    export_date_selection.pack(side='left')
    Button(export_frame, text='导出', command=lambda: storage.db_inst.export_data(export_data_vars, export_date_selection)).pack(side='right', padx=(100, 0))

    alarm_frame = LabelFrame(tab, text='报警面板', padding=[10 for _ in range(4)])
    alarm_frame.pack(side='left')

    for k, v in alarm_data_vars.items():
        tmp_frame = Frame_tk(alarm_frame)
        tmp_frame.pack(anchor='w')
        Checkbutton(tmp_frame, variable=v, onvalue=True, offvalue=False, text=k).pack(side='left')


def buy_sell_panel(notebook):
    tab = Frame_ttk(notebook)
    notebook.add(tab, text='清仓/挂单综合面板')
    notebook.pack(fill='both', expand=True)


def wave_rate_panel(notebook):
    wave_rate_tab = Frame_ttk(notebook)
    notebook.add(wave_rate_tab, text='年化波动率视图')
    notebook.pack(fill='both', expand=True)
    Thread(target=wave_rate_view, daemon=True, args=(wave_rate_tab, )).start()


# def stop_loss_panel(notebook):
#     stop_loss_tab = Frame_ttk(notebook)
#     notebook.add(stop_loss_tab, text='止损数据视图')
#     notebook.pack(fill='both', expand=True)
#     stop_loss_view(stop_loss_tab)


def wave_rate_view(tab):
    storage.db_inst.init_wave_data()
    data = storage.db_inst.get_recent_wave_data()
    container = Frame_ttk(tab)
    container.pack(fill='both', expand=True)
    container.grid_rowconfigure(0, weight=1)
    # container.grid_columnconfigure(0, weight=1)
    # container.pack(expand=True)
    for index, daily_data in enumerate(data):
        tree = Treeview(container, columns=list(daily_data.columns), show='headings')

        col_width = {'wave_rate_year': 60, 'coin_type': 100, 'rank': 50}
        col_name_map = {'rank': '排名', 'coin_type': '币种类型', 'wave_rate_year': '年化波动率', 'timestamp': '日期'}
        for col in daily_data.columns:
            tree.heading(col, text=col_name_map[col])
            tree.column(col, width=col_width.get(col, 80), anchor='center')
            # tree.column(col, anchor='center')

        for _, row in daily_data.iterrows():
            tree.insert('', END, values=list(row))

        bar_vertical = Scrollbar(container, orient='vertical', command=tree.yview)
        tree.configure(yscrollcommand=bar_vertical.set)
        container.columnconfigure(2 * index, weight=1)

        tree.grid(row=0, column=2 * index, sticky="nsew")
        bar_vertical.grid(row=0, column=2 * index + 1, sticky='ns')


def stop_loss_view(notebook, data):
    stop_loss_tab = Frame_ttk(notebook)
    notebook.add(stop_loss_tab, text='止损数据视图')
    notebook.pack(fill='both', expand=True)
    container = Frame_ttk(stop_loss_tab)
    container.pack(fill='both', expand=True)
    container.grid_rowconfigure(0, weight=1)
    # container.grid_columnconfigure(2, weight=1)

    tree = Treeview(container, columns=list(data.columns), show='headings')

    for col in data.columns:
        tree.heading(col, text=storage.db_inst.ontime_kline_col_name_map[col])
        tree.column(col, anchor='center')

    for _, row in data.iterrows():
        tree.insert('', END, values=list(row))

    bar_vertical = Scrollbar(container, orient='vertical', command=tree.yview)
    tree.configure(yscrollcommand=bar_vertical.set)
    container.columnconfigure(0, weight=1)

    tree.grid(row=0, column=0, sticky="nsew")
    bar_vertical.grid(row=0, column=1, sticky='ns')


def update_wave_rate_task():
    # update wave rate data every day from 00:01:00 to 00:02:00
    def update_wave_rate_data():
        while True:
            storage.db_inst.update_wave_rate_data()
            time.sleep(60)
    Thread(target=update_wave_rate_data, daemon=True).start()


def clean_old_wave_rate_task():
    def clean_old_wave_rate_inner():
        while True:
            storage.db_inst.clean_wave_rate_old_data()
            time.sleep(60)
    Thread(target=clean_old_wave_rate_inner, daemon=True).start()


def thread_tasks():
    update_wave_rate_task()
    clean_old_wave_rate_task()


def main():
    style = Style()
    style.map('TEntry', bordercolor=[('disabled', 'red')])

    data_collect_panel(notebook_control)
    buy_sell_panel(notebook_control)
    wave_rate_panel(notebook_control)
    # stop_loss_panel(notebook_view)

    thread_tasks()
    root.mainloop()


if __name__ == '__main__':
    root = Tk()
    root.title('量化交易窗口工具')
    collect_data_radio = StringVar(value='no')
    manual_collect_radio = StringVar(value='stop')
    export_data_vars = {
        '年化波动率': BooleanVar(value=True),
        '币种止损数据': BooleanVar(value=False),
    }
    alarm_data_vars = {
        '采集数据缺失': BooleanVar(value=True),
        '间隔止损报警': BooleanVar(value=True),
        '固定止损报警': BooleanVar(value=True),
        '移动止损报警': BooleanVar(value=True),
    }

    notebook_control = Notebook(root)
    notebook_view = Notebook(root)

    collect_data_thread = CollectDataThread()
    main()
