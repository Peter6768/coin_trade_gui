from threading import Thread, Event
import time
from datetime import datetime
from functools import partial

from tkinter import Frame as Frame_tk, END, Tk, StringVar, BooleanVar
from tkinter.ttk import Frame as Frame_ttk, LabelFrame, Label, Radiobutton, Combobox, Button, Entry, Checkbutton, Treeview, Scrollbar, Style, Notebook

import utils
import storage

logger = utils.get_logger()


class CollectDataThread:
    def __init__(self):
        self.stop_event = Event()

    @staticmethod
    def collect_data_thread_start(*args, **kwargs):
        utils.activate_widget(*args, **kwargs)

    @staticmethod
    def collect_data_thread_end(*args, **kwargs):
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
    collect_data_set = Button(collect_data_frame, text='修改', command=lambda: CollectDataThread.collect_data_thread_start(collect_data_radio1, collect_data_radio2, collect_data_apply, special={collect_data_coin_name: 'readonly'}))
    collect_data_apply = Button(collect_data_frame, text='应用', state='disabled', command=lambda: CollectDataThread.collect_data_thread_end(collect_data_radio1, collect_data_radio2, collect_data_apply, collect_data_coin_name))

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
        export_date_selection['values'] = ['1', '2']
        resp = storage.db_inst.execute_df('select distinct timestamp from wave_rate;')
        timestamp_options = sorted(resp['timestamp'].unique())[-8:]
        if datetime.now().date() == datetime.fromtimestamp(timestamp_options[-1]).date():
            export_date_selection['values'] = [datetime.fromtimestamp(i).strftime('%Y-%m-%d') for i in timestamp_options[:-1]]
        else:
            export_date_selection['values'] = [datetime.fromtimestamp(i).strftime('%Y-%m-%d') for i in timestamp_options[-7:]]
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

    # ontime_coin_type_frame = Frame(data_collect_frame)
    # ontime_coin_type_frame.pack(anchor='w', fill='x')
    # ttk.Label(ontime_coin_type_frame, text='止损数据币种类型').pack(side='left')
    # ontime_coin_type_combo = ttk.Combobox(ontime_coin_type_frame, state='disabled')
    # ontime_coin_type_combo.set('加载中')
    # ontime_coin_type_combo.pack(side='left')
    # ontime_coin_type_set = ttk.Button(ontime_coin_type_frame, text='修改', command=lambda: utils.activate_widget(ontime_coin_type_apply, special={ontime_coin_type_combo: 'readonly'}))
    # ontime_coin_type_set.pack(side='right')
    # ontime_coin_type_apply = ttk.Button(ontime_coin_type_frame, text='应用', state='disabled',
    #                                     command=lambda: utils.disable_widget(ontime_coin_type_combo, ontime_coin_type_apply))
    # ontime_coin_type_apply.pack(side='right')
    # Thread(target=utils.load_ontime_coin_type_thread, args=(ontime_coin_type_combo, ), daemon=True).start()

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


def stop_loss_panel(notebook):
    stop_loss_tab = Frame_ttk(notebook)
    notebook.add(stop_loss_tab, text='止损数据视图')
    notebook.pack(fill='both', expand=True)
    stop_loss_view(stop_loss_tab)


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


def stop_loss_view(tab):
    container = Frame_ttk(tab)
    container.pack(fill='y', expand=True)
    container.grid_columnconfigure(0, weight=1)
    container.grid_columnconfigure(2, weight=1)

    tree = Treeview(container, columns=[], show='headings')

    bar_vertical = Scrollbar(container, orient='vertical', command=tree.yview)
    tree.configure(yscrollcommand=bar_vertical.set)

    tree.grid(row=0, column=1, sticky="nsew")
    bar_vertical.grid(row=0, column=2, sticky='ns')


def update_wave_rate_task():
    # update wave rate data every day from 00:01:00 to 00:02:00
    def update_wave_rate_data():
        while True:
            now = datetime.now()
            seconds = now.hour * 3600 + now.minute * 60 + now.second
            if 60 < seconds <= 120:
                logger.info('periodically update wave rate data')
                storage.db_inst.update_wave_rate_date()
            time.sleep(60)
    Thread(target=update_wave_rate_data, daemon=True).start()


def thread_tasks():
    update_wave_rate_task()


def main():
    style = Style()
    style.map('TEntry', bordercolor=[('disabled', 'red')])

    notebook_control = Notebook(root)
    notebook_view = Notebook(root)
    data_collect_panel(notebook_control)
    buy_sell_panel(notebook_control)
    wave_rate_panel(notebook_control)
    stop_loss_panel(notebook_view)
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
    main()
