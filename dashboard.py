import tkinter as tk
from tkinter import ttk


def data_collect_panel(root, notebook):
    tab = ttk.Frame(notebook)
    notebook.add(tab, text='数据采集综合面板')
    notebook.pack(fill='both', expand=True)

    ttk.Label(tab, text='行情采集').pack(side='left')
    collect_data_radio = tk.StringVar()
    collect_data_radio1 = ttk.Radiobutton(tab, text='采集', variable=collect_data_radio, value='yes').pack(side='left')
    collect_data_radio2 = ttk.Radiobutton(tab, text='停止采集', variable=collect_data_radio, value='no').pack(side='left')

    # aa = tk.StringVar(value='option1')
    # radio1 = tk.Radiobutton(tab, text='opt1', variable=aa, value='opt1').pack()
    # radio2 = tk.Radiobutton(tab, text='opt2', variable=aa, value='opt2').pack()

    # color = tk.StringVar(value='red')
    # ttk.Radiobutton(tab, text='red', variable=color, value='red').pack(anchor='w')
    # ttk.Radiobutton(tab, text='blue', variable=color, value='blue').pack(anchor='w')
    # ttk.Button(tab, text='get color', command=lambda: print(color.get())).pack(pady=10)

def buy_sell_panel(root, notebook):
    tab = ttk.Frame(notebook)
    notebook.add(tab, text='清仓/挂单综合面板')
    notebook.pack(fill='both', expand=True)

def ontime_data_view_panel(root, notebook):
    tab = ttk.Frame(notebook)
    notebook.add(tab, text='实时数据监控面板')
    notebook.pack(fill='both', expand=True)


def main():
    root = tk.Tk()
    notebook_control = ttk.Notebook(root)
    notebook_view = ttk.Notebook(root)

    data_collect_panel(root, notebook_control)
    buy_sell_panel(root, notebook_control)
    ontime_data_view_panel(root, notebook_view)
    root.mainloop()

if __name__ == '__main__':
    main()