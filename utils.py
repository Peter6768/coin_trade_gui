from os import path
import logging
from logging.handlers import TimedRotatingFileHandler
import time
from datetime import datetime as dt


LOG_PATH = path.join(path.realpath('.'), 'trade.log')


def get_logger():
    log_path_real = path.realpath(LOG_PATH)
    if not path.exists(log_path_real):
        with open(log_path_real, 'w'):
            pass
    log_format = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] [%(filename)s] [%(funcName)s:%(lineno)d] [PID:%(process)d TID:%(thread)d] %(message)s")
    logger_inner = logging.getLogger('trade')
    logger_inner.setLevel(logging.INFO)

    # file_handler = logging.FileHandler(LOG_PATH, encoding='utf-8')
    # file_handler.setFormatter(log_format)

    # logger_inner.addHandler(file_handler)
    handler = TimedRotatingFileHandler(filename=LOG_PATH, when='midnight', interval=1, backupCount=3, encoding='utf-8')
    handler.setFormatter(log_format)

    logger_inner.addHandler(handler)

    return logger_inner


def activate_widget(*args, **kwargs):
    if kwargs.get('special', {}):
        for k, v in kwargs.get('special').items():
            k['state'] = v
    for i in args:
        i['state'] = 'normal'


def disable_widget(*args):
    for i in args:
        i['state'] = 'disabled'


logger = get_logger()


def load_ontime_coin_type_thread(widget, *args):
    try:
        coin_types = storage.db_inst.execute('select distinct coin_type from wave_rate;')
        if len(coin_types) == 0:
            return
        else:
            coin_types = [item[0] for item in coin_types]
            coin_types.sort()
            widget['values'] = coin_types
            widget.set(coin_types[0])
            return
    except Exception as e:
        logger.exception('load ontime coin type error: %s', e)


def timestamp_to_date(d, fmt=None):
    return dt.fromtimestamp(d).strftime(fmt or '%Y-%m-%d')


import storage
