import os
import logging
import time


LOG_PATH = os.path.join(os.path.realpath('.'), 'trade.log')


def get_logger():
    log_path_real = os.path.realpath(LOG_PATH)
    if not os.path.exists(log_path_real):
        with open(log_path_real, 'w'):
            pass
    log_format = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] [%(filename)s] [%(funcName)s:%(lineno)d] [PID:%(process)d TID:%(thread)d] %(message)s")
    logger = logging.getLogger('trade')
    logger.setLevel(logging.INFO)

    file_handler = logging.FileHandler(LOG_PATH, encoding='utf-8')
    file_handler.setFormatter(log_format)

    logger.addHandler(file_handler)

    return logger


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
        count = 1
        while count <= 10:
            coin_types = storage.db_inst.execute('select distinct coin_type from wave_rate;')
            if len(coin_types) == 0:
                time.sleep(15)
                count += 1
                continue
            else:
                coin_types = [item[0] for item in coin_types]
                coin_types.sort()
                widget['values'] = coin_types
                widget.set(coin_types[0])
                return
    except Exception as e:
        logger.exception('load ontime coin type error: %s', e)


import storage
