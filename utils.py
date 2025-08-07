import os
import logging

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

    file_handler = logging.FileHandler(LOG_PATH)
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
