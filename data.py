from concurrent import futures
import time
# from httpcore import ConnectError as httpcore_ConnectError
from httpx import ConnectError as httpx_ConnectError

import okx.PublicData
import okx.MarketData
from tkinter.messagebox import showinfo

import utils

logger = utils.get_logger()

FLAG = '0'    # 0: real trade, 1: simulate trade


def get_all_coin_name(coin_type='SWAP'):
    """/api/v5/public/instruments"""
    publicdata_api = okx.PublicData.PublicAPI(flag=FLAG)
    retry_count = 1
    while retry_count <= 3:
        try:
            rst = publicdata_api.get_instruments(instType=coin_type)
            if not rst['code']:
                logger.error('get coin name error. return code is %s, error occur: %s', rst['code'], rst['msg'])
                raise Exception('get coin name error')
            coin_names = [i for i in {j['instId'] for j in rst['data'] if ('usdt' in j['instId'].lower() and j['instType'] == 'SWAP' and ('test' not in j['instId'].lower()))}]
            coin_names.sort()
        except Exception as e:
            logger.exception('get all coin name via public data api error: %s', e)
            retry_count += 1
            time.sleep(5)
            continue
        publicdata_api.close()
        return coin_names


def get_one_coin_kline(coin_type, begin_timestamp, end_timestamp):
    marketdata_api = okx.MarketData.MarketAPI(flag=FLAG)
    count = 1
    while count <= 3:
        try:
            resp = marketdata_api.get_candlesticks(instId=coin_type, bar='5m', limit=24 * 12, before=begin_timestamp, after=end_timestamp)
        except httpx_ConnectError as e:
            logger.exception('please open vpn and restart this program. params: %s, detail: %s', (begin_timestamp, end_timestamp), e)
            showinfo('提示', '无法采集数据, 请打开vpn后重启程序')
            exit(1)
        except Exception as e:
            logger.exception('get coin %s kline data error: %s', coin_type, e)
            count += 1
            continue
        code = int(resp['code'])
        if code == 50011:
            logger.error('request kline data for coin %s is too fast, sleep and retry', coin_type)
            time.sleep(20)
            continue
        elif code:
            logger.error('response code error: %s, msg: %s', resp['code'], resp.get('msg', ''))
            return
        else:
            marketdata_api.close()
            return resp['data']


def get_kline_data(timespan=90, before=None, after=None):
    """
    /api/v5/market/candles
    get recently {timespan} days coin kline data
    """
    coin_names = get_all_coin_name()
    if not coin_names:
        logger.error('try to get all coin names but result is empty. can not continue get k line data')
    marketdata_api = okx.MarketData.MarketAPI(flag=FLAG)
    coin_kline_data = {}

    def get_coin_kline(coin_name_inner):
        count = 1
        while count <= 5:
            try:
                resp = marketdata_api.get_candlesticks(instId=coin_name_inner, bar='1D', limit=timespan, before=before, after=after)
            except httpx_ConnectError as e:
                logger.exception('please open vpn and restart this program. params: %s, detail: %s', (before, after), e)
                showinfo('提示', '无法采集数据, 请打开vpn后重启程序')
                exit(1)
            except Exception as e:
                logger.error('get coin %s kline data error, retry num: %s: %s', coin_name_inner, count, e)
                time.sleep(2)
                continue
            code = int(resp['code'])
            if code == 50011:
                logger.info('request for %s kline data frequency too fast, retry no.%s time', coin_name_inner, count)
                count += 1
                time.sleep(2)
                continue
            elif code:
                logger.error('get coin %s kline data error: %s', coin_name_inner, resp['data'])
                return
            else:
                coin_kline_data[coin_name_inner] = [i[:5] for i in resp['data']]
                time.sleep(2.1)
                return

    try:
        with futures.ThreadPoolExecutor(max_workers=15) as executor:
            tasks = [executor.submit(get_coin_kline, coin_name) for coin_name in coin_names]
            for _ in futures.as_completed(tasks):
                pass
    except Exception as e:
        logger.exception('get coin kline data error: %s', e)
    finally:
        marketdata_api.close()
        return coin_kline_data
