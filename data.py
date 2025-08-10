import concurrent
import time

import okx.PublicData
import okx.MarketData

import utils

logger = utils.get_logger()

FLAG = '0'    # 0: real trade, 1: simulate trade


def get_all_coin_name(coin_type='SWAP'):
    """/api/v5/public/instruments"""
    coin_names = []
    try:
        publicdata_api = okx.PublicData.PublicAPI(flag=FLAG)
        rst = publicdata_api.get_instruments(instType=coin_type)
        if not rst['code']:
            logger.error('get coin name error. return code is %s, error occur: %s', rst['code'], rst['msg'])
            raise Exception('get coin name error')
        coin_names = [i for i in {j['instId'] for j in rst['data'] if ('usdt' in j['instId'].lower() and j['instType'] == 'SWAP' and ('test' not in j['instId'].lower()))}]
        coin_names.sort()
    except Exception as e:
        logger.exception('get all coin name via public data api error: %s', e)
    finally:
        return coin_names


def get_kline_data(timespan=90, before=None):
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
                resp = marketdata_api.get_candlesticks(instId=coin_name_inner, bar='1D', limit=timespan, before=before)
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
        with concurrent.futures.ThreadPoolExecutor(max_workers=15) as executor:
            futures = [executor.submit(get_coin_kline, coin_name) for coin_name in coin_names]
            for _ in concurrent.futures.as_completed(futures):
                pass
    except Exception as e:
        logger.exception('get coin kline data error: %s', e)
    finally:
        return coin_kline_data
