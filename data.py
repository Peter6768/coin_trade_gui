import okx.PublicData
import okx.MarketData

import utils

logger = utils.get_logger()

FLAG = 1    # 0: real trade, 1: simulate trade


def get_all_coin_name(coin_type='SWAP'):
    """/api/v5/public/instruments"""
    coin_names = []
    try:
        publicdata_api = okx.PublicData.PublicAPI(flag=FLAG)
        rst = publicdata_api.get_instruments(instType=coin_type)
        if not rst['code']:
            logger.error('get coin name error. return code is %s, error occur: %s', rst['code'], rst['msg'])
            raise Exception('get coin name error')
        coin_names = [i for i in {j['instId'] for j in rst['data'] if ('usdt' in j['instId'].lower() and j['instFamily'] == 'SWAP')}]
        coin_names.sort()
    except Exception as e:
        logger.exception('get all coin name via public data api error: %s', e)
    finally:
        return coin_names


def get_kline_data(timespan=15):
    """
    /api/v5/market/candles
    get recently 15 days coin kline data
    """
    coin_names = get_all_coin_name()
    if not coin_names:
        logger.error('try to get all coin names but result is empty. can not continue get k line data')
    marketdata_api = okx.MarketData.MarketAPI(flag=FLAG)
    coin_kline_data = {}
    try:
        for coin_name in coin_names:
            rst = marketdata_api.get_candlesticks(instId=coin_name, bar='1D', limit=timespan)
            coin_kline_data[coin_name] = rst['data']
    except Exception as e:
        logger.exception('get coin kline data error: %s', e)
    finally:
        return coin_kline_data
