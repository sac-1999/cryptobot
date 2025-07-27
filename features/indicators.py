import pandas_ta as ta
import pandas as pd

class Indicators:
    @staticmethod
    def ema(data, length):
        """Exponential Moving Average"""
        colname = f'ema_{length}'
        data[colname] = ta.ema(data['close'], length=length)
        return data

    @staticmethod
    def supertrend(data, length, multiplier):
        """Supertrend - only use the main trend line"""
        st = ta.supertrend(high=data['high'], low=data['low'], close=data['close'],
                           length=length, multiplier=multiplier)
        colname = f'supertrend_{length}_{multiplier}'
        data[colname] = st[f'SUPERT_{length}_{multiplier}.0']
        return data

    @staticmethod
    def vwap(data):
        """VWAP"""
        data.index = pd.to_datetime(data['timestamp'])
        colname = f'vwap'
        data[colname] = ta.vwap(data['high'], data['low'], data['close'], data['volume'])
        data.reset_index(drop=True, inplace=True)
        return data

    @staticmethod
    def local_maxima(data, window):
        """Local high maxima"""
        rolling_max = data['high'].rolling(window, center=True, min_periods=window).max()
        colname = f'local_max_{window}'
        data[colname] = (data['high'] == rolling_max).astype(int)
        return data

    @staticmethod
    def local_minima(data, window):
        """Local low minima"""
        rolling_min = data['low'].rolling(window, center=True, min_periods=window).min()
        colname = f'local_min_{window}'
        data[colname] = (data['low'] == rolling_min).astype(int)
        return data

    @staticmethod
    def rsi(data, length):
        """RSI"""
        colname = f'rsi_{length}'
        data[colname] = ta.rsi(data['close'], length=length)
        return data

    @staticmethod
    def atr(data, length):
        """Average True Range"""
        colname = f'atr_{length}'
        data[colname] = ta.atr(data['high'], data['low'], data['close'], length=length)
        return data

    @staticmethod
    def macd(data):
        """MACD Histogram only"""
        macd_data = ta.macd(data['close'])
        data['macd'] = macd_data['MACDh_12_26_9']
        return data

    @staticmethod
    def williams_r(data, length):
        """Williams %R"""
        colname = f'williamsr_{length}'
        data[colname] = ta.willr(high=data['high'], low=data['low'], close=data['close'], length=length)
        return data
