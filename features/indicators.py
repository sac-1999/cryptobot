import pandas as pd
import numpy as np

def sma_pct_from_pclose(data, length):
    """
    Percentage difference between previous close and SMA(length),
    normalized by previous close.
    """
    prev_close = data['close'].shift(1)
    sma = data['close'].rolling(length).mean()
    col = f'sma_pct_pclose_{length}'
    data[col] = (prev_close - sma) / prev_close
    return data

class Indicators:
    @staticmethod
    def ema(data, length):
        """Exponential Moving Average"""
        colname = f'ema_{length}'
        data[colname] = data['close'].ewm(span=length, adjust=False).mean()
        return data

    @staticmethod
    def supertrend(data, length, multiplier):
        """
        Supertrend implementation (trend line only)
        Formula:
            ATR = Average True Range
            UpperBand = (High+Low)/2 + multiplier*ATR
            LowerBand = (High+Low)/2 - multiplier*ATR
            Trend flips when close crosses bands
        """
        # True Range
        hl = data['high'] - data['low']
        hc = (data['high'] - data['close'].shift()).abs()
        lc = (data['low'] - data['close'].shift()).abs()
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)

        # ATR
        atr = tr.rolling(length).mean()

        # Basic bands
        hl2 = (data['high'] + data['low']) / 2
        upperband = hl2 + multiplier * atr
        lowerband = hl2 - multiplier * atr

        # Supertrend line
        st = pd.Series(index=data.index, dtype=float)
        direction = pd.Series(1, index=data.index)  # 1 = uptrend, -1 = downtrend

        st.iloc[0] = hl2.iloc[0]  # start value
        for i in range(1, len(data)):
            if data['close'].iloc[i] > upperband.iloc[i - 1]:
                direction.iloc[i] = 1
            elif data['close'].iloc[i] < lowerband.iloc[i - 1]:
                direction.iloc[i] = -1
            else:
                direction.iloc[i] = direction.iloc[i - 1]
                if direction.iloc[i] == 1 and lowerband.iloc[i] < lowerband.iloc[i - 1]:
                    lowerband.iloc[i] = lowerband.iloc[i - 1]
                if direction.iloc[i] == -1 and upperband.iloc[i] > upperband.iloc[i - 1]:
                    upperband.iloc[i] = upperband.iloc[i - 1]

            st.iloc[i] = lowerband.iloc[i] if direction.iloc[i] == 1 else upperband.iloc[i]

        colname = f'supertrend_{length}_{multiplier}'
        data[colname] = st
        return data

    @staticmethod
    def vwap(data):
        """Volume Weighted Average Price"""
        typical_price = (data['high'] + data['low'] + data['close']) / 3
        vwap = (typical_price * data['volume']).cumsum() / data['volume'].cumsum()
        data['vwap'] = vwap
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
        """Relative Strength Index"""
        delta = data['close'].diff()
        gain = np.where(delta > 0, delta, 0.0)
        loss = np.where(delta < 0, -delta, 0.0)

        avg_gain = pd.Series(gain).rolling(length).mean()
        avg_loss = pd.Series(loss).rolling(length).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        colname = f'rsi_{length}'
        data[colname] = rsi
        return data

    @staticmethod
    def atr(data, length):
        """Average True Range"""
        hl = data['high'] - data['low']
        hc = (data['high'] - data['close'].shift()).abs()
        lc = (data['low'] - data['close'].shift()).abs()
        tr = pd.concat([hl, hc, lc], axis=1).max(axis=1)
        colname = f'atr_{length}'
        data[colname] = tr.rolling(length).mean()
        return data

    @staticmethod
    def macd(data, fast=12, slow=26, signal=9):
        """MACD Histogram only"""
        ema_fast = data['close'].ewm(span=fast, adjust=False).mean()
        ema_slow = data['close'].ewm(span=slow, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
        data['macd'] = macd_line - signal_line
        return data

    @staticmethod
    def williams_r(data, length):
        """Williams %R"""
        highest_high = data['high'].rolling(length).max()
        lowest_low = data['low'].rolling(length).min()
        willr = -100 * (highest_high - data['close']) / (highest_high - lowest_low)
        colname = f'williamsr_{length}'
        data[colname] = willr
        return data