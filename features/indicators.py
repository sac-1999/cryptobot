import pandas as pd
import numpy as np


class Indicators:
    @staticmethod
    def ema(data, col, length):
        """Exponential Moving Average normalized by EMA"""
        colname = f'ema_{col}_{length}'
        # EMA of the previous value
        data[colname] = (
            data[col]
            .shift(1)
            .ewm(span=length, adjust=False, ignore_na=True, min_periods=length//2)
            .mean()
        )
        # Normalize difference from current value
        data[colname] = (data[col] - data[colname]) / data[colname]
        return data

    @staticmethod
    def adx(data, length=14):
        """Average Directional Index (+DI and -DI) normalized by ADX."""
        # True Range
        tr1 = data['high'] - data['low']
        tr2 = (data['high'] - data['close'].shift()).abs()
        tr3 = (data['low'] - data['close'].shift()).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    
        # Directional Movement
        up_move = data['high'].diff()
        down_move = -data['low'].diff()
    
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    
        # ATR with Wilder smoothing
        atr = tr.ewm(
            span=length, adjust=False, ignore_na=True, min_periods=length//2
        ).mean()
    
        plus_di = 100 * pd.Series(plus_dm, index=data.index).ewm(
            span=length, adjust=False, ignore_na=True, min_periods=length//2
        ).mean() / atr
    
        minus_di = 100 * pd.Series(minus_dm, index=data.index).ewm(
            span=length, adjust=False, ignore_na=True, min_periods=length//2
        ).mean() / atr
    
        dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    
        adx = dx.ewm(
            span=length, adjust=False, ignore_na=True, min_periods=length//2
        ).mean()
    
        # Add columns
        data[f'plus_di_{length}'] = plus_di
        data[f'minus_di_{length}'] = minus_di
        data[f'adx_{length}'] = adx
    
        return data

    @staticmethod
    def eom(data, length=14):
        """
        Ease of Movement (EOM) with EMA smoothing, then return normalization.
        Adds:
            eom_raw               - EOM in % change form
            eom_ema_<length>      - EMA of EOM in % change form
        """
        # Avoid division by zero in box ratio
        hl_range = (data['high'] - data['low']).replace(0, np.nan)
        box_ratio = (data['volume'] / 1_000_000) / hl_range
    
        # Raw EOM formula
        eom_raw = ((data['high'] + data['low']) / 2 - (data['high'].shift(1) + data['low'].shift(1)) / 2) / box_ratio
    
        # EMA smoothing
        eom_ema = eom_raw.ewm(
            span=length, adjust=False, ignore_na=True, min_periods=length//2
        ).mean()
    
        # Convert both to return form
        data['eom_raw'] = eom_raw / eom_raw.shift(1) - 1
        data[f'eom_ema_{length}'] = eom_ema / eom_ema.shift(1) - 1
    
        return data
    
    @staticmethod
    def adl(data, length=14):
        """
        Accumulation/Distribution Line with EMA smoothing, then return normalization.
        Adds:
            adl_raw               - ADL in % change form
            adl_ema_<length>      - EMA of ADL in % change form
        """
        hl_range = (data['high'] - data['low']).replace(0, np.nan)
        mfm = ((data['close'] - data['low']) - (data['high'] - data['close'])) / hl_range
        mfv = mfm.fillna(0) * data['volume']
    
        # Step 1: raw cumulative ADL
        adl_raw = mfv.cumsum()
    
        # Step 2: EMA of raw ADL
        adl_ema = adl_raw.ewm(
            span=length, adjust=False, ignore_na=True, min_periods=length//2
        ).mean()
    
        # Step 3: Convert both to return form
        data['adl_raw'] = adl_raw / adl_raw.shift(1) - 1
        data[f'adl_ema_{length}'] = adl_ema / adl_ema.shift(1) - 1
    
        return data

    @staticmethod
    def cci(data, length=20):
        """
        Commodity Channel Index (CCI) with EMA smoothing, then return normalization.
        Adds:
            cci_raw               - CCI in % change form
            cci_ema_<length>      - EMA of CCI in % change form
        """
        
        # Typical Price
        tp = (data['high'] + data['low'] + data['close']) / 3
        
        # Mean Deviation
        mad = tp.rolling(length, min_periods=length//2).apply(lambda x: np.mean(np.abs(x - np.mean(x))), raw=True)
        # Raw CCI
        cci_raw = (tp - tp.rolling(length, min_periods=length//2).mean()) / (0.015 * mad)
    
        # EMA smoothing
        cci_ema = cci_raw.ewm(
            span=length, adjust=False, ignore_na=True, min_periods=length//2
        ).mean()
    
        # Convert to return form
        data['cci_raw'] = cci_raw / cci_raw.shift(1) - 1
        data[f'cci_ema_{length}'] = cci_ema / cci_ema.shift(1) - 1
    
        return data

    @staticmethod
    def atr_bands(data, length=20):
        """
        Volatility Bands based on SMA ± ATR, normalized by HLC3.
        ATR length = SMA length.
        Adds:
            l_atr_<length> - (SMA - ATR) / HLC3
            r_atr_<length> - (SMA + ATR) / HLC3
        """
        # Typical Price (HLC3)
        hlc3 = (data['high'] + data['low'] + data['close']) / 3
    
        # SMA of close
        sma = data['close'].rolling(length).mean()
    
        # True Range
        high_low = data['high'] - data['low']
        high_close_prev = (data['high'] - data['close'].shift(1)).abs()
        low_close_prev = (data['low'] - data['close'].shift(1)).abs()
        tr = pd.concat([high_low, high_close_prev, low_close_prev], axis=1).max(axis=1)
    
        # ATR with same length
        atr = tr.rolling(length).mean()
    
        # Raw bands with new names
        data[f'l_atr_{length}'] = (sma - atr) / hlc3
        data[f'r_atr_{length}'] = (sma + atr) / hlc3
    
        return data
    @staticmethod
    def chaikin_oscillator(data, short_length=3, long_length=10):
        """
        Chaikin Oscillator: (ADL(short) - ADL(long)) / (ADL(short) + ADL(long))
        Adds:
            chaikin_raw_<short>_<long>
        """
        # Money Flow Multiplier
        mfm = ((data['close'] - data['low']) - (data['high'] - data['close'])) / (data['high'] - data['low'])
        mfm = mfm.fillna(0)  # handle divide-by-zero
    
        # Accumulation/Distribution Line (ADL)
        adl = (mfm * data['volume']).cumsum()
    
        # Short and long SMA (no EMA)
        adl_short = adl.rolling(short_length).mean()
        adl_long = adl.rolling(long_length).mean()
    
        # Normalized Chaikin Oscillator
        chaikin = (adl_short - adl_long) / (adl_short + adl_long)
    
        data[f'chaikin_raw_{short_length}_{long_length}'] = chaikin
    
        return data

    @staticmethod
    def mfi(data, length=14):
        """
        Normalized Money Flow Index (MFI) in range [-1, 1].
        Adds:
            mfi_raw_<length> - normalized MFI
        """
        # Typical Price
        tp = (data['high'] + data['low'] + data['close']) / 3
    
        # Raw Money Flow
        raw_mf = tp * data['volume']
    
        # Positive / Negative Money Flow
        pos_mf = raw_mf.where(tp > tp.shift(1), 0.0)
        neg_mf = raw_mf.where(tp < tp.shift(1), 0.0)
    
        # Sum over length
        pos_sum = pos_mf.rolling(length).sum()
        neg_sum = neg_mf.rolling(length).sum()
    
        # MFI (0–100)
        mfi = 100 * (pos_sum / (pos_sum + neg_sum))
        mfi = mfi.fillna(50)  # neutral at midpoint
    
        # Normalize to [-1, 1]
        mfi_norm = (mfi - 50) / 50
    
        data[f'mfi_raw_{length}'] = mfi_norm
    
        return data

    @staticmethod
    def obv(data, length=20):
        """
        On-Balance Volume (OBV) with EMA smoothing.
        Adds:
            obv_ema_<length> - EMA-smoothed OBV in return form
        """
        obv = (np.sign(data['close'] - data['close'].shift(1)) * data['volume']).fillna(0).cumsum()
    
        obv_ema = obv.ewm(span=length, adjust=False, ignore_na=True, min_periods=length//2).mean()
    
        # Convert to return form
        data[f'obv_ema_{length}'] = obv_ema / obv_ema.shift(1) - 1
    
        return data

    @staticmethod
    def efi(data, length=20):
        """
        Elder's Force Index (EFI) with EMA smoothing.
        Adds:
            efi_ema_<length> - EMA-smoothed EFI in return form
        """
        efi = (data['close'] - data['close'].shift(1)) * data['volume']
    
        efi_ema = efi.ewm(span=length, adjust=False, ignore_na=True, min_periods=length//2).mean()
    
        # Convert to return form
        data[f'efi_ema_{length}'] = efi_ema / efi_ema.shift(1) - 1
    
        return data
    @staticmethod
    def bbands(data, length=20, num_std=2):
        """
        Bollinger Bands.
        Adds:
            bbands_upper_raw_<length> - (upper band / close) in return form
            bbands_lower_raw_<length> - (lower band / close) in return form
        """
        sma = data['close'].rolling(length).mean()
        std = data['close'].rolling(length).std()
    
        upper_band = sma + num_std * std
        lower_band = sma - num_std * std
    
        data[f'bbands_upper_raw_{length}'] = upper_band / data['close'] - 1
        data[f'bbands_lower_raw_{length}'] = lower_band / data['close'] - 1
    
        return data

    @staticmethod
    def macd(data, fast=12, slow=26):
        """
        Normalized Moving Average Convergence Divergence (MACD).
        Adds:
            macd_<fast>_<slow> - (EMA_fast - EMA_slow) / (EMA_fast + EMA_slow)
        """
        ema_fast = data['close'].ewm(span=fast, adjust=False, ignore_na=True, min_periods=fast//2).mean()
        ema_slow = data['close'].ewm(span=slow, adjust=False, ignore_na=True, min_periods=slow//2).mean()
    
        data[f'macd_{fast}_{slow}'] = (ema_fast - ema_slow) / (ema_fast + ema_slow)
    
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
        data[colname] = (data['close'] - st)/data['close']
        return data

    @staticmethod
    def vwap(data):
        """Volume Weighted Average Price"""
        typical_price = (data['high'] + data['low'] + data['close']) / 3
        vwap = (typical_price * data['volume']).cumsum() / data['volume'].cumsum()
        data['vwap'] = (data['close'] - vwap)/data['close']
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
