import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import cacher
from indicators import Indicators
import dataloader

def multiday_data(symbol, enddate, freq, local_timezone="Asia/Kolkata"):
    """
    Get at least 252 candles ending at enddate for a given symbol & frequency.
    Returns sorted dataframe by timestamp.
    """
    totalcandles = 0
    dflist = []

    enddate = pd.to_datetime(enddate)

    while totalcandles < 252:
        df = dataloader.get_last_n_snapshot_bars(
            symbol=symbol,
            end_time=enddate,
            interval=freq,
            n_bars=252,
            local_timezone=local_timezone
        )

        if df is not None and not df.empty:
            df = df.sort_values("timestamp").reset_index(drop=True)
            dflist.insert(0, df)
            totalcandles += len(df)

        enddate -= timedelta(days=1)

        if enddate.year <= 2020:
            break

    if not dflist:
        return None

    return pd.concat(dflist).drop_duplicates(
        subset=["timestamp"]
    ).sort_values("timestamp").reset_index(drop=True)


@cacher.persistent_cache(subdir="Comp_indicator_big", non_empty=True)
def compute(symbol, date_tm, freq, local_timezone="Asia/Kolkata"):
    fetch_date_tm = pd.to_datetime(date_tm) - timedelta(seconds=30)

    df = multiday_data(symbol, fetch_date_tm, freq, local_timezone=local_timezone)
    if df is None or df.empty:
        return None

    df.reset_index(drop=True, inplace=True)

    lengths = [14, 21, 30, 60, 100, 150, 200]

    # EMA
    for length in lengths:
        df = Indicators.ema(df, col="close", length=length)

    # ADX
    for length in lengths:
        df = Indicators.adx(df, length=length)

    # EOM
    for length in lengths:
        df = Indicators.eom(df, length=length)

    # ADL
    for length in lengths:
        df = Indicators.adl(df, length=length)

    # CCI
    for length in lengths:
        df = Indicators.cci(df, length=length)

    # ATR bands
    for length in lengths:
        df = Indicators.atr_bands(df, length=length)

    # Chaikin Oscillator
    for length in lengths:
        short_len = max(2, length // 5)
        long_len = max(short_len + 1, length // 2)
        df = Indicators.chaikin_oscillator(df, short_length=short_len, long_length=long_len)

    # MFI
    for length in lengths:
        df = Indicators.mfi(df, length=length)

    # OBV
    for length in lengths:
        df = Indicators.obv(df, length=length)

    # EFI
    for length in lengths:
        df = Indicators.efi(df, length=length)

    # Bollinger Bands
    for length in lengths:
        df = Indicators.bbands(df, length=length, num_std=2)

    # MACD
    df = Indicators.macd(df, fast=12, slow=26)
    for length in lengths:
        fast_len = max(5, length // 2)
        slow_len = max(fast_len + 1, length)
        df = Indicators.macd(df, fast=fast_len, slow=slow_len)

    # Supertrend
    for length in lengths:
        df = Indicators.supertrend(df, length, multiplier=2)

    # VWAP
    df = Indicators.vwap(df)

    # RSI
    for length in lengths:
        df = Indicators.rsi(df, length=length)

    # Drop incomplete rows
    df.dropna(inplace=True)

    # Prepare snapshot
    snapshot = df.tail(1).copy().reset_index(drop=True)
    snapshot['timestamp'] = pd.to_datetime(date_tm)

    # Collect feature columns
    main_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'open_time', 'close_time', 'quote_asset_volume' , 'num_trades', 'taker_buy_base_volume' 'taker_buy_quote_volume', 'symbol', 'taker_buy_base_volume',  'taker_buy_quote_volume']
    feature_cols = [col for col in snapshot.columns if col not in main_columns]

    return snapshot[['timestamp', 'symbol'] + feature_cols]
