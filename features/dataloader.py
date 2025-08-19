import pandas as pd
import requests
from datetime import datetime, timedelta
import pytz
from cacher import persistent_cache

# @persistent_cache(subdir="api_call", non_empty=False)
def get_binance_klines(symbol: str, interval: str, start_time: datetime, end_time: datetime, local_timezone: str = "Asia/Kolkata") -> pd.DataFrame:
    """
    Fetch 1-minute kline data from Binance between start_time and end_time (both datetime objects).
    All times should be timezone-aware UTC.
    """
    # Convert start and end times to UTC timestamps in milliseconds
    start_time = start_time.astimezone(pytz.UTC)
    end_time = end_time.astimezone(pytz.UTC)
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    url = "https://api.binance.com/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 1000,
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data for {symbol}: {response.text}")

    data = response.json()
    if not data:
        return pd.DataFrame()

    df = pd.DataFrame(data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "num_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore"
    ])

    df = df.astype({
        "open": float,
        "high": float,
        "low": float,
        "close": float,
        "volume": float,
        "quote_asset_volume": float,
        "num_trades": int,
        "taker_buy_base_volume": float,
        "taker_buy_quote_volume": float,
    })

    # Convert open/close time to local timezone
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_convert(local_timezone)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.tz_convert(local_timezone)

    return df


def build_manual_candle(df: pd.DataFrame) -> dict:
    """
    Manually build OHLCV candle from 1-minute data.
    """
    return {
        "open_time": df["open_time"].min(),
        "close_time": df["close_time"].max(),
        "open": df["open"].iloc[0],
        "high": df["high"].max(),
        "low": df["low"].min(),
        "close": df["close"].iloc[-1],
        "volume": df["volume"].sum(),
        "quote_asset_volume": df["quote_asset_volume"].sum(),
        "num_trades": df["num_trades"].sum(),
        "taker_buy_base_volume": df["taker_buy_base_volume"].sum(),
        "taker_buy_quote_volume": df["taker_buy_quote_volume"].sum(),
    }

@persistent_cache(subdir="snapshot_bars", non_empty=False)
def get_symbol_snapshot_bar(symbol: str, date_tm: datetime, interval: str, local_timezone: str = "Asia/Kolkata") -> pd.DataFrame:
    """
    For a given symbol and datetime, get the snapshot candle for the preceding interval.
    Example: If interval is '15min' and datetime is 15:30, we get candle from 15:15 to 15:30.
    """
    if not isinstance(date_tm, datetime):
        raise TypeError("`date_tm` must be a datetime object.")

    tz = pytz.timezone(local_timezone)
    if date_tm.tzinfo is None:
        local_dt = tz.localize(date_tm)
    else:
        local_dt = date_tm.astimezone(tz)

    now = datetime.now(tz)
    if local_dt + pd.to_timedelta('1m')>= now:
        raise ValueError(
            f"[Forward Bias Detected] ❌ Attempted to fetch data for future timestamp.\n"
            f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"Requested time: {local_dt.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Wait for 1min bar to complete maybe!!"
        )

    try:
        interval_td = pd.to_timedelta(interval)
    except Exception:
        raise ValueError(f"Invalid interval format: {interval}. Try formats like '15min', '1H' etc.")

    n_bars = int(interval_td / pd.Timedelta(minutes=1))
    start_dt = local_dt - interval_td

    df = get_binance_klines(
        symbol=symbol,
        interval="1m",
        start_time=start_dt - timedelta(minutes=5),
        end_time=local_dt,
        local_timezone=local_timezone
    )

    if df.empty:
        raise ValueError(f"No 1-minute data returned for {symbol} between {start_dt} and {local_dt}.")

    df = df[df["open_time"] < local_dt].tail(n_bars)

    if len(df) < n_bars:
        raise ValueError(
            f"Not enough 1-minute bars to build a {interval} candle for {symbol} ending at {local_dt}.\n"
            f"Expected {n_bars}, got {len(df)}.\n"
            f"Possibly the latest candle is still forming — try again after the interval completes."
        )

    candle = build_manual_candle(df)
    candle["symbol"] = symbol
    df = pd.DataFrame([candle])
    df['timestamp'] = pd.to_datetime(local_dt)
    return df

def get_last_n_snapshot_bars(symbol: str, end_time: datetime, interval: str, n_bars: int, local_timezone: str = "Asia/Kolkata") -> pd.DataFrame:
    """
    Get the last `n_bars` snapshot candles for the given symbol ending at `end_time`.

    Parameters
    ----------
    symbol : str
        Trading pair symbol, e.g. "BTCUSDT".
    end_time : datetime
        The last timestamp to include (inclusive). Should be in local time or timezone-aware.
    interval : str
        Candle interval, e.g. '15min', '1H', etc.
    n_bars : int
        Number of snapshot bars to fetch.
    local_timezone : str, optional
        Timezone to localize/convert timestamps to.

    Returns
    -------
    pd.DataFrame
        DataFrame containing the last `n_bars` snapshot candles ending at `end_time`.
    """
    tz = pytz.timezone(local_timezone)
    if end_time.tzinfo is None:
        end_time = tz.localize(end_time)
    else:
        end_time = end_time.astimezone(tz)

    all_bars = []
    current_time = end_time

    for _ in range(n_bars):
        df_bar = get_symbol_snapshot_bar(
            symbol=symbol,
            date_tm=current_time,
            interval=interval,
            local_timezone=local_timezone
        )
        all_bars.append(df_bar)
        current_time -= pd.to_timedelta(interval)  # Step back one interval

    result_df = pd.concat(all_bars, ignore_index=True)
    result_df = result_df.sort_values("timestamp").reset_index(drop=True)
    return result_df
