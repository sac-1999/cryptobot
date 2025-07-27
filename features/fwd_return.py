import dataloader
import pandas as pd
from datetime import datetime
import pytz
import cacher

@cacher.load_or_save_pickle(subdir='fwd_ret',  )
def fwd_return(symbol, date_tm, freq, verbose=1):
    tz = pytz.timezone('Asia/Kolkata')

    # Ensure input datetime is timezone-aware
    date_tm = pd.to_datetime(date_tm)
    if date_tm.tzinfo is None or date_tm.tzinfo.utcoffset(date_tm) is None:
        date_tm = date_tm.tz_localize(tz)
    else:
        date_tm = date_tm.tz_convert(tz)

    if date_tm >= datetime.now(tz):
        raise Exception(
            f'Forward bias in live data! Time right now is {datetime.now(tz)} '
            f'but trying to fetch data for {date_tm}'
        )

    if verbose:
        print(f"📥 Requested time: {date_tm}, frequency: {freq}")

    lag = pd.to_timedelta(freq)
    fetch_date_tm = date_tm + lag

    if verbose:
        print(f"📅 Fetching data from: {fetch_date_tm}")

    df = dataloader.get(symbol, fetch_date_tm)

    if verbose:
        print(f"📊 Raw data from dataloader:\n{df}")

    df = df.sort_values("timestamp").reset_index(drop=True)

    df["timestamp"] = pd.to_datetime(df["timestamp"])
    if df["timestamp"].dt.tz is None:
        df["timestamp"] = df["timestamp"].dt.tz_localize(tz)
    else:
        df["timestamp"] = df["timestamp"].dt.tz_convert(tz)

    # orgdf = df.copy()
    df = df[df['timestamp'] >= date_tm]
    if df.empty:
        return pd.DataFrame(columns= ['timestamp', 'Fwd_Ret'])
    # try:
    #     (df["close"].iloc[-1] - df['close'].iloc[0])/df['close'].iloc[0]
    # except Exception as e:
    #     print("filtetered df : ",df)
    #     print(date_tm)
    #     print("orgdf :" , orgdf)
    #     print(str(e))

    df["Fwd_Ret"] = (df["close"].iloc[-1] - df['close'].iloc[0])/df['close'].iloc[0]

    if df.empty:
        raise Exception(f"No data available after filtering for timestamp >= {date_tm}")

    return df.head(1)[["timestamp", "Fwd_Ret"]]

# # Example usage
# if __name__ == "__main__":
#     # print(fwd_return('BTCUSD', datetime(2025, 7, 26, 19, 10), '5min'))
#     # print(fwd_return('BTCUSD', datetime(2025, 7, 26, 19, 20), '10min'))
#     print(fwd_return('BTCUSD', datetime(2025, 7, 26, 19, 50), '10min'))
