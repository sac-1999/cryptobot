import hourly_returns
import pandas as pd
from datetime import timedelta, datetime

def train_data(symbol, date):
    base_date = pd.to_datetime(date)
    all_rows = []

    # Loop through all 24 hours in the day
    for hour in range(24):
        dt = base_date + timedelta(hours=hour)
        try:
            row = hourly_returns.compute(symbol, dt)
            if row is not None and not row.empty:
                all_rows.append(row)
        except Exception as e:
            print(f"[WARN] Skipped hour {dt} due to error: {e}")

    return pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()


def predict_data(symbol, date_tm):
    return hourly_returns.compute(symbol, date_tm)

print(train_data('BTCUSD', datetime.date(2025, 7, 18)))
