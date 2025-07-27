import freq_rets, fwd_return, comp_indicator
import pandas as pd
from datetime import timedelta, datetime
import pytz
import cacher

ASIA_TZ = 'Asia/Kolkata'

@cacher.load_or_save_pickle(subdir='train_data_with_ind',  verbose=1)
def train_data(symbol, date, freq, num_features):
    base_date = pd.to_datetime(date).tz_localize(ASIA_TZ)
    all_rows = []
    label_rows = []

    # Create a time range for the full day at the given frequency
    end_date = base_date + timedelta(days=1)
    time_range = pd.date_range(start=base_date, end=end_date, freq=freq, inclusive='left', tz=ASIA_TZ)

    for dt in time_range:
        try:
            row = freq_rets.compute(symbol, dt, freq, num_features)
            row_1 = comp_indicator.compute(symbol, dt, freq)
            row = row.merge(row_1, on = ['timestamp'])
            label_row = fwd_return.fwd_return(symbol, dt, freq)
            if row is not None and not row.empty:
                all_rows.append(row)
                label_rows.append(label_row)
        except Exception as e:
            print(f"[WARN] Skipped {dt} due to error: {e}")

    feature_df = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
    label_df = pd.concat(label_rows, ignore_index=True) if label_rows else pd.DataFrame()
    return feature_df.merge(label_df, on='timestamp', how='inner')

# Example
# print(train_data('BTCUSD', datetime(2024, 2, 10, 15, 30), freq='30min', num_features=20))

