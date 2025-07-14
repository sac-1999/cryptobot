import requests
import time
from datetime import datetime, timedelta
import pandas as pd
import pytz
import json

def create_trade(entry, stoploss, tradetype, target, tradetime):
    data = None
    with open("trades.json", "r") as f:
        data = json.load(f)

    if data.get(tradetime) is not None:
        return 
    data[tradetime]={
        "entry": entry,
        "stoploss": stoploss,
        "target": target,
        "tradetype": tradetype,
        "traded" : False
    }
    with open("trades.json", "w") as f:
        json.dump(data, f, indent=2)


class Strategy:
    def __init__(self, symbol, rr = 1.5, timer = 1):
        self.symbol = symbol
        self.rr = rr
        self.timer = timer

    def _get_unix_timestamp(self, dt):
        return int(time.mktime(dt.timetuple()))

    def _load_historical_data(self, interval='1m', days=1):
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)

        start_unix = self._get_unix_timestamp(start_dt)
        end_unix = self._get_unix_timestamp(end_dt)

        url = 'https://api.india.delta.exchange/v2/history/candles'
        params = {
            'resolution': interval,
            'symbol': self.symbol,
            'start': start_unix,
            'end': end_unix
        }
        headers = {'Accept': 'application/json'}
        response = requests.get(url, params=params, headers=headers)

        if response.status_code == 200:
            data = response.json()
            df = pd.DataFrame(data['result'])
            df['time'] = pd.to_datetime(df['time'], unit='s',  utc=True).dt.tz_convert(pytz.timezone('Asia/Kolkata'))
            df = df[::-1]
            return df
        else:
            raise ConnectionError(f"Failed to fetch data: {response.status_code} - {response.text}")

    def run_strategy(self):
        try:
            df = self._load_historical_data()
            df['prevcandle'] = (df['close'] < df['high'].shift(1)) & (df['close'] > df['low'].shift(1))
            df['buy'] = (df['close'] > df['high'].shift(1)) & (df['prevcandle'].shift(1)) & (df['prevcandle'].shift(1))
            df['sell'] = (df['close'] < df['low'].shift(1)) & (df['prevcandle'].shift(1)) & (df['prevcandle'].shift(1))
            df['time'] = df['time'].dt.strftime('%Y-%m-%d %H:%M:%S')
            print(df.tail(4))
            df = df.tail(2).head(1)
            row = df.iloc[0]

            if row['buy']:
                entry = row['close'] - 10
                stoploss = row['low'] - 30
                target = entry + self.rr * (entry - stoploss)
                create_trade(entry, stoploss, 'buy', target,row['time'][:16])

            if row['sell']:
                entry = row['close'] + 10
                stoploss = row['high'] + 30
                target = entry - self.rr * (stoploss - entry)
                create_trade(entry, stoploss, 'sell', target,row['time'][:16])
                            
        except Exception as e:
            print(f"Error running strategy: {e}")

    def start(self):
        while(True):
            if datetime.today().minute % self.timer == 0:
                self.run_strategy()
                time.sleep(30)
            else:
                time.sleep(30)

    def stop(self):
        self.running = False

strategy = Strategy('BTCUSD')
strategy.start()