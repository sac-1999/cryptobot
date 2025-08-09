import pandas as pd

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