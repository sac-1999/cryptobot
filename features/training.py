Here's your updated code with an added **scikit-learn Linear Regression** training module that trains over the generated feature set and label:

```python
import calendar_util
import dataset
import cacher
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split

# We are training at 12 am Asia time. 
# @cacher.load_or_save_pickle(subdir='model')
def training(symbol, train_date, port_freq, train_freq, look_back_days, num_features):
    # Always pass this date_tm through calendar get_train_freq function
    dates = calendar_util.get_training_dates(train_date, look_back_days, frequency=train_freq)
    data = pd.concat([dataset.train_data(symbol, dt, port_freq, num_features) for dt in dates])

    if data.empty:
        print("No data found for training.")
        return

    # Separate features and label
    X = data.drop(columns=['timestamp', 'label'], errors='ignore')
    y = data['label'] if 'label' in data.columns else None

    if y is None or y.empty:
        print("No label column found in the dataset.")
        return

    # Train a Linear Regression model
    model = LinearRegression()
    model.fit(X, y)

    return model  # Optionally return model if needed elsewhere