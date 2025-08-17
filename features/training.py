import numpy as np
import pandas as pd
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error

from ml_models import get_ml_model
import dataset, calendar_util


def training(
    symbol,
    train_date,
    port_freq,
    train_freq,
    look_back_days,
    num_features,
    feature_cols=None,
    label_col="label"
):
    """
    Train a given model on the dataset and return predictions + evaluation.

    Parameters
    ----------
    symbol : str
        Trading symbol
    train_date : datetime/date
        The target training date
    port_freq : str
        Portfolio frequency (unused here, but passed for scheduling)
    train_freq : str
        Model training frequency
    look_back_days : int
        Lookback period for dataset
    num_features : int
        Number of top features to use (if None, use all)
    model : tuple
        Model tuple as (name, type, estimator) from get_ml_model
    feature_cols : list[str], optional
        List of feature columns. If None, uses all except label.
    label_col : str
        Target label column
    class_thresh : float
        Classification threshold (unused for regression, kept for compatibility)
    """

    label_data = label_data(symbol: str, date: datetime, interval: str, fwd_pred_int: str, local_timezone: str = "Asia/Kolkata")
    model = ml_models.get_ml_model()

    # ---- Example dummy dataset ----
    # In practice, you’d call create_multi_day_dataset() or get_train_data()
    df = 

    if feature_cols is None:
        feature_cols = [c for c in df.columns if c != label_col]

    X = df[feature_cols]
    y = df[label_col]

    # Train-test split (simple holdout for demo)
    split_idx = int(len(df) * 0.8)
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]

    # Fit model
    estimator.fit(X_train, y_train)

    # Predict
    y_pred = estimator.predict(X_test)

    # Metrics
    metrics = {
        "r2": r2_score(y_test, y_pred),
        "rmse": mean_squared_error(y_test, y_pred, squared=False),
        "mae": mean_absolute_error(y_test, y_pred),
    }

    print(f"[INFO] {model_name} results: {metrics}")

    return {
        "model_name": model_name,
        "estimator": estimator,
        "metrics": metrics,
        "y_true": y_test,
        "y_pred": y_pred,
    }