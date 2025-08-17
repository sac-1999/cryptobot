from sklearn.linear_model import Ridge
from sklearn.ensemble import BaggingRegressor
from lightgbm import LGBMRegressor

def get_ml_model(dataset_df=None):
    """
    Returns a list of models with explicit names and types.
    Optionally accepts dataset_df to size min_child_samples dynamically.
    """

    # Base RidgeCV model
    ridge_base = Ridge()

    # Bagging wrapper (sklearn >= 1.2 uses `estimator` instead of `base_estimator`)
    ridge_bagging = BaggingRegressor(
        estimator=ridge_base,
        n_estimators=100,
        max_samples=0.7,
        max_features=0.5,
        bootstrap=True,
        bootstrap_features=False,
        n_jobs=-1,
        random_state=42
    )

    # Default fallback if dataset_df is not passed
    n_samples = len(dataset_df) if dataset_df is not None else 1000
    min_child_samples = max(10, int(n_samples * 0.01))

    model_list = [
        ("Ridge_Bagging", "reg", ridge_bagging),
        (
            "LGBM_Regression", "reg",
            LGBMRegressor(
                n_estimators=128,
                learning_rate=0.01,
                max_depth=-1,
                random_state=42,
                min_child_samples=min_child_samples,
                min_child_weight=0.01,
                num_leaves=8,
                n_jobs=8
            )
        ),
    ]

    return model_list