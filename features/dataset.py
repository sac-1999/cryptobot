import freq_rets, fwd_return, comp_indicator
import pandas as pd
from datetime import timedelta, datetime
import pytz
import cacher

# Import liquidity modules
from liquidity_integration import LiquidityProcessor
import dataloader  

ASIA_TZ = 'Asia/Kolkata'

@cacher.load_or_save_pickle(subdir='train_data_with_liquidity',  verbose=1)
def train_data_with_liquidity(symbol, date, freq, num_features, 
                             include_liquidity=True, 
                             liquidity_feature_set='basic'):
    """
    Enhanced training data function that includes liquidity features
    
    Args:
        symbol: Trading symbol
        date: Base date for training data
        freq: Frequency for data collection
        num_features: Number of features for freq_rets
        include_liquidity: Whether to include liquidity features
        liquidity_feature_set: Type of liquidity features ('basic', 'volume', 'spread', 'microstructure', 'comprehensive')
    
    Returns:
        DataFrame with features, indicators, liquidity features, and labels
    """
    base_date = pd.to_datetime(date).tz_localize(ASIA_TZ)
    all_rows = []
    label_rows = []

    # Initialize liquidity processor if needed
    liquidity_processor = None
    if include_liquidity:
        liquidity_processor = LiquidityProcessor(dataloader)

    # Create a time range for the full day at the given frequency
    end_date = base_date + timedelta(days=1)
    time_range = pd.date_range(start=base_date, end=end_date, freq=freq, inclusive='left', tz=ASIA_TZ)

    for dt in time_range:
        try:
            # Get your existing features
            row = freq_rets.compute(symbol, dt, freq, num_features)
            row_1 = comp_indicator.compute(symbol, dt, freq)
            
            # Merge existing features
            if row is not None and row_1 is not None:
                row = row.merge(row_1, on=['timestamp'])
                
                # Add liquidity features if requested
                if include_liquidity and liquidity_processor is not None:
                    try:
                        liquidity_features = liquidity_processor.compute_liquidity_features(
                            symbol, dt, liquidity_feature_set
                        )
                        
                        if liquidity_features is not None:
                            # Merge liquidity features
                            row = row.merge(liquidity_features, on=['timestamp'], how='left')
                        else:
                            print(f"[WARN] No liquidity features for {dt}")
                            
                    except Exception as e:
                        print(f"[WARN] Liquidity feature error at {dt}: {e}")
                
                # Get labels
                label_row = fwd_return.fwd_return(symbol, dt, freq)
                
                if row is not None and not row.empty:
                    all_rows.append(row)
                    label_rows.append(label_row)
                    
        except Exception as e:
            print(f"[WARN] Skipped {dt} due to error: {e}")

    feature_df = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
    label_df = pd.concat(label_rows, ignore_index=True) if label_rows else pd.DataFrame()
    
    if feature_df.empty or label_df.empty:
        return pd.DataFrame()
        
    final_df = feature_df.merge(label_df, on='timestamp', how='inner')
    
    # Print summary
    if include_liquidity:
        # Count liquidity features
        liquidity_cols = []
        for col in final_df.columns:
            if any(keyword in col.lower() for keyword in 
                  ['volume', 'vwap', 'spread', 'pressure', 'liquidity', 'amihud', 'obv', 'cmf', 
                   'mfi', 'resilience', 'shock', 'lambda', 'flow']):
                liquidity_cols.append(col)
        
        print(f"[INFO] Training data created with {len(final_df.columns)} total features")
        print(f"[INFO] Liquidity features: {len(liquidity_cols)} ({liquidity_feature_set} set)")
        print(f"[INFO] Rows: {len(final_df)}")
    
    return final_df


# Backwards compatible function (your original function unchanged)
@cacher.load_or_save_pickle(subdir='train_data_with_ind',  verbose=1)
def train_data(symbol, date, freq, num_features):
    """
    Original train_data function - unchanged for backwards compatibility
    """
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


# Convenience functions for different liquidity feature sets
def train_data_basic_liquidity(symbol, date, freq, num_features):
    """Training data with basic liquidity features (recommended for most use cases)"""
    return train_data_with_liquidity(symbol, date, freq, num_features, 
                                   include_liquidity=True, 
                                   liquidity_feature_set='basic')

def train_data_comprehensive_liquidity(symbol, date, freq, num_features):
    """Training data with comprehensive liquidity features (all indicators)"""
    return train_data_with_liquidity(symbol, date, freq, num_features, 
                                   include_liquidity=True, 
                                   liquidity_feature_set='comprehensive')

def train_data_volume_liquidity(symbol, date, freq, num_features):
    """Training data with volume-based liquidity features only"""
    return train_data_with_liquidity(symbol, date, freq, num_features, 
                                   include_liquidity=True, 
                                   liquidity_feature_set='volume')

def train_data_spread_liquidity(symbol, date, freq, num_features):
    """Training data with spread-based liquidity features only"""
    return train_data_with_liquidity(symbol, date, freq, num_features, 
                                   include_liquidity=True, 
                                   liquidity_feature_set='spread')


# Utility function to compare feature sets
def compare_feature_sets(symbol, date, freq, num_features):
    """
    Compare different feature sets to see what's included
    
    Returns:
        Dictionary with feature counts for each set
    """
    results = {}
    
    # Original
    original_df = train_data(symbol, date, freq, num_features)
    results['original'] = len(original_df.columns) if not original_df.empty else 0
    
    # Different liquidity sets
    liquidity_sets = ['basic', 'volume', 'spread', 'microstructure', 'comprehensive']
    
    for liq_set in liquidity_sets:
        try:
            df = train_data_with_liquidity(symbol, date, freq, num_features, 
                                         include_liquidity=True, 
                                         liquidity_feature_set=liq_set)
            results[f'with_{liq_set}_liquidity'] = len(df.columns) if not df.empty else 0
        except Exception as e:
            results[f'with_{liq_set}_liquidity'] = f"Error: {e}"
    
    return results


# Batch processing function for multiple days
def create_multi_day_dataset(symbol, start_date, end_date, freq, num_features, 
                           include_liquidity=True, liquidity_feature_set='basic'):
    """
    Create training dataset for multiple days
    
    Args:
        symbol: Trading symbol
        start_date: Start date for dataset
        end_date: End date for dataset  
        freq: Frequency for data collection
        num_features: Number of features for freq_rets
        include_liquidity: Whether to include liquidity features
        liquidity_feature_set: Type of liquidity features
    
    Returns:
        Combined DataFrame for all days
    """
    start_date = pd.to_datetime(start_date)
    end_date = pd.to_datetime(end_date)
    
    all_data = []
    current_date = start_date
    
    while current_date <= end_date:
        try:
            if include_liquidity:
                day_data = train_data_with_liquidity(
                    symbol, current_date, freq, num_features, 
                    include_liquidity=True, 
                    liquidity_feature_set=liquidity_feature_set
                )
            else:
                day_data = train_data(symbol, current_date, freq, num_features)
            
            if not day_data.empty:
                all_data.append(day_data)
                print(f"[INFO] Processed {current_date.date()}: {len(day_data)} rows")
            else:
                print(f"[WARN] No data for {current_date.date()}")
                
        except Exception as e:
            print(f"[ERROR] Failed to process {current_date.date()}: {e}")
        
        current_date += timedelta(days=1)
    
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        print(f"[INFO] Multi-day dataset created: {len(combined_df)} total rows")
        return combined_df
    else:
        print("[ERROR] No data collected for any day")
        return pd.DataFrame()


# Example usage and testing
# if __name__ == "__main__":
#     # Example usage
#     symbol = 'BTCUSD'
#     date = datetime(2024, 2, 10, 15, 30)
#     freq = '30min'
#     num_features = 20
    
#     comprehensive_data = train_data_comprehensive_liquidity(symbol, date, freq, num_features)
#     print(comprehensive_data.columns)