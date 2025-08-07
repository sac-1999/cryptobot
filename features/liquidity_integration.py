import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import cacher
from liquidity_indicators import LiquidityIndicators

class LiquidityProcessor:
    """
    Integration class for liquidity indicators with your existing pipeline
    """
    
    def __init__(self, dataloader_module):
        """
        Initialize with your dataloader module
        Args:
            dataloader_module: Your existing dataloader module
        """
        self.dataloader = dataloader_module
    
    def get_liquidity_data(self, symbol: str, enddate: datetime, 
                          lookback_days: int = 5) -> pd.DataFrame:
        """
        Get sufficient 1-minute data for liquidity calculation
        
        Args:
            symbol: Trading symbol
            enddate: End date for data
            lookback_days: Days to look back for sufficient data
            
        Returns:
            DataFrame with 1-minute OHLCV data
        """
        df_list = []
        current_date = enddate
        total_bars = 0
        target_bars = lookback_days * 1440  # 1440 minutes per day
        
        while total_bars < target_bars and current_date.year >= 2020:
            try:
                daily_data = self.dataloader.get(symbol, current_date)
                if daily_data is not None and len(daily_data) > 0:
                    df_list.insert(0, daily_data)
                    total_bars += len(daily_data)
                    print(f"Loaded {len(daily_data)} bars for {current_date.date()}, total: {total_bars}")
            except Exception as e:
                print(f"Failed to load data for {current_date.date()}: {e}")
            
            current_date = current_date - timedelta(days=1)
        
        if not df_list:
            return None
            
        combined_df = pd.concat(df_list, ignore_index=True)
        combined_df = combined_df.sort_values('timestamp').reset_index(drop=True)
        
        return combined_df
    
    @cacher.load_or_save_pickle(subdir='liquidity_features')
    def compute_liquidity_features(self, symbol: str, date_tm: datetime, 
                                 feature_set: str = 'comprehensive') -> pd.DataFrame:
        """
        Compute liquidity features for a given symbol and date
        
        Args:
            symbol: Trading symbol
            date_tm: Target datetime
            feature_set: 'basic', 'volume', 'spread', 'microstructure', or 'comprehensive'
            
        Returns:
            DataFrame with liquidity features for the target timestamp
        """
        # Get 1-minute data
        fetch_date = pd.to_datetime(date_tm) - timedelta(seconds=30)
        df = self.get_liquidity_data(symbol, fetch_date, lookback_days=5)
        
        if df is None or len(df) < 100:  # Need minimum data
            print(f"Insufficient data for {symbol} at {date_tm}")
            return None
        
        # Compute features based on feature set
        if feature_set == 'basic':
            result_df = self._compute_basic_features(df)
        elif feature_set == 'volume':
            result_df = LiquidityIndicators.compute_all_volume_indicators(df)
        elif feature_set == 'spread':
            result_df = LiquidityIndicators.compute_all_spread_indicators(df)
        elif feature_set == 'microstructure':
            result_df = LiquidityIndicators.compute_all_microstructure_indicators(df)
        else:  # comprehensive
            result_df = LiquidityIndicators.compute_comprehensive_liquidity_features(df)
        
        # Remove rows with NaN values
        result_df = result_df.dropna()
        
        if len(result_df) == 0:
            return None
        
        # Get the last row (most recent)
        final_result = result_df.tail(1).copy()
        final_result.reset_index(drop=True, inplace=True)
        final_result['timestamp'] = pd.to_datetime(date_tm)
        
        # Remove OHLCV columns to match your existing pattern
        ohlcv_cols = ['open', 'high', 'low', 'close', 'volume']
        cols_to_remove = [col for col in ohlcv_cols if col in final_result.columns]
        final_result = final_result.drop(columns=cols_to_remove)
        
        return final_result
    
    def _compute_basic_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute a basic set of most important liquidity features"""
        result = df.copy()
        
        # Most important volume indicators
        result['volume_ratio_20'] = LiquidityIndicators.volume_ratio(df, 20)
        result['vwap_deviation'] = LiquidityIndicators.vwap_deviation(df)
        result['cmf'] = LiquidityIndicators.chaikin_money_flow(df, 20)
        result['obv'] = LiquidityIndicators.on_balance_volume(df)
        
        # Most important spread indicators
        result['hl_spread'] = LiquidityIndicators.high_low_spread(df)
        result['amihud_illiq'] = LiquidityIndicators.amihud_illiquidity(df, 20)
        result['effective_spread'] = LiquidityIndicators.effective_spread(df)
        
        # Most important microstructure indicators
        result['price_range_eff'] = LiquidityIndicators.price_range_efficiency(df)
        buying_pressure, selling_pressure = LiquidityIndicators.buying_selling_pressure(df)
        result['pressure_ratio'] = buying_pressure / (selling_pressure + 1e-8)
        
        # Advanced indicators
        result['vol_volatility_ratio'] = LiquidityIndicators.volume_volatility_ratio(df, 20)
        result['liquidity_shock'] = LiquidityIndicators.liquidity_shock_indicator(df)
        
        return result
    
    def get_feature_list(self, feature_set: str = 'comprehensive') -> list:
        """
        Get list of feature names for a given feature set
        
        Args:
            feature_set: Feature set name
            
        Returns:
            List of feature column names
        """
        # Create dummy data to get feature names
        dummy_data = pd.DataFrame({
            'timestamp': pd.date_range('2024-01-01', periods=100, freq='1min'),
            'open': np.random.randn(100).cumsum() + 100,
            'high': np.random.randn(100).cumsum() + 102,
            'low': np.random.randn(100).cumsum() + 98,
            'close': np.random.randn(100).cumsum() + 100,
            'volume': np.random.randint(1000, 10000, 100)
        })
        
        if feature_set == 'basic':
            result_df = self._compute_basic_features(dummy_data)
        elif feature_set == 'volume':
            result_df = LiquidityIndicators.compute_all_volume_indicators(dummy_data)
        elif feature_set == 'spread':
            result_df = LiquidityIndicators.compute_all_spread_indicators(dummy_data)
        elif feature_set == 'microstructure':
            result_df = LiquidityIndicators.compute_all_microstructure_indicators(dummy_data)
        else:  # comprehensive
            result_df = LiquidityIndicators.compute_comprehensive_liquidity_features(dummy_data)
        
        # Remove OHLCV and timestamp columns
        exclude_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        feature_cols = [col for col in result_df.columns if col not in exclude_cols]
        
        return feature_cols

# Example usage and integration with your existing code
def integrate_liquidity_features(original_compute_func, dataloader_module):
    """
    Decorator to add liquidity features to your existing compute function
    
    Args:
        original_compute_func: Your existing compute function
        dataloader_module: Your dataloader module
        
    Returns:
        Enhanced compute function with liquidity features
    """
    def enhanced_compute(symbol, date_tm, freq, include_liquidity=True, 
                        liquidity_feature_set='basic'):
        """
        Enhanced compute function that includes liquidity features
        
        Args:
            symbol: Trading symbol
            date_tm: Target datetime
            freq: Frequency for resampling
            include_liquidity: Whether to include liquidity features
            liquidity_feature_set: Type of liquidity features to include
        """
        # Get original features (your existing technical indicators)
        original_features = original_compute_func(symbol, date_tm, freq)
        
        if not include_liquidity or original_features is None:
            return original_features
        
        # Get liquidity features
        liquidity_processor = LiquidityProcessor(dataloader_module)
        liquidity_features = liquidity_processor.compute_liquidity_features(
            symbol, date_tm, liquidity_feature_set
        )
        
        if liquidity_features is None:
            print(f"Warning: Could not compute liquidity features for {symbol} at {date_tm}")
            return original_features
        
        # Merge features
        # Both should have the same timestamp
        liquidity_features = liquidity_features.drop(columns=['timestamp'])
        
        # Concatenate horizontally
        combined_features = pd.concat([original_features, liquidity_features], axis=1)
        
        return combined_features
    
    return enhanced_compute



# Modified version of your original compute function with liquidity
@cacher.load_or_save_pickle(subdir='enhanced_indicators')
def compute_with_liquidity(symbol, date_tm, freq, dataloader_module, 
                          include_liquidity=True, liquidity_feature_set='basic'):
    """
    Enhanced version of your compute function that includes liquidity features
    
    This function combines your existing technical indicators with liquidity indicators
    
    Args:
        symbol: Trading symbol
        date_tm: Target datetime  
        freq: Frequency for technical indicators
        dataloader_module: Your dataloader module
        include_liquidity: Whether to include liquidity features
        liquidity_feature_set: 'basic', 'volume', 'spread', 'microstructure', or 'comprehensive'
    
    Returns:
        DataFrame with combined technical and liquidity indicators
    """
    from indicators import Indicators  # Your existing indicators
    
    # Your existing multiday_data function logic
    def multiday_data(symbol, enddate, freq):
        totalcandles = 0
        dflist = []
        while(totalcandles < 252):
            df = dataloader_module.compute(symbol, enddate, freq)
            if df is not None:
                dflist.insert(0,df)
                totalcandles = totalcandles + len(df)
            enddate = enddate - timedelta(1)
            if enddate.year == 2020:
                return None
        return pd.concat(dflist)
    
    # Get data for technical indicators
    fetch_date_tm = pd.to_datetime(date_tm) - timedelta(seconds=30)
    df = multiday_data(symbol, fetch_date_tm, freq)
    if df is None:
        return None
        
    df.reset_index(drop=True, inplace=True)
    df = df.sort_values("timestamp").reset_index(drop=True)
    
    # Your existing technical indicators
    ema_list = [7,14,21,44,50,63,100, 132, 200, 256]
    for ema in ema_list:
        df = Indicators.ema(df, ema)
    for length, multi in [(10,3), (15,1), (10, 2), (8,2), (8,3)]:
        df = Indicators.supertrend(df, length, multi)
    df = Indicators.vwap(df)
    df = Indicators.rsi(df, 14)
    df = Indicators.macd(df)
    df = df.dropna()
    
    # Normalize technical indicators (your existing logic)
    maincolumns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']	
    for col in df.columns:
        if col in maincolumns or 'rsi' in col:
            continue
        df[col] = (df[col] - df['close'])/ df['close']
    
    # Get final technical indicators row
    tech_indicators = df.tail(1).copy()
    tech_indicators.reset_index(drop=True, inplace=True)
    tech_indicators['timestamp'] = pd.to_datetime(date_tm)
    tech_indicators = tech_indicators.drop(columns=['open', 'high', 'low', 'close', 'volume'])
    
    # Add liquidity features if requested
    if include_liquidity:
        liquidity_processor = LiquidityProcessor(dataloader_module)
        liquidity_features = liquidity_processor.compute_liquidity_features(
            symbol, date_tm, liquidity_feature_set
        )
        
        if liquidity_features is not None:
            # Remove timestamp from liquidity features to avoid duplication
            liquidity_features = liquidity_features.drop(columns=['timestamp'])
            
            # Normalize liquidity features (except ratios and percentages)
            # This follows your pattern of normalizing indicators
            for col in liquidity_features.columns:
                if any(keyword in col.lower() for keyword in ['ratio', 'percentile', 'eff', 'pressure', 'shock']):
                    continue  # Don't normalize ratios, percentiles, efficiency measures, etc.
                if 'deviation' in col.lower() or 'spread' in col.lower():
                    continue  # These are already normalized
                # You might want to add more conditions based on the specific indicators
            
            # Combine technical and liquidity features
            combined_features = pd.concat([tech_indicators, liquidity_features], axis=1)
            return combined_features
        else:
            print(f"Warning: Could not compute liquidity features for {symbol} at {date_tm}")
    
    return tech_indicators
