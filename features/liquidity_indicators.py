import pandas as pd
import numpy as np
from typing import Union, List, Optional

class LiquidityIndicators:
    """
    Comprehensive liquidity indicators for 1-minute OHLCV data
    """
    
    @staticmethod
    def validate_df(df: pd.DataFrame) -> None:
        """Validate that DataFrame has required columns"""
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns: {missing_cols}")
    
    # =============================================================================
    # VOLUME-BASED LIQUIDITY INDICATORS
    # =============================================================================
    
    @staticmethod
    def volume_rate(df: pd.DataFrame, window: int = 1) -> pd.Series:
        """Volume per unit time (volume/minute for 1min bars)"""
        LiquidityIndicators.validate_df(df)
        return df['volume'] / window
    
    @staticmethod
    def average_volume(df: pd.DataFrame, window: int = 20) -> pd.Series:
        """Rolling average volume"""
        LiquidityIndicators.validate_df(df)
        return df['volume'].rolling(window=window, min_periods=1).mean()
    
    @staticmethod
    def volume_ratio(df: pd.DataFrame, window: int = 20) -> pd.Series:
        """Current volume / Average volume"""
        LiquidityIndicators.validate_df(df)
        avg_vol = LiquidityIndicators.average_volume(df, window)
        return df['volume'] / avg_vol
    
    @staticmethod
    def volume_percentile_rank(df: pd.DataFrame, window: int = 100) -> pd.Series:
        """Volume percentile rank over rolling window"""
        LiquidityIndicators.validate_df(df)
        return df['volume'].rolling(window=window, min_periods=1).rank(pct=True)
    
    @staticmethod
    def vwap_deviation(df: pd.DataFrame) -> pd.Series:
        """VWAP deviation: (price - VWAP) / VWAP"""
        LiquidityIndicators.validate_df(df)
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        vwap = (typical_price * df['volume']).cumsum() / df['volume'].cumsum()
        return (df['close'] - vwap) / vwap
    
    @staticmethod
    def volume_weighted_std(df: pd.DataFrame, window: int = 20) -> pd.Series:
        """Volume weighted standard deviation"""
        LiquidityIndicators.validate_df(df)
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        
        def vw_std(prices, volumes):
            if len(prices) == 0 or volumes.sum() == 0:
                return np.nan
            weights = volumes / volumes.sum()
            weighted_mean = (prices * weights).sum()
            weighted_var = ((prices - weighted_mean) ** 2 * weights).sum()
            return np.sqrt(weighted_var)
        
        return typical_price.rolling(window=window).apply(
            lambda x: vw_std(x, df['volume'].iloc[x.index])
        )
    
    @staticmethod
    def twap(df: pd.DataFrame, window: int = 20) -> pd.Series:
        """Time Weighted Average Price"""
        LiquidityIndicators.validate_df(df)
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        return typical_price.rolling(window=window, min_periods=1).mean()
    
    @staticmethod
    def twap_vwap_spread(df: pd.DataFrame, window: int = 20) -> pd.Series:
        """TWAP vs VWAP spread"""
        LiquidityIndicators.validate_df(df)
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        twap = LiquidityIndicators.twap(df, window)
        
        # Rolling VWAP
        def rolling_vwap(prices, volumes, window):
            vwap_series = []
            for i in range(len(prices)):
                start_idx = max(0, i - window + 1)
                price_slice = prices.iloc[start_idx:i+1]
                volume_slice = volumes.iloc[start_idx:i+1]
                if volume_slice.sum() > 0:
                    vwap_val = (price_slice * volume_slice).sum() / volume_slice.sum()
                else:
                    vwap_val = price_slice.mean()
                vwap_series.append(vwap_val)
            return pd.Series(vwap_series, index=prices.index)
        
        vwap = rolling_vwap(typical_price, df['volume'], window)
        return (twap - vwap) / vwap
    
    @staticmethod
    def accumulation_distribution(df: pd.DataFrame) -> pd.Series:
        """Accumulation/Distribution Line"""
        LiquidityIndicators.validate_df(df)
        clv = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
        clv = clv.fillna(0)  # Handle division by zero when high == low
        ad = (clv * df['volume']).cumsum()
        return ad
    
    @staticmethod
    def chaikin_money_flow(df: pd.DataFrame, window: int = 20) -> pd.Series:
        """Chaikin Money Flow"""
        LiquidityIndicators.validate_df(df)
        clv = ((df['close'] - df['low']) - (df['high'] - df['close'])) / (df['high'] - df['low'])
        clv = clv.fillna(0)
        money_flow_volume = clv * df['volume']
        cmf = money_flow_volume.rolling(window=window).sum() / df['volume'].rolling(window=window).sum()
        return cmf
    
    @staticmethod
    def money_flow_index(df: pd.DataFrame, window: int = 14) -> pd.Series:
        """Money Flow Index"""
        LiquidityIndicators.validate_df(df)
        typical_price = (df['high'] + df['low'] + df['close']) / 3
        money_flow = typical_price * df['volume']
        
        positive_mf = np.where(typical_price > typical_price.shift(1), money_flow, 0)
        negative_mf = np.where(typical_price < typical_price.shift(1), money_flow, 0)
        
        positive_mf_sum = pd.Series(positive_mf).rolling(window=window).sum()
        negative_mf_sum = pd.Series(negative_mf).rolling(window=window).sum()
        
        mfi = 100 - (100 / (1 + positive_mf_sum / negative_mf_sum))
        return mfi
    
    @staticmethod
    def on_balance_volume(df: pd.DataFrame) -> pd.Series:
        """On-Balance Volume"""
        LiquidityIndicators.validate_df(df)
        price_change = df['close'].diff()
        obv_change = np.where(price_change > 0, df['volume'],
                             np.where(price_change < 0, -df['volume'], 0))
        return pd.Series(obv_change).cumsum()
    
    # =============================================================================
    # SPREAD & PRICE IMPACT INDICATORS
    # =============================================================================
    
    @staticmethod
    def high_low_spread(df: pd.DataFrame, normalize: bool = True) -> pd.Series:
        """High-Low Spread"""
        LiquidityIndicators.validate_df(df)
        spread = df['high'] - df['low']
        if normalize:
            return spread / df['close']
        return spread
    
    @staticmethod
    def high_low_spread_ratio(df: pd.DataFrame) -> pd.Series:
        """High-Low Spread Ratio"""
        LiquidityIndicators.validate_df(df)
        return (df['high'] - df['low']) / (df['high'] + df['low'])
    
    @staticmethod
    def volume_adjusted_price_range(df: pd.DataFrame) -> pd.Series:
        """Price range normalized by volume"""
        LiquidityIndicators.validate_df(df)
        price_range = df['high'] - df['low']
        return price_range / np.sqrt(df['volume'] + 1)  # +1 to avoid division by zero
    
    @staticmethod
    def volume_adjusted_price_change(df: pd.DataFrame) -> pd.Series:
        """Volume-adjusted price change"""
        LiquidityIndicators.validate_df(df)
        price_change = df['close'] - df['open']
        return price_change / np.sqrt(df['volume'] + 1)
    
    @staticmethod
    def amihud_illiquidity(df: pd.DataFrame, window: int = 20) -> pd.Series:
        """Amihud Illiquidity Ratio"""
        LiquidityIndicators.validate_df(df)
        returns = df['close'].pct_change().abs()
        illiquidity = returns / (df['volume'] + 1)  # +1 to avoid division by zero
        return illiquidity.rolling(window=window, min_periods=1).mean()
    
    # =============================================================================
    # MICROSTRUCTURE INDICATORS
    # =============================================================================
    
    @staticmethod
    def volume_per_trade(df: pd.DataFrame, trade_count_proxy: int = 1) -> pd.Series:
        """Volume per trade (assuming trade count proxy)"""
        LiquidityIndicators.validate_df(df)
        return df['volume'] / trade_count_proxy
    
    @staticmethod
    def zero_volume_count(df: pd.DataFrame, window: int = 60) -> pd.Series:
        """Count of zero volume periods in rolling window"""
        LiquidityIndicators.validate_df(df)
        zero_volume = (df['volume'] == 0).astype(int)
        return zero_volume.rolling(window=window, min_periods=1).sum()
    
    @staticmethod
    def price_range_efficiency(df: pd.DataFrame) -> pd.Series:
        """Price Range Efficiency: (Close - Open) / (High - Low)"""
        LiquidityIndicators.validate_df(df)
        price_move = df['close'] - df['open']
        price_range = df['high'] - df['low']
        efficiency = np.where(price_range != 0, price_move / price_range, 0)
        return pd.Series(efficiency, index=df.index)
    
    @staticmethod
    def volume_weighted_price_range(df: pd.DataFrame) -> pd.Series:
        """Volume weighted price range"""
        LiquidityIndicators.validate_df(df)
        price_range = df['high'] - df['low']
        return price_range * df['volume']
    
    # =============================================================================
    # TIME-BASED LIQUIDITY MEASURES
    # =============================================================================
    
    @staticmethod
    def volume_concentration_ratio(df: pd.DataFrame, session_window: int = 390) -> pd.Series:
        """Volume concentration (current volume / session volume)"""
        LiquidityIndicators.validate_df(df)
        session_volume = df['volume'].rolling(window=session_window, min_periods=1).sum()
        return df['volume'] / session_volume
    
    @staticmethod
    def volume_clustering_index(df: pd.DataFrame, window: int = 20) -> pd.Series:
        """Volume clustering index"""
        LiquidityIndicators.validate_df(df)
        vol_mean = df['volume'].rolling(window=window, min_periods=1).mean()
        vol_std = df['volume'].rolling(window=window, min_periods=1).std()
        return (df['volume'] - vol_mean) / (vol_std + 1e-8)
    
    @staticmethod
    def volume_volatility_ratio(df: pd.DataFrame, window: int = 20) -> pd.Series:
        """Volume to volatility ratio"""
        LiquidityIndicators.validate_df(df)
        returns = df['close'].pct_change()
        volatility = returns.rolling(window=window, min_periods=1).std()
        avg_volume = df['volume'].rolling(window=window, min_periods=1).mean()
        return avg_volume / (volatility + 1e-8)
    
    @staticmethod
    def volatility_per_unit_volume(df: pd.DataFrame, window: int = 20) -> pd.Series:
        """Volatility per unit volume"""
        LiquidityIndicators.validate_df(df)
        returns = df['close'].pct_change()
        volatility = returns.rolling(window=window, min_periods=1).std()
        avg_volume = df['volume'].rolling(window=window, min_periods=1).mean()
        return volatility / (avg_volume + 1e-8)
    
    # =============================================================================
    # MARKET DEPTH PROXIES
    # =============================================================================
    
    @staticmethod
    def volume_to_range_ratio(df: pd.DataFrame) -> pd.Series:
        """Volume to Range Ratio"""
        LiquidityIndicators.validate_df(df)
        price_range = df['high'] - df['low']
        return df['volume'] / (price_range + 1e-8)
    
    @staticmethod
    def effective_spread(df: pd.DataFrame) -> pd.Series:
        """Effective Spread approximation"""
        LiquidityIndicators.validate_df(df)
        midpoint = (df['high'] + df['low']) / 2
        effective_spread = 2 * np.abs(df['close'] - midpoint) / (df['high'] + df['low'])
        return effective_spread
    
    @staticmethod
    def resilience_indicator(df: pd.DataFrame, volume_threshold_percentile: float = 0.9, 
                           window: int = 5) -> pd.Series:
        """Price resilience after volume spikes"""
        LiquidityIndicators.validate_df(df)
        volume_threshold = df['volume'].quantile(volume_threshold_percentile)
        volume_spikes = df['volume'] > volume_threshold
        
        price_recovery = []
        for i in range(len(df)):
            if i < window or not volume_spikes.iloc[i]:
                price_recovery.append(0)
            else:
                pre_spike_price = df['close'].iloc[i-1]
                post_spike_prices = df['close'].iloc[i:i+window]
                if len(post_spike_prices) > 0:
                    recovery = np.abs(post_spike_prices.iloc[-1] - pre_spike_price) / pre_spike_price
                    price_recovery.append(recovery)
                else:
                    price_recovery.append(0)
        
        return pd.Series(price_recovery, index=df.index)
    
    # =============================================================================
    # ADVANCED LIQUIDITY METRICS
    # =============================================================================
    
    @staticmethod
    def buying_selling_pressure(df: pd.DataFrame) -> tuple:
        """Buying and selling pressure based on close position in OHLC range"""
        LiquidityIndicators.validate_df(df)
        price_range = df['high'] - df['low']
        close_position = (df['close'] - df['low']) / (price_range + 1e-8)
        
        buying_pressure = close_position * df['volume']
        selling_pressure = (1 - close_position) * df['volume']
        
        return buying_pressure, selling_pressure
    
    @staticmethod
    def volume_price_momentum(df: pd.DataFrame, window: int = 20) -> pd.Series:
        """Volume-weighted price momentum"""
        LiquidityIndicators.validate_df(df)
        price_change = df['close'].pct_change()
        volume_weighted_momentum = (price_change * df['volume']).rolling(window=window).sum()
        total_volume = df['volume'].rolling(window=window).sum()
        return volume_weighted_momentum / (total_volume + 1e-8)
    
    @staticmethod
    def liquidity_shock_indicator(df: pd.DataFrame, volume_threshold_percentile: float = 0.95,
                                price_threshold_percentile: float = 0.1) -> pd.Series:
        """Detect liquidity shocks (high volume, low price movement)"""
        LiquidityIndicators.validate_df(df)
        volume_threshold = df['volume'].quantile(volume_threshold_percentile)
        price_changes = np.abs(df['close'].pct_change())
        price_threshold = price_changes.quantile(price_threshold_percentile)
        
        volume_spike = df['volume'] > volume_threshold
        small_price_move = price_changes < price_threshold
        
        liquidity_shock = volume_spike & small_price_move
        return liquidity_shock.astype(int)
    
    @staticmethod
    def kyles_lambda(df: pd.DataFrame, window: int = 20) -> pd.Series:
        """Kyle's Lambda approximation (price impact coefficient)"""
        LiquidityIndicators.validate_df(df)
        price_changes = np.abs(df['close'].pct_change())
        
        def rolling_regression_slope(y, x, window):
            slopes = []
            for i in range(len(y)):
                start_idx = max(0, i - window + 1)
                y_slice = y.iloc[start_idx:i+1]
                x_slice = x.iloc[start_idx:i+1]
                
                if len(y_slice) > 1 and x_slice.std() > 1e-8:
                    correlation = y_slice.corr(x_slice)
                    slope = correlation * (y_slice.std() / x_slice.std())
                    slopes.append(slope)
                else:
                    slopes.append(0)
            return pd.Series(slopes, index=y.index)
        
        lambda_coeff = rolling_regression_slope(price_changes, df['volume'], window)
        return lambda_coeff
    
    # =============================================================================
    # CONVENIENCE METHODS
    # =============================================================================
    
    @classmethod
    def compute_all_volume_indicators(cls, df: pd.DataFrame, windows: dict = None) -> pd.DataFrame:
        """Compute all volume-based indicators"""
        if windows is None:
            windows = {'short': 10, 'medium': 20, 'long': 60}
        
        result = df.copy()
        
        # Volume indicators
        for name, window in windows.items():
            result[f'avg_volume_{name}'] = cls.average_volume(df, window)
            result[f'volume_ratio_{name}'] = cls.volume_ratio(df, window)
            result[f'volume_percentile_{name}'] = cls.volume_percentile_rank(df, window*2)
        
        result['vwap_deviation'] = cls.vwap_deviation(df)
        result['ad_line'] = cls.accumulation_distribution(df)
        result['cmf'] = cls.chaikin_money_flow(df)
        result['mfi'] = cls.money_flow_index(df)
        result['obv'] = cls.on_balance_volume(df)
        
        return result
    
    @classmethod
    def compute_all_spread_indicators(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Compute all spread and price impact indicators"""
        result = df.copy()
        
        result['hl_spread'] = cls.high_low_spread(df)
        result['hl_spread_ratio'] = cls.high_low_spread_ratio(df)
        result['vol_adj_range'] = cls.volume_adjusted_price_range(df)
        result['vol_adj_change'] = cls.volume_adjusted_price_change(df)
        result['amihud_illiq'] = cls.amihud_illiquidity(df)
        result['effective_spread'] = cls.effective_spread(df)
        
        return result
    
    @classmethod
    def compute_all_microstructure_indicators(cls, df: pd.DataFrame) -> pd.DataFrame:
        """Compute all microstructure indicators"""
        result = df.copy()
        
        result['price_range_eff'] = cls.price_range_efficiency(df)
        result['vol_weighted_range'] = cls.volume_weighted_price_range(df)
        result['zero_vol_count'] = cls.zero_volume_count(df)
        result['resilience'] = cls.resilience_indicator(df)
        
        buying_pressure, selling_pressure = cls.buying_selling_pressure(df)
        result['buying_pressure'] = buying_pressure
        result['selling_pressure'] = selling_pressure
        result['pressure_ratio'] = buying_pressure / (selling_pressure + 1e-8)
        
        return result
    
    @classmethod
    def compute_comprehensive_liquidity_features(cls, df: pd.DataFrame, 
                                               windows: dict = None) -> pd.DataFrame:
        """Compute comprehensive set of liquidity indicators"""
        if windows is None:
            windows = {'short': 5, 'medium': 20, 'long': 60}
        
        # Start with original data
        result = df.copy()
        
        # Volume-based indicators
        result = cls.compute_all_volume_indicators(result, windows)
        
        # Spread and price impact
        result = cls.compute_all_spread_indicators(result)
        
        # Microstructure
        result = cls.compute_all_microstructure_indicators(result)
        
        # Advanced indicators
        result['vol_volatility_ratio'] = cls.volume_volatility_ratio(df)
        result['volatility_per_volume'] = cls.volatility_per_unit_volume(df)
        result['volume_clustering'] = cls.volume_clustering_index(df)
        result['vol_price_momentum'] = cls.volume_price_momentum(df)
        result['liquidity_shock'] = cls.liquidity_shock_indicator(df)
        result['kyles_lambda'] = cls.kyles_lambda(df)
        
        return result