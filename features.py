"""
Feature engineering functions that MUST match training.
"""
import pandas as pd
import numpy as np
from config import ADV_WINDOW


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build features that match training exactly.
    
    Args:
        df: OHLCV dataframe with columns [datetime, open, high, low, close, volume]
        
    Returns:
        DataFrame with added features: ret_1, ema12, rv_24, adv
    """
    df = df.copy().sort_values("datetime")
    
    # Returns
    df["ret_1"] = df["close"].pct_change()
    
    # EMA12 (exponential moving average)
    df["ema12"] = df["close"].ewm(span=12, adjust=False).mean()
    
    # RV24 (realized volatility - rolling std of returns)
    df["rv_24"] = df["ret_1"].rolling(24).std()
    
    # ADV (average dollar volume) for liquidity ranking
    df["dv"] = df["close"] * df["volume"]
    df["adv"] = df["dv"].rolling(ADV_WINDOW, min_periods=ADV_WINDOW).mean()
    
    return df


def get_inference_features() -> list:
    """
    Return the exact feature list expected by inference API.
    MUST match training feature order.
    """
    return ["ema12", "rv_24"]


def prepare_inference_payload(df: pd.DataFrame) -> dict:
    """
    Prepare payload for inference API call.
    
    Args:
        df: DataFrame with features
        
    Returns:
        Dict in format expected by inference API
    """
    features = get_inference_features()
    
    # Filter to rows with valid features
    valid_mask = df[features].notna().all(axis=1)
    valid_df = df[valid_mask].copy()
    
    if valid_df.empty:
        return {"rows": []}
    
    # Convert to list of dicts
    rows = []
    for _, row in valid_df.iterrows():
        row_dict = {}
        for feat in features:
            row_dict[feat] = float(row[feat])
        rows.append(row_dict)
    
    return {"rows": rows}