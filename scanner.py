"""
Real-time scanner service that consumes inference API.
Pure orchestration - no ML training.
SCANNER VERSION: 2025-12-19-FIXED-MASK
"""
import ccxt
import pandas as pd
import numpy as np
import requests
from datetime import timedelta
from scipy.stats import spearmanr
from tqdm import tqdm
from uuid import uuid4
import os
from supabase import create_client, Client

from config import (
    TIMEFRAME_HOURS, TIMEFRAME, TOP_K, ADV_WINDOW,
    INFERENCE_URL, EXCHANGE, SYMBOL_SUFFIX, OHLCV_LIMIT,
    LARGE_TIER_THRESHOLD, MID_TIER_THRESHOLD, SEED
)
from features import build_features, get_inference_features, prepare_inference_payload


def get_supabase_client() -> Client:
    """Initialize Supabase client with service role key."""
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set")
    
    return create_client(url, key)


def last_closed_bar(hours: int) -> pd.Timestamp:
    """
    Calculate the last closed bar timestamp.
    Never use partial candles.
    """
    now = pd.Timestamp.utcnow()
    return now.floor(f"{hours}h") - pd.Timedelta(hours=hours)


def fetch_ohlcv_data(last_closed: pd.Timestamp) -> pd.DataFrame:
    """
    Fetch OHLCV data from exchange.
    
    Args:
        last_closed: Last closed bar timestamp
        
    Returns:
        DataFrame with columns [symbol, datetime, open, high, low, close, volume]
    """
    ex = ccxt.toobit({"enableRateLimit": True})
    mkts = ex.load_markets()
    
    symbols = [
        s for s, m in mkts.items()
        if s.endswith(SYMBOL_SUFFIX) and m.get("active", True)
    ]
    
    print(f"Fetching {len(symbols)} symbols...")
    
    # Calculate how far back to fetch (enough for features)
    # Need at least 24 bars for rv_24 + ADV_WINDOW
    bars_needed = max(24, ADV_WINDOW) + 10  # buffer
    since_ts = int((last_closed - timedelta(hours=TIMEFRAME_HOURS * bars_needed)).timestamp() * 1000)
    
    all_data = []
    
    for sym in tqdm(symbols, desc="Fetching OHLCV"):
        try:
            bars = ex.fetch_ohlcv(sym, TIMEFRAME, since=since_ts, limit=OHLCV_LIMIT)
            if not bars:
                continue
            
            df = pd.DataFrame(bars, columns=["ts", "open", "high", "low", "close", "volume"])
            df["datetime"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            df["symbol"] = sym
            df = df[["symbol", "datetime", "open", "high", "low", "close", "volume"]]
            
            # Only keep bars up to last_closed (no lookahead)
            df = df[df["datetime"] <= last_closed]
            
            if not df.empty:
                all_data.append(df)
                
        except Exception as e:
            print(f"Error fetching {sym}: {e}")
            continue
    
    if not all_data:
        raise ValueError("No data fetched")
    
    return pd.concat(all_data, ignore_index=True)


def call_inference_api(payload: dict) -> list:
    """
    Call the inference API and return predictions.
    
    Args:
        payload: Dict with "rows" key containing feature dicts
        
    Returns:
        List of dicts with "raw_alpha" predictions
    """
    if not payload["rows"]:
        return []
    
    try:
        resp = requests.post(INFERENCE_URL, json=payload, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"Inference API error: {e}")
        raise


def rank_cross_sectional(df: pd.DataFrame, last_closed: pd.Timestamp) -> pd.DataFrame:
    """
    Perform cross-sectional ranking with liquidity tiering.
    
    Args:
        df: DataFrame with raw_alpha predictions
        last_closed: Last closed bar timestamp
        
    Returns:
        DataFrame with scanner_score and tier columns
    """
    # Filter to last closed bar only
    latest = df[df["datetime"] == last_closed].copy()
    
    if latest.empty:
        raise ValueError(f"No data for last closed bar: {last_closed}")
    
    # Liquidity ranking by ADV
    latest["adv_filled"] = latest["adv"].fillna(0.0)
    latest["liq_rank"] = latest["adv_filled"].rank(ascending=False, method="first")
    n_sym = len(latest)
    
    # Assign tiers
    latest["tier"] = "SMALL"
    latest.loc[latest["liq_rank"] <= LARGE_TIER_THRESHOLD * n_sym, "tier"] = "LARGE"
    latest.loc[
        (latest["liq_rank"] > LARGE_TIER_THRESHOLD * n_sym) &
        (latest["liq_rank"] <= MID_TIER_THRESHOLD * n_sym),
        "tier"
    ] = "MID"
    
    # Robust z-score per tier (MAD-based) - FIXED VERSION 2025-12-24
    def cs_z_mad(group):
        med = group["raw_alpha"].median()
        mad = np.median(np.abs(group["raw_alpha"] - med))
        
        # CRITICAL FIX: Use minimum MAD threshold instead of zero-filling
        # This preserves cross-sectional ordering for low-variance tiers
        min_mad = 1e-6  # Minimum variance threshold
        if mad < min_mad or not np.isfinite(mad):
            mad = min_mad
        
        group["scanner_score"] = (group["raw_alpha"] - med) / (1.4826 * mad)
        return group
    
    ranked = latest.groupby("tier", group_keys=False).apply(cs_z_mad)
    
    return ranked


def generate_output(ranked: pd.DataFrame) -> dict:
    """
    Generate final scanner output with LONG/SHORT candidates per tier.
    
    Args:
        ranked: DataFrame with scanner_score and tier
        
    Returns:
        Dict with structure: {tier: {long: [...], short: [...]}}
    """
    output = {}
    
    for tier in ["LARGE", "MID", "SMALL"]:
        tier_df = ranked[ranked["tier"] == tier]
        
        if tier_df.empty:
            output[tier] = {"long": [], "short": []}
            continue
        
        # Top K LONG
        long_candidates = (
            tier_df.nlargest(TOP_K, "scanner_score")
            [["symbol", "scanner_score", "adv"]]
            .to_dict(orient="records")
        )
        
        # Top K SHORT
        short_candidates = (
            tier_df.nsmallest(TOP_K, "scanner_score")
            [["symbol", "scanner_score", "adv"]]
            .to_dict(orient="records")
        )
        
        output[tier] = {
            "long": long_candidates,
            "short": short_candidates
        }
    
    return output


def save_to_supabase(ranked: pd.DataFrame, last_closed: pd.Timestamp, execution_time_ms: int) -> str:
    """
    Save scanner results to Supabase with immutable snapshot semantics.
    
    Args:
        ranked: DataFrame with scanner results
        last_closed: Timestamp of the scan
        execution_time_ms: Execution time in milliseconds
        
    Returns:
        run_id: UUID of the created run
    """
    supabase = get_supabase_client()
    run_id = str(uuid4())
    
    # Prepare run metadata
    run_row = {
        "run_id": run_id,
        "asof_ts": last_closed.isoformat(),
        "timeframe": TIMEFRAME,
        "model_id": "lgb_v1",
        "universe_size": len(ranked),
        "execution_time_ms": execution_time_ms
    }
    
    # Prepare results rows with rankings
    rows = []
    for tier in ["LARGE", "MID", "SMALL"]:
        tier_df = ranked[ranked["tier"] == tier].copy()
        if tier_df.empty:
            continue
            
        # Add rankings
        tier_df = tier_df.sort_values("scanner_score", ascending=False).reset_index(drop=True)
        tier_df["rank_long"] = tier_df.index + 1
        
        tier_df = tier_df.sort_values("scanner_score", ascending=True).reset_index(drop=True)
        tier_df["rank_short"] = tier_df.index + 1
        
        # Convert to rows
        for _, row in tier_df.iterrows():
            rows.append({
                "run_id": run_id,
                "symbol": row["symbol"],
                "tier": row["tier"],
                "raw_alpha": float(row["raw_alpha"]) if pd.notna(row["raw_alpha"]) else None,
                "scanner_score": float(row["scanner_score"]) if pd.notna(row["scanner_score"]) else None,
                "rank_long": int(row["rank_long"]),
                "rank_short": int(row["rank_short"]),
                "adv": float(row["adv"]) if pd.notna(row["adv"]) else None
            })
    
    # Insert to Supabase (append-only)
    supabase.table("scanner_runs").insert(run_row).execute()
    supabase.table("scanner_results").insert(rows).execute()
    
    print(f"✅ Saved to Supabase: run_id={run_id}, {len(rows)} results")
    return run_id


def run_scanner() -> dict:
    """
    Main scanner execution.
    
    Returns:
        Dict with scanner results
    """
    import time
    start_time = time.time()
    
    # HARD STOP: Ensure we're running the fixed version
    assert "SCANNER VERSION: 2025-12-19-FIXED-MASK" in open(__file__).read(), "WRONG SCANNER VERSION!"
    
    print("=" * 60)
    print("REAL-TIME SCANNER SERVICE")
    print("=" * 60)
    
    # 1. Determine last closed bar
    last_closed = last_closed_bar(TIMEFRAME_HOURS)
    print(f"\nLast closed bar: {last_closed}")
    
    # 2. Fetch OHLCV data
    print("\nFetching OHLCV data...")
    raw_data = fetch_ohlcv_data(last_closed)
    print(f"Fetched {raw_data['symbol'].nunique()} symbols")
    
    # 3. Build features per symbol
    print("\nBuilding features...")
    all_features = []
    for sym in raw_data["symbol"].unique():
        sym_df = raw_data[raw_data["symbol"] == sym].copy()
        sym_df = build_features(sym_df)
        all_features.append(sym_df)
    
    feature_df = pd.concat(all_features, ignore_index=True)
    
    # 4. Prepare inference payload (only last closed bar with valid features)
    print("\nPreparing inference payload...")
    latest_features = feature_df[feature_df["datetime"] == last_closed].copy()
    
    # Check data lag
    if latest_features.empty:
        raise ValueError(f"No data available for last closed bar: {last_closed}")
    
    # Create mask for rows with complete features
    features = get_inference_features()
    valid_mask = latest_features[features].notna().all(axis=1)
    
    # Build payload from ONLY valid rows (CRITICAL FIX)
    latest_valid = latest_features.loc[valid_mask].copy()
    payload = prepare_inference_payload(latest_valid)
    
    print(f"Total rows: {len(latest_features)}, Valid features: {valid_mask.sum()}")
    print(f"Inference payload size: {len(payload['rows'])} rows")
    
    if not payload["rows"]:
        raise ValueError("No valid features for inference")
    
    # 5. Call inference API
    print("\nCalling inference API...")
    predictions = call_inference_api(payload)
    print(f"Received {len(predictions)} predictions")
    
    # 6. Merge predictions back using mask
    latest_features["raw_alpha"] = np.nan  # Initialize all as NaN
    
    # Bulletproof validation
    assert len(predictions) == len(latest_valid), f"Prediction count mismatch: {len(predictions)} != {len(latest_valid)}"
    assert len(predictions) == valid_mask.sum(), f"Prediction vs mask mismatch: {len(predictions)} != {valid_mask.sum()}"
    
    # Safe assignment using mask - FIXED VERSION 2025-12-19
    latest_features.loc[valid_mask, "raw_alpha"] = [p["raw_alpha"] for p in predictions]
    
    # 7. Cross-sectional ranking
    print("\nPerforming cross-sectional ranking...")
    ranked = rank_cross_sectional(latest_features, last_closed)
    
    # 8. Generate output
    print("\nGenerating output...")
    output = generate_output(ranked)
    
    # 9. Save to Supabase
    execution_time_ms = int((time.time() - start_time) * 1000)
    print(f"\nSaving to Supabase...")
    run_id = save_to_supabase(ranked, last_closed, execution_time_ms)
    
    # Print summary
    print("\n" + "=" * 60)
    print("SCANNER RESULTS")
    print("=" * 60)
    print(f"Timestamp: {last_closed}")
    print(f"Universe size: {len(ranked)}")
    print(f"Run ID: {run_id}")
    print(f"\nTier distribution:")
    print(ranked["tier"].value_counts())
    
    for tier in ["LARGE", "MID", "SMALL"]:
        print(f"\n{'=' * 60}")
        print(f"TIER: {tier}")
        print(f"{'=' * 60}")
        
        print(f"\nTOP {TOP_K} LONG:")
        for i, candidate in enumerate(output[tier]["long"], 1):
            print(f"{i}. {candidate['symbol']:20s} score={candidate['scanner_score']:7.3f}")
        
        print(f"\nTOP {TOP_K} SHORT:")
        for i, candidate in enumerate(output[tier]["short"], 1):
            print(f"{i}. {candidate['symbol']:20s} score={candidate['scanner_score']:7.3f}")
    
    return {
        "timestamp": str(last_closed),
        "universe_size": len(ranked),
        "run_id": run_id,
        "tiers": output
    }


if __name__ == "__main__":
    try:
        result = run_scanner()
        print("\n✅ Scanner completed successfully")
    except Exception as e:
        print(f"\n❌ Scanner failed: {e}")
        raise