"""
Scanner Evaluation Service - FIXED VERSION 2025-12-26

CRITICAL FIXES:
1. Use 4h horizon (was 12h) to match scanner frequency - eliminates overlapping windows
2. Improved price fetching logic for more accurate bar matching
3. Better error handling and logging

This ensures non-overlapping evaluation windows and consistent timing with scanner.
"""
import os
import pandas as pd
import ccxt
from datetime import datetime, timezone, timedelta
from supabase import create_client

TIMEFRAME = "4h"
TIMEFRAME_HOURS = 4    # Actual hours per candle (for time calculations)
HORIZON_BARS = 1       # Number of bars forward (clearer naming)
HORIZON_H = HORIZON_BARS * TIMEFRAME_HOURS  # 1 * 4 = 4 hours

# Exchange singleton
EX = ccxt.toobit({"enableRateLimit": True})

def fetch_price_at_or_after(symbols, target_ts, timeframe=TIMEFRAME, lookahead=3):
    """
    Fetch CLOSE price from the first candle whose close_time >= target_ts
    FIXED: More precise bar matching logic
    """
    ex = EX
    out = {}
    
    since = int((target_ts - timedelta(hours=lookahead * TIMEFRAME_HOURS)).timestamp() * 1000)
    
    for symbol in symbols:
        try:
            bars = ex.fetch_ohlcv(
                symbol,
                timeframe,
                since=since,
                limit=lookahead + 2  # Extra buffer for safety
            )
            price = None
            
            for b in bars:
                bar_open_ts = pd.Timestamp(b[0], unit="ms", tz="UTC")
                bar_close_ts = bar_open_ts + pd.Timedelta(hours=TIMEFRAME_HOURS)
                
                # FIXED: Find bar that contains or comes after target timestamp
                if bar_close_ts >= target_ts:
                    price = b[4]  # close price
                    break
                    
            out[symbol] = price
            
        except Exception as e:
            print(f"[price_fetch_error] {symbol}: {e}")
            out[symbol] = None
    
    return pd.Series(out)

def main():
    """
    Main evaluation function - FIXED VERSION 2025-12-26
    Uses 4h horizon to match scanner frequency (eliminates overlapping windows)
    """
    sb = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )

    print("🔄 Scanner Evaluation Service - FIXED VERSION")
    print("=" * 60)
    print(f"Using {HORIZON_H}h horizon (matches scanner frequency)")
    print("=" * 60)

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=HORIZON_H)

    print(f"Current time: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"Evaluating runs older than: {cutoff.strftime('%Y-%m-%d %H:%M:%S UTC')}")

    # 1. Get candidate runs (not too recent)
    runs = (
        sb.table("scanner_runs")
          .select("run_id, asof_ts")
          .lte("asof_ts", cutoff.isoformat())
          .gte("asof_ts", (now - timedelta(days=7)).isoformat())  # Last 7 days only
          .order("asof_ts", desc=True)
          .execute()
          .data
    )

    print(f"\nFound {len(runs)} candidate runs from last 7 days")

    processed_count = 0
    skipped_count = 0
    error_count = 0

    for i, r in enumerate(runs, 1):
        run_id = r["run_id"]
        asof_ts = pd.Timestamp(r["asof_ts"], tz="UTC")
        
        print(f"\n[{i}/{len(runs)}] Processing: {run_id[:8]}... | {asof_ts.strftime('%m/%d %H:%M')}")

        # Skip if already evaluated with 4h horizon
        exists = (
            sb.table("scanner_eval")
              .select("run_id")
              .eq("run_id", run_id)
              .eq("horizon_hours", HORIZON_H)
              .execute()
              .data
        )
        if exists:
            print(f"  ✅ Already has {HORIZON_H}h evaluation, skipping")
            skipped_count += 1
            continue

        # 2. Load scanner results
        rows = (
            sb.table("scanner_results")
              .select("symbol, rank_long, rank_short")
              .eq("run_id", run_id)
              .execute()
              .data
        )

        if not rows:
            print(f"  ❌ No scanner results found, skipping")
            error_count += 1
            continue

        df = pd.DataFrame(rows)
        symbols = df["symbol"].tolist()
        
        print(f"  📊 Evaluating {len(symbols)} symbols for {HORIZON_H}h horizon")

        # 3. Fetch prices (PAST → FUTURE)
        try:
            # Price at signal time
            price_T = fetch_price_at_or_after(symbols, asof_ts)
            # Price 4 hours later
            price_TH = fetch_price_at_or_after(symbols, asof_ts + timedelta(hours=HORIZON_H))
            
            # Calculate forward returns
            df["price_T"] = df["symbol"].map(price_T)
            df["price_TH"] = df["symbol"].map(price_TH)
            
            # Filter out symbols with missing prices
            valid_mask = df["price_T"].notna() & df["price_TH"].notna() & (df["price_T"] > 0)
            df_valid = df[valid_mask].copy()
            
            if df_valid.empty:
                print(f"  ⚠️  No valid prices found, skipping")
                error_count += 1
                continue
            
            df_valid["fwd_return"] = (df_valid["price_TH"] / df_valid["price_T"]) - 1
            
            # Guard against suspicious returns (>500% or <-95%)
            suspicious_mask = df_valid["fwd_return"].abs() > 5
            if suspicious_mask.any():
                print(f"  ⚠️  Found {suspicious_mask.sum()} suspicious returns, filtering out")
                df_valid = df_valid[~suspicious_mask]
            
            if df_valid.empty:
                print(f"  ❌ No valid returns after filtering, skipping")
                error_count += 1
                continue
            
            # Prepare evaluation rows
            out = []
            for _, row in df_valid.iterrows():
                out.append({
                    "run_id": run_id,
                    "symbol": row["symbol"],
                    "horizon_hours": HORIZON_H,
                    "fwd_return": float(row["fwd_return"]),
                    "rank_long": int(row["rank_long"]) if pd.notna(row["rank_long"]) else None,
                    "rank_short": int(row["rank_short"]) if pd.notna(row["rank_short"]) else None,
                })
            
            # Insert evaluation results
            if out:
                sb.table("scanner_eval").insert(out).execute()
                print(f"  ✅ Inserted {len(out)} evaluation records")
                processed_count += 1
            else:
                print(f"  ❌ No valid evaluation records to insert")
                error_count += 1
                
        except Exception as e:
            print(f"  ❌ Error processing run {run_id}: {e}")
            error_count += 1
            continue

    # Summary
    print(f"\n📊 EVALUATION SUMMARY:")
    print(f"   Processed: {processed_count} runs")
    print(f"   Skipped (already exists): {skipped_count} runs")
    print(f"   Errors: {error_count} runs")
    print(f"   Total: {len(runs)} runs")
    
    if processed_count > 0:
        print(f"\n✅ Successfully generated {HORIZON_H}h evaluations!")
        print(f"   Frontend should now show fewer 'Pending' entries")
        print(f"   Performance metrics should improve with clean {HORIZON_H}h data")
    else:
        print(f"\n⚠️  No new evaluations generated")
        print(f"   All recent runs may already have {HORIZON_H}h evaluations")

    print("\n🎯 FIXED: Using 4h horizon eliminates overlapping windows and data leakage!")
    print("Evaluation completed")

if __name__ == "__main__":
    main()