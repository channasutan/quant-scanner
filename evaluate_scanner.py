import os
import pandas as pd
import ccxt
from datetime import datetime, timezone, timedelta
from supabase import create_client

HORIZON_H = 4

def fetch_price(symbols, timestamp):
    """Fetch close prices at specific timestamp using ccxt."""
    ex = ccxt.toobit({"enableRateLimit": True})
    prices = {}
    
    # Convert timestamp to milliseconds
    since = int(timestamp.timestamp() * 1000)
    
    for symbol in symbols:
        try:
            # Fetch one candle at the timestamp
            bars = ex.fetch_ohlcv(symbol, "4h", since=since, limit=1)
            if bars and len(bars) > 0:
                # Use close price
                prices[symbol] = bars[0][4]  # close price
            else:
                prices[symbol] = None
        except Exception as e:
            print(f"Error fetching {symbol}: {e}")
            prices[symbol] = None
    
    return pd.Series(prices)

def main():
    sb = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=HORIZON_H)

    # 1. Get candidate runs (not too recent)
    runs = (
        sb.table("scanner_runs")
          .select("run_id, asof_ts")
          .lte("asof_ts", cutoff.isoformat())
          .execute()
          .data
    )

    print(f"Found {len(runs)} candidate runs for evaluation")

    for r in runs:
        run_id = r["run_id"]
        asof_ts = pd.Timestamp(r["asof_ts"], tz="UTC")
        
        print(f"Processing run_id: {run_id}, asof_ts: {asof_ts}")

        # Skip if already evaluated
        exists = (
            sb.table("scanner_eval")
              .select("run_id")
              .eq("run_id", run_id)
              .eq("horizon_hours", HORIZON_H)
              .execute()
              .data
        )
        if exists:
            print(f"  Already evaluated, skipping")
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
            print(f"  No results found, skipping")
            continue

        df = pd.DataFrame(rows)
        symbols = df["symbol"].tolist()
        
        print(f"  Evaluating {len(symbols)} symbols")

        # 3. Fetch prices (PAST → FUTURE)
        try:
            price_T = fetch_price(symbols, asof_ts)
            price_TH = fetch_price(symbols, asof_ts + timedelta(hours=HORIZON_H))
            
            # Calculate forward returns
            df["price_T"] = df["symbol"].map(price_T)
            df["price_TH"] = df["symbol"].map(price_TH)
            
            # Filter out symbols with missing prices
            valid_mask = df["price_T"].notna() & df["price_TH"].notna() & (df["price_T"] > 0)
            df_valid = df[valid_mask].copy()
            
            if df_valid.empty:
                print(f"  No valid prices found, skipping")
                continue
            
            df_valid["fwd_return"] = (df_valid["price_TH"] / df_valid["price_T"]) - 1
            
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
                print(f"  Inserted {len(out)} evaluation records")
            else:
                print(f"  No valid evaluation records to insert")
                
        except Exception as e:
            print(f"  Error processing run {run_id}: {e}")
            continue

    print("Evaluation completed")

if __name__ == "__main__":
    main()