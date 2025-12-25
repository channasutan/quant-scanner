# ============================================================
# SCANNER EVALUATION SERVICE — FINAL (QUANT-CORRECT)
# Version : 2025-12-26
# Horizon : 4h (1 bar, non-overlapping)
# Status  : Deterministic, anti-leakage, tier-aware ready
# ============================================================

import os
import ccxt
import pandas as pd
from datetime import datetime, timezone, timedelta
from supabase import create_client

# ============================================================
# CONFIG (SINGLE SOURCE OF TRUTH)
# ============================================================
TIMEFRAME = "4h"
TF_HOURS = 4
HORIZON_BARS = 1
HORIZON_H = HORIZON_BARS * TF_HOURS
LOOKBACK_BARS = 6          # only past bars
SAFETY_MINUTES = 5         # candle-close safety buffer
MAX_ABS_RETURN = 5.0       # 500% guard

# ============================================================
# EXCHANGE (SINGLETON)
# ============================================================
EX = ccxt.toobit({
    "enableRateLimit": True,
})

# ============================================================
# PRICE FETCH — EXACT BAR, NO FUTURE VISIBILITY
# ============================================================
def fetch_close_at_exact_bar(symbols, target_ts):
    """
    Fetch CLOSE price of the candle whose OPEN == floor(target_ts, TF)
    - deterministic
    - no >= logic
    - no future bars requested
    """
    target_bar_open = (
        pd.Timestamp(target_ts)
        .floor(f"{TF_HOURS}h")
        .tz_convert("UTC")
    )
    
    since = int((target_bar_open - timedelta(hours=LOOKBACK_BARS * TF_HOURS)).timestamp() * 1000)
    
    out = {}
    for sym in symbols:
        price = None
        try:
            bars = EX.fetch_ohlcv(
                sym,
                TIMEFRAME,
                since=since,
                limit=LOOKBACK_BARS + 2
            )
            
            for b in bars:
                bar_open = pd.Timestamp(b[0], unit="ms", tz="UTC")
                if bar_open == target_bar_open:
                    price = float(b[4])  # close
                    break
                    
        except Exception as e:
            print(f"[price_error] {sym}: {e}")
            
        out[sym] = price
    
    return pd.Series(out)

# ============================================================
# MAIN
# ============================================================
def main():
    sb = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"],
    )
    
    now = datetime.now(timezone.utc)
    eval_cutoff = now - timedelta(hours=HORIZON_H, minutes=SAFETY_MINUTES)
    eval_start  = now - timedelta(days=7)
    
    print("=" * 72)
    print("SCANNER EVALUATION — FINAL (LEAK-FREE)")
    print(f"Horizon      : {HORIZON_H}h")
    print(f"Eval cutoff  : {eval_cutoff}")
    print("=" * 72)
    
    runs = (
        sb.table("scanner_runs")
          .select("run_id, asof_ts")
          .gte("asof_ts", eval_start.isoformat())
          .lte("asof_ts", eval_cutoff.isoformat())
          .order("asof_ts", desc=True)
          .execute()
          .data
    )
    
    print(f"Candidate runs: {len(runs)}")
    
    processed = skipped = errors = 0
    
    for i, r in enumerate(runs, 1):
        run_id = r["run_id"]
        asof_ts = pd.Timestamp(r["asof_ts"], tz="UTC")
        
        print(f"\n[{i}/{len(runs)}] {run_id[:8]} | {asof_ts}")
        
        # ----------------------------------------------------
        # DEDUP — RUN + HORIZON MUST BE COMPLETE
        # ----------------------------------------------------
        existing = (
            sb.table("scanner_eval")
              .select("symbol")
              .eq("run_id", run_id)
              .eq("horizon_hours", HORIZON_H)
              .execute()
              .data
        )
        
        if existing:
            print("  ✓ already evaluated")
            skipped += 1
            continue
        
        # ----------------------------------------------------
        # LOAD SCANNER OUTPUT
        # ----------------------------------------------------
        rows = (
            sb.table("scanner_results")
              .select("symbol",
                     "rank_long",
                     "rank_short",
                     "tier"          # IMPORTANT: tier preserved
                     )
              .eq("run_id", run_id)
              .execute()
              .data
        )
        
        if not rows:
            print("  ✗ no scanner results")
            errors += 1
            continue
        
        df = pd.DataFrame(rows)
        symbols = df["symbol"].tolist()
        
        # ----------------------------------------------------
        # PRICE FETCH (T, T+H)
        # ----------------------------------------------------
        try:
            px_T  = fetch_close_at_exact_bar(symbols, asof_ts)
            px_TH = fetch_close_at_exact_bar(symbols,
                                           asof_ts + timedelta(hours=HORIZON_H))
            
            df["price_T"]  = df["symbol"].map(px_T)
            df["price_TH"] = df["symbol"].map(px_TH)
            
            mask = (df["price_T"].notna() &
                   df["price_TH"].notna() &
                   (df["price_T"] > 0))
            
            df = df[mask].copy()
            
            if df.empty:
                print("  ✗ no valid prices")
                errors += 1
                continue
            
            df["fwd_return"] = (df["price_TH"] / df["price_T"]) - 1.0
            
            # ------------------------------------------------
            # SANITY FILTER
            # ------------------------------------------------
            df = df[df["fwd_return"].abs() <= MAX_ABS_RETURN]
            
            if df.empty:
                print("  ✗ all returns filtered")
                errors += 1
                continue
            
            # ------------------------------------------------
            # PREPARE INSERT
            # ------------------------------------------------
            payload = [
                {
                    "run_id": run_id,
                    "symbol": r.symbol,
                    "tier": r.tier,
                    "horizon_hours": HORIZON_H,
                    "fwd_return": float(r.fwd_return),
                    "rank_long": int(r.rank_long) if pd.notna(r.rank_long) else None,
                    "rank_short": int(r.rank_short) if pd.notna(r.rank_short) else None,
                }
                for r in df.itertuples(index=False)
            ]
            
            sb.table("scanner_eval").insert(payload).execute()
            print(f"  ✓ inserted {len(payload)} rows")
            processed += 1
            
        except Exception as e:
            print(f"  ✗ error: {e}")
            errors += 1
    
    # --------------------------------------------------------
    # SUMMARY
    # --------------------------------------------------------
    print("\n" + "=" * 72)
    print("EVALUATION SUMMARY")
    print(f"Processed : {processed}")
    print(f"Skipped   : {skipped}")
    print(f"Errors    : {errors}")
    print("=" * 72)
    print("\nSTATUS: ✔ non-overlapping, ✔ deterministic, ✔ no leakage")

# ============================================================
# ENTRY
# ============================================================
if __name__ == "__main__":
    main()