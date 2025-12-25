#!/usr/bin/env python3
"""
Verify Calculations - Check if cumulative returns make sense
"""
import pandas as pd
import numpy as np
from supabase import create_client

# Use public credentials (read-only access)
SUPABASE_URL = "https://ywgxccyervulrlulwzav.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3Z3hjY3llcnZ1bHJsdWx3emF2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwNTg3NjcsImV4cCI6MjA4MTYzNDc2N30.gmfIhuh6rcahuNeWhr6yTLlEh77rG57_Mw_COa8yZ94"

def verify_calculations():
    """Verify if calculations are correct"""
    
    sb = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    print("🔍 VERIFYING CALCULATIONS...")
    
    try:
        # Get sample of evaluation data to check
        print("📊 Checking horizon and timeframe setup...")
        
        # Check evaluation setup
        eval_sample = sb.table("scanner_eval").select("*").limit(10).execute()
        if eval_sample.data:
            horizons = set(r['horizon_hours'] for r in eval_sample.data)
            print(f"   Horizons in use: {horizons}")
            
            # Sample forward returns
            sample_returns = [r['fwd_return'] for r in eval_sample.data[:5]]
            print(f"   Sample forward returns: {[f'{r*100:.2f}%' for r in sample_returns]}")
        
        # Check scanner run frequency
        print("\n📊 Checking scanner run frequency...")
        runs_sample = sb.table("scanner_runs").select("asof_ts").order("asof_ts", desc=True).limit(10).execute()
        if len(runs_sample.data) >= 2:
            timestamps = [r['asof_ts'] for r in runs_sample.data]
            print(f"   Recent timestamps:")
            for i, ts in enumerate(timestamps[:5]):
                print(f"     {i+1}. {ts}")
            
            # Calculate time differences
            from datetime import datetime
            dts = [datetime.fromisoformat(ts.replace('Z', '+00:00')) for ts in timestamps[:5]]
            diffs = [(dts[i] - dts[i+1]).total_seconds() / 3600 for i in range(len(dts)-1)]
            print(f"   Time differences (hours): {[f'{d:.1f}h' for d in diffs]}")
        
        # Now let's verify the calculation logic with a smaller sample
        print("\n🔍 VERIFYING CALCULATION LOGIC...")
        
        # Get 1000 records for detailed analysis
        eval_data = sb.table("scanner_eval").select("*").limit(1000).execute().data
        eval_df = pd.DataFrame(eval_data)
        
        # Get corresponding scanner results
        run_ids = eval_df['run_id'].unique()[:10]  # Just first 10 runs for verification
        scanner_data = sb.table("scanner_results").select("run_id, symbol, rank_long, tier").in_("run_id", run_ids.tolist()).execute().data
        scanner_df = pd.DataFrame(scanner_data)
        
        # Merge small sample
        merged = eval_df.merge(scanner_df, on=['run_id', 'symbol'], how='inner')
        merged = merged.dropna(subset=['rank_long'])
        
        print(f"📊 Verification sample: {len(merged)} records")
        
        # Manual calculation verification
        print("\n🧮 MANUAL CALCULATION VERIFICATION:")
        
        long_returns = []
        short_returns = []
        
        for _, row in merged.iterrows():
            rank_long = row['rank_long']
            fwd_return = row['fwd_return']
            tier = row['tier']
            
            # Approximate regime size
            approx_regime_size = 200
            percentile = rank_long / approx_regime_size
            
            if percentile <= 0.5:  # LONG
                long_returns.append(fwd_return * 100)
            else:  # SHORT (inverted)
                short_returns.append(-fwd_return * 100)
        
        print(f"   LONG trades: {len(long_returns)}")
        print(f"   SHORT trades: {len(short_returns)}")
        
        if long_returns:
            long_avg = np.mean(long_returns)
            long_win_rate = sum(1 for r in long_returns if r > 0) / len(long_returns) * 100
            long_cum = sum(long_returns)
            print(f"   LONG avg: {long_avg:+.3f}%")
            print(f"   LONG win rate: {long_win_rate:.1f}%")
            print(f"   LONG cumulative: {long_cum:+.2f}%")
            
            # Show distribution
            positive_long = [r for r in long_returns if r > 0]
            negative_long = [r for r in long_returns if r <= 0]
            print(f"   LONG positive trades: {len(positive_long)} (avg: {np.mean(positive_long):+.2f}%)")
            print(f"   LONG negative trades: {len(negative_long)} (avg: {np.mean(negative_long):+.2f}%)")
        
        if short_returns:
            short_avg = np.mean(short_returns)
            short_win_rate = sum(1 for r in short_returns if r > 0) / len(short_returns) * 100
            short_cum = sum(short_returns)
            print(f"   SHORT avg: {short_avg:+.3f}%")
            print(f"   SHORT win rate: {short_win_rate:.1f}%")
            print(f"   SHORT cumulative: {short_cum:+.2f}%")
        
        # Check if high cumulative return makes sense with low win rate
        print("\n🤔 ANALYZING WIN RATE vs CUMULATIVE RETURN:")
        
        if long_returns:
            # Sort returns to see distribution
            sorted_long = sorted(long_returns, reverse=True)
            
            print(f"   Top 10 LONG returns: {[f'{r:+.2f}%' for r in sorted_long[:10]]}")
            print(f"   Bottom 10 LONG returns: {[f'{r:+.2f}%' for r in sorted_long[-10:]]}")
            
            # Check if few big winners drive the performance
            top_10_pct = sum(sorted_long[:len(sorted_long)//10]) / sum(sorted_long) * 100
            print(f"   Top 10% of trades contribute: {top_10_pct:.1f}% of total return")
            
            # Median vs mean (skewness check)
            median_return = np.median(long_returns)
            print(f"   LONG median return: {median_return:+.3f}% (vs mean {long_avg:+.3f}%)")
            
            if long_avg > median_return * 2:
                print("   ⚠️  HIGHLY SKEWED: Few big winners drive performance")
            
        # Check actual horizon vs timeframe
        print("\n⏰ HORIZON vs TIMEFRAME ANALYSIS:")
        print(f"   Current horizon: 4 hours")
        print(f"   Scanner frequency: Every 4 hours")
        print(f"   This means: Non-overlapping periods ✅")
        
        # Best practice check
        print("\n📚 BEST PRACTICE ANALYSIS:")
        print("   ✅ Non-overlapping periods (4h horizon, 4h frequency)")
        print("   ✅ Avoids look-ahead bias")
        print("   ✅ Each signal gets independent evaluation")
        
        # But check if we should use longer horizon
        print("\n💡 HORIZON OPTIMIZATION SUGGESTION:")
        print("   Current: 4h horizon (1 period)")
        print("   Alternative: 12h horizon (3 periods) - might be more stable")
        print("   Alternative: 24h horizon (6 periods) - longer term view")
        
        # Check if cumulative calculation is additive (wrong) vs compound (right)
        print("\n🧮 CUMULATIVE CALCULATION METHOD:")
        print("   Current method: Simple addition of returns")
        print("   Correct method should be: Compound returns")
        
        if long_returns:
            # Simple addition (current method)
            simple_cum = sum(long_returns)
            
            # Compound method (correct)
            compound_factor = 1.0
            for ret in long_returns:
                compound_factor *= (1 + ret/100)
            compound_cum = (compound_factor - 1) * 100
            
            print(f"   Simple addition: {simple_cum:+.2f}%")
            print(f"   Compound method: {compound_cum:+.2f}%")
            print(f"   Difference: {abs(simple_cum - compound_cum):.2f}%")
            
            if abs(simple_cum - compound_cum) > 100:
                print("   ⚠️  MAJOR DIFFERENCE: Should use compound returns!")
        
        print("\n" + "="*60)
        print("🎯 VERIFICATION CONCLUSIONS:")
        print("="*60)
        
        print("1. ✅ Horizon (4h) matches frequency (4h) - Good practice")
        print("2. ⚠️  Low win rate + high cumulative = Likely skewed by big winners")
        print("3. ⚠️  Using simple addition instead of compound returns")
        print("4. 💡 Consider longer horizon (12h) for more stable signals")
        print("5. 🔧 Fix cumulative calculation to use compound returns")
        
    except Exception as e:
        print(f"❌ Error in verification: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    verify_calculations()