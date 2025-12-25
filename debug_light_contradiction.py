#!/usr/bin/env python3
"""
Debug LIGHT Contradiction - Why backtest shows profit but frontend shows loss?
"""
import pandas as pd
from supabase import create_client

# Use public credentials (read-only access)
SUPABASE_URL = "https://ywgxccyervulrlulwzav.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3Z3hjY3llcnZ1bHJsdWx3emF2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwNTg3NjcsImV4cCI6MjA4MTYzNDc2N30.gmfIhuh6rcahuNeWhr6yTLlEh77rG57_Mw_COa8yZ94"

def debug_light_contradiction():
    """Debug why LIGHT shows different results in different analyses"""
    
    sb = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    print("🔍 Debugging LIGHT Contradiction...")
    print("="*80)
    
    try:
        # 1. REPRODUCE ORIGINAL BACKTEST EXACTLY
        print("\n1️⃣ REPRODUCING ORIGINAL BACKTEST (from full_database_analysis.py):")
        
        # Get ALL LIGHT evaluation data
        eval_response = sb.table("scanner_eval").select("*").like("symbol", "%LIGHT%").execute()
        eval_data = pd.DataFrame(eval_response.data)
        
        # Get ALL LIGHT scanner results  
        run_ids = eval_data['run_id'].unique()
        scanner_response = sb.table("scanner_results").select("*").like("symbol", "%LIGHT%").in_("run_id", run_ids.tolist()).execute()
        scanner_data = pd.DataFrame(scanner_response.data)
        
        print(f"   Total LIGHT eval records: {len(eval_data)}")
        print(f"   Total LIGHT scanner records: {len(scanner_data)}")
        
        # Merge like the original backtest
        merged = eval_data.merge(scanner_data, on=['run_id', 'symbol'], how='inner', suffixes=('_eval', '_scanner'))
        
        # Check available columns
        print(f"   Available columns: {list(merged.columns)}")
        
        # Use the correct rank_long column name
        rank_col = 'rank_long_scanner' if 'rank_long_scanner' in merged.columns else 'rank_long'
        merged = merged.dropna(subset=[rank_col])
        
        print(f"   Merged records with rank_long: {len(merged)}")
        
        # Calculate strategy returns EXACTLY like original backtest
        strategy_returns = []
        for _, row in merged.iterrows():
            tier = row['tier']
            rank_long = row[rank_col]
            fwd_return = row['fwd_return']
            
            # Original backtest regime size calculation
            if tier == 'LARGE':
                approx_regime_size = 200  # HIGH_VOL
            elif tier == 'MID':
                approx_regime_size = 200  # MID_VOL  
            else:  # SMALL
                approx_regime_size = 200  # LOW_VOL
                
            percentile = rank_long / approx_regime_size
            
            # Original strategy logic: LONG if top 50%, SHORT if bottom 50%
            if percentile <= 0.5:  # LONG position
                strategy_return = fwd_return
                direction = 'LONG'
            else:  # SHORT position  
                strategy_return = -fwd_return  # Invert for short
                direction = 'SHORT'
                
            strategy_returns.append({
                'raw_return': fwd_return * 100,
                'strategy_return': strategy_return * 100,
                'direction': direction,
                'percentile': percentile,
                'tier': tier,
                'run_id': row['run_id']
            })
        
        strategy_df = pd.DataFrame(strategy_returns)
        
        # Overall performance
        total_trades = len(strategy_df)
        avg_strategy_return = strategy_df['strategy_return'].mean()
        
        # LONG-only performance
        long_data = strategy_df[strategy_df['direction'] == 'LONG']
        long_avg = long_data['strategy_return'].mean() if not long_data.empty else 0
        long_count = len(long_data)
        
        print(f"   Original Backtest Results:")
        print(f"     Total trades: {total_trades}")
        print(f"     Overall avg: {avg_strategy_return:+.3f}%")
        print(f"     LONG trades: {long_count}")
        print(f"     LONG avg: {long_avg:+.3f}%")
        
        # 2. CHECK WHAT FRONTEND SEES WITH SAME DATA
        print(f"\n2️⃣ FRONTEND METHOD WITH SAME DATA:")
        
        # Use the same merged data but apply frontend logic
        frontend_strategy_returns = []
        
        for _, row in merged.iterrows():
            tier = row['tier']
            rank_long = row[rank_col]
            fwd_return = row['fwd_return']
            run_id = row['run_id']
            
            # Frontend method: Get actual regime size
            regime_size_response = sb.table("scanner_results").select("symbol", count="exact", head=True).eq("run_id", run_id).eq("tier", tier).execute()
            actual_regime_size = regime_size_response.count or 1
            
            percentile = rank_long / actual_regime_size
            
            # Frontend logic: LONG if top 50%, NEUTRAL if bottom 50%
            if percentile <= 0.5:  # LONG position
                strategy_return = fwd_return * 100
                direction = 'LONG'
                frontend_strategy_returns.append(strategy_return)
            # NEUTRAL positions are not included in performance calculation
        
        frontend_avg = sum(frontend_strategy_returns) / len(frontend_strategy_returns) if frontend_strategy_returns else 0
        
        print(f"   Frontend Method Results:")
        print(f"     LONG trades: {len(frontend_strategy_returns)}")
        print(f"     LONG avg: {frontend_avg:+.3f}%")
        
        # 3. COMPARE REGIME SIZE CALCULATIONS
        print(f"\n3️⃣ REGIME SIZE COMPARISON:")
        
        # Sample some runs to see the difference
        sample_runs = merged['run_id'].unique()[:5]
        
        for run_id in sample_runs:
            run_data = merged[merged['run_id'] == run_id].iloc[0]
            tier = run_data['tier']
            rank_long = run_data[rank_col]
            
            # Backtest method
            if tier == 'LARGE':
                backtest_regime_size = 200
            elif tier == 'MID':
                backtest_regime_size = 200
            else:
                backtest_regime_size = 200
            
            # Frontend method
            regime_size_response = sb.table("scanner_results").select("symbol", count="exact", head=True).eq("run_id", run_id).eq("tier", tier).execute()
            frontend_regime_size = regime_size_response.count or 1
            
            backtest_percentile = rank_long / backtest_regime_size
            frontend_percentile = rank_long / frontend_regime_size
            
            backtest_direction = 'LONG' if backtest_percentile <= 0.5 else 'SHORT'
            frontend_direction = 'LONG' if frontend_percentile <= 0.5 else 'NEUTRAL'
            
            print(f"   Run {run_id[:8]}: Tier {tier}, Rank {rank_long}")
            print(f"     Backtest: regime={backtest_regime_size}, percentile={backtest_percentile:.3f}, direction={backtest_direction}")
            print(f"     Frontend: regime={frontend_regime_size}, percentile={frontend_percentile:.3f}, direction={frontend_direction}")
        
        # 4. CHECK IF THERE'S A SYSTEMATIC BIAS
        print(f"\n4️⃣ SYSTEMATIC BIAS CHECK:")
        
        # Count how many trades change classification
        classification_changes = 0
        for _, row in merged.iterrows():
            tier = row['tier']
            rank_long = row[rank_col]
            run_id = row['run_id']
            
            # Backtest classification
            backtest_regime_size = 200
            backtest_percentile = rank_long / backtest_regime_size
            backtest_is_long = backtest_percentile <= 0.5
            
            # Frontend classification
            regime_size_response = sb.table("scanner_results").select("symbol", count="exact", head=True).eq("run_id", run_id).eq("tier", tier).execute()
            frontend_regime_size = regime_size_response.count or 1
            frontend_percentile = rank_long / frontend_regime_size
            frontend_is_long = frontend_percentile <= 0.5
            
            if backtest_is_long != frontend_is_long:
                classification_changes += 1
        
        print(f"   Classification changes: {classification_changes}/{len(merged)} ({classification_changes/len(merged)*100:.1f}%)")
        
        # 5. FINAL ANALYSIS
        print(f"\n5️⃣ FINAL ANALYSIS:")
        
        if abs(long_avg - frontend_avg) > 0.5:
            print(f"   🚨 SIGNIFICANT DIFFERENCE FOUND!")
            print(f"   Backtest LONG: {long_avg:+.3f}% ({long_count} trades)")
            print(f"   Frontend LONG: {frontend_avg:+.3f}% ({len(frontend_strategy_returns)} trades)")
            print(f"   Difference: {long_avg - frontend_avg:+.3f}%")
            
            print(f"\n   Possible causes:")
            print(f"   1. Different regime size calculations affect LONG/SHORT classification")
            print(f"   2. Backtest uses approximation (200), frontend uses actual counts")
            print(f"   3. Some trades classified as LONG in backtest become NEUTRAL in frontend")
            print(f"   4. Frontend excludes NEUTRAL trades from performance calculation")
        else:
            print(f"   ✅ Results are consistent")
        
        # 6. SHOW WHICH METHOD IS MORE ACCURATE
        print(f"\n6️⃣ ACCURACY ASSESSMENT:")
        print(f"   Backtest method: Uses approximation, includes SHORT trades")
        print(f"   Frontend method: Uses actual regime sizes, LONG-only strategy")
        print(f"   ")
        print(f"   The frontend method is more accurate because:")
        print(f"   - Uses actual regime sizes instead of approximation")
        print(f"   - Implements LONG-only strategy correctly")
        print(f"   - Excludes NEUTRAL positions (no directional bet)")
        
    except Exception as e:
        print(f"❌ Error debugging contradiction: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_light_contradiction()