#!/usr/bin/env python3
"""
Test Frontend Extended Data - Should now include Dec 19+ data
"""
import pandas as pd
from supabase import create_client

# Use public credentials (read-only access)
SUPABASE_URL = "https://ywgxccyervulrlulwzav.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3Z3hjY3llcnZ1bHJsdWx3emF2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwNTg3NjcsImV4cCI6MjA4MTYzNDc2N30.gmfIhuh6rcahuNeWhr6yTLlEh77rG57_Mw_COa8yZ94"

def test_frontend_extended_data():
    """Test that frontend now includes more historical data"""
    
    sb = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    print("🧪 Testing Frontend Extended Data (100 runs instead of 20)...")
    
    try:
        dbSymbol = "LIGHT/USDT:USDT"
        
        # Simulate new frontend approach (100 runs)
        extended_runs_response = sb.table("scanner_runs").select("""
            run_id,
            asof_ts,
            universe_size,
            scanner_results!inner(
                symbol,
                scanner_score,
                rank_long,
                tier
            )
        """).eq("scanner_results.symbol", dbSymbol).order("asof_ts", desc=True).limit(100).execute()
        
        extended_runs = extended_runs_response.data
        
        # Get evaluation data for extended runs
        extended_run_ids = [run['run_id'] for run in extended_runs]
        extended_eval_response = sb.table("scanner_eval").select("*").in_("run_id", extended_run_ids).eq("symbol", dbSymbol).eq("horizon_hours", 12).execute()
        extended_eval_data = extended_eval_response.data
        
        if not extended_eval_data:
            extended_eval_response = sb.table("scanner_eval").select("*").in_("run_id", extended_run_ids).eq("symbol", dbSymbol).eq("horizon_hours", 4).execute()
            extended_eval_data = extended_eval_response.data
        
        print(f"📊 Extended LIGHT runs: {len(extended_runs)}")
        print(f"📊 Extended LIGHT eval records: {len(extended_eval_data)}")
        
        # Calculate strategy returns with extended data
        extended_strategy_returns = []
        for run in extended_runs:
            result = run['scanner_results'][0]
            evaluation = next((e for e in extended_eval_data if e['run_id'] == run['run_id']), None)
            
            if evaluation and evaluation['fwd_return'] is not None:
                # Calculate actual regime size (frontend method)
                regime_size_response = sb.table("scanner_results").select("symbol", count="exact", head=True).eq("run_id", run['run_id']).eq("tier", result['tier']).execute()
                regime_size = regime_size_response.count or 1
                
                rank_in_regime = result['rank_long'] or 999
                percentile = rank_in_regime / regime_size
                
                if percentile <= 0.5:  # LONG
                    strategy_return = evaluation['fwd_return'] * 100
                    extended_strategy_returns.append({
                        'return': strategy_return,
                        'timestamp': run['asof_ts'],
                        'run_id': run['run_id'][:8]
                    })
        
        if extended_strategy_returns:
            extended_avg = sum([r['return'] for r in extended_strategy_returns]) / len(extended_strategy_returns)
            extended_profitable = len([r for r in extended_strategy_returns if r['return'] > 0])
            extended_win_rate = (extended_profitable / len(extended_strategy_returns)) * 100
            
            print(f"\n📊 EXTENDED FRONTEND RESULTS:")
            print(f"   LONG trades: {len(extended_strategy_returns)}")
            print(f"   Average strategy return: {extended_avg:+.3f}%")
            print(f"   Win rate: {extended_win_rate:.1f}%")
            print(f"   Profitable/Total: {extended_profitable}/{len(extended_strategy_returns)}")
            
            # Show date range
            timestamps = [r['timestamp'] for r in extended_strategy_returns]
            print(f"   Date range: {min(timestamps)} to {max(timestamps)}")
            
            # Compare with previous results
            print(f"\n📈 COMPARISON:")
            print(f"   Previous (20 runs): -0.077% average, 10 trades")
            print(f"   Extended (100 runs): {extended_avg:+.3f}% average, {len(extended_strategy_returns)} trades")
            print(f"   Improvement: {extended_avg - (-0.077):+.3f}%")
            
            # Show recent vs older performance
            extended_strategy_returns.sort(key=lambda x: x['timestamp'], reverse=True)
            recent_10 = extended_strategy_returns[:10]
            older_data = extended_strategy_returns[10:]
            
            if recent_10:
                recent_avg = sum([r['return'] for r in recent_10]) / len(recent_10)
                print(f"\n📅 PERFORMANCE BY PERIOD:")
                print(f"   Recent 10 trades: {recent_avg:+.3f}% average")
            
            if older_data:
                older_avg = sum([r['return'] for r in older_data]) / len(older_data)
                print(f"   Older {len(older_data)} trades: {older_avg:+.3f}% average")
                print(f"   📊 Older data is {'better' if older_avg > recent_avg else 'worse'} than recent")
        
        print(f"\n✅ EXPECTED FRONTEND BEHAVIOR:")
        print(f"   - LIGHT should now show {extended_avg:+.3f}% average (not negative)")
        print(f"   - Performance metrics should include all {len(extended_strategy_returns)} LONG trades")
        print(f"   - Historical events table should show up to 50 events per page")
        print(f"   - Overall performance should be more representative")
        
    except Exception as e:
        print(f"❌ Error testing extended data: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_frontend_extended_data()