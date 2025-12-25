#!/usr/bin/env python3
"""
Investigate Discrepancy Between Backtest and Frontend
Both use same DB - why different results for LIGHT?
"""
import pandas as pd
from supabase import create_client

# Use public credentials (read-only access)
SUPABASE_URL = "https://ywgxccyervulrlulwzav.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3Z3hjY3llcnZ1bHJsdWx3emF2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwNTg3NjcsImV4cCI6MjA4MTYzNDc2N30.gmfIhuh6rcahuNeWhr6yTLlEh77rG57_Mw_COa8yZ94"

def investigate_discrepancy():
    """Investigate why backtest shows +11.2% but frontend shows -0.096% for LIGHT"""
    
    sb = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    print("🔍 Investigating Backtest vs Frontend Discrepancy...")
    print("="*80)
    
    try:
        # 1. GET ALL LIGHT DATA (like backtest did)
        print("\n1️⃣ BACKTEST APPROACH - ALL LIGHT DATA:")
        
        # Get ALL LIGHT evaluation data
        all_eval_response = sb.table("scanner_eval").select("*").like("symbol", "%LIGHT%").execute()
        all_eval_data = all_eval_response.data
        
        # Get ALL LIGHT scanner results
        all_run_ids = [item['run_id'] for item in all_eval_data]
        all_scanner_response = sb.table("scanner_results").select("*").like("symbol", "%LIGHT%").in_("run_id", all_run_ids).execute()
        all_scanner_data = all_scanner_response.data
        
        print(f"   Total LIGHT eval records: {len(all_eval_data)}")
        print(f"   Total LIGHT scanner records: {len(all_scanner_data)}")
        
        # Create lookup for scanner results
        scanner_lookup = {}
        for scanner_item in all_scanner_data:
            key = f"{scanner_item['run_id']}_{scanner_item['symbol']}"
            scanner_lookup[key] = scanner_item
        
        # Calculate strategy returns (backtest method)
        backtest_strategy_returns = []
        for eval_item in all_eval_data:
            raw_return = eval_item.get('fwd_return', 0) or 0
            
            # Look up corresponding scanner result
            key = f"{eval_item['run_id']}_{eval_item['symbol']}"
            scanner_result = scanner_lookup.get(key)
            
            if scanner_result and scanner_result.get('rank_long'):
                # Calculate regime size (backtest method - approximation)
                tier = scanner_result.get('tier')
                if tier == 'LARGE':
                    regime_size = 200  # HIGH_VOL
                elif tier == 'MID':
                    regime_size = 200  # MID_VOL
                else:  # SMALL
                    regime_size = 200  # LOW_VOL
                
                rank_long = scanner_result.get('rank_long')
                percentile = rank_long / regime_size
                
                # Strategy logic: LONG if top 50%
                if percentile <= 0.5:
                    strategy_return = raw_return * 100  # LONG
                    backtest_strategy_returns.append(strategy_return)
        
        backtest_avg = sum(backtest_strategy_returns) / len(backtest_strategy_returns) if backtest_strategy_returns else 0
        backtest_profitable = len([r for r in backtest_strategy_returns if r > 0])
        backtest_win_rate = (backtest_profitable / len(backtest_strategy_returns)) * 100 if backtest_strategy_returns else 0
        
        print(f"   Backtest LONG trades: {len(backtest_strategy_returns)}")
        print(f"   Backtest average: {backtest_avg:+.3f}%")
        print(f"   Backtest win rate: {backtest_win_rate:.1f}%")
        
        # 2. GET RECENT LIGHT DATA (like frontend does)
        print("\n2️⃣ FRONTEND APPROACH - RECENT LIGHT DATA:")
        
        dbSymbol = "LIGHT/USDT:USDT"
        
        # Get recent runs with LIGHT (frontend method)
        recent_runs_response = sb.table("scanner_runs").select("""
            run_id,
            asof_ts,
            universe_size,
            scanner_results!inner(
                symbol,
                scanner_score,
                rank_long,
                tier
            )
        """).eq("scanner_results.symbol", dbSymbol).order("asof_ts", desc=True).limit(20).execute()
        
        recent_runs = recent_runs_response.data
        
        # Get evaluation data for recent runs
        recent_run_ids = [run['run_id'] for run in recent_runs]
        recent_eval_response = sb.table("scanner_eval").select("*").in_("run_id", recent_run_ids).eq("symbol", dbSymbol).eq("horizon_hours", 12).execute()
        recent_eval_data = recent_eval_response.data
        
        if not recent_eval_data:
            recent_eval_response = sb.table("scanner_eval").select("*").in_("run_id", recent_run_ids).eq("symbol", dbSymbol).eq("horizon_hours", 4).execute()
            recent_eval_data = recent_eval_response.data
        
        print(f"   Recent LIGHT runs: {len(recent_runs)}")
        print(f"   Recent LIGHT eval records: {len(recent_eval_data)}")
        
        # Calculate strategy returns (frontend method)
        frontend_strategy_returns = []
        for run in recent_runs:
            result = run['scanner_results'][0]
            evaluation = next((e for e in recent_eval_data if e['run_id'] == run['run_id']), None)
            
            if evaluation and evaluation['fwd_return'] is not None:
                # Calculate actual regime size (frontend method)
                regime_size_response = sb.table("scanner_results").select("symbol", count="exact", head=True).eq("run_id", run['run_id']).eq("tier", result['tier']).execute()
                regime_size = regime_size_response.count or 1
                
                rank_in_regime = result['rank_long'] or 999
                percentile = rank_in_regime / regime_size
                
                if percentile <= 0.5:  # LONG
                    strategy_return = evaluation['fwd_return'] * 100
                    frontend_strategy_returns.append(strategy_return)
        
        frontend_avg = sum(frontend_strategy_returns) / len(frontend_strategy_returns) if frontend_strategy_returns else 0
        frontend_profitable = len([r for r in frontend_strategy_returns if r > 0])
        frontend_win_rate = (frontend_profitable / len(frontend_strategy_returns)) * 100 if frontend_strategy_returns else 0
        
        print(f"   Frontend LONG trades: {len(frontend_strategy_returns)}")
        print(f"   Frontend average: {frontend_avg:+.3f}%")
        print(f"   Frontend win rate: {frontend_win_rate:.1f}%")
        
        # 3. ANALYZE THE DIFFERENCES
        print(f"\n3️⃣ DIFFERENCE ANALYSIS:")
        print(f"   Backtest: {backtest_avg:+.3f}% ({len(backtest_strategy_returns)} trades)")
        print(f"   Frontend: {frontend_avg:+.3f}% ({len(frontend_strategy_returns)} trades)")
        print(f"   Difference: {backtest_avg - frontend_avg:+.3f}%")
        
        # 4. CHECK REGIME SIZE CALCULATION DIFFERENCES
        print(f"\n4️⃣ REGIME SIZE COMPARISON:")
        
        # Sample a few runs to compare regime size calculation methods
        sample_runs = recent_runs[:3]
        for run in sample_runs:
            result = run['scanner_results'][0]
            
            # Backtest method (approximation)
            tier = result['tier']
            if tier == 'LARGE':
                backtest_regime_size = 200
            elif tier == 'MID':
                backtest_regime_size = 200
            else:
                backtest_regime_size = 200
            
            # Frontend method (actual count)
            regime_size_response = sb.table("scanner_results").select("symbol", count="exact", head=True).eq("run_id", run['run_id']).eq("tier", result['tier']).execute()
            frontend_regime_size = regime_size_response.count or 1
            
            rank = result['rank_long']
            backtest_percentile = rank / backtest_regime_size if rank else 0
            frontend_percentile = rank / frontend_regime_size if rank else 0
            
            print(f"   Run {run['run_id'][:8]}: Tier {tier}")
            print(f"     Backtest regime size: {backtest_regime_size} -> percentile: {backtest_percentile:.3f}")
            print(f"     Frontend regime size: {frontend_regime_size} -> percentile: {frontend_percentile:.3f}")
            print(f"     Rank: {rank}")
        
        # 5. TIME PERIOD ANALYSIS
        print(f"\n5️⃣ TIME PERIOD ANALYSIS:")
        
        # Get timestamps for all data vs recent data
        all_timestamps = [item.get('run_id') for item in all_eval_data]
        recent_timestamps = [run['run_id'] for run in recent_runs]
        
        # Get actual run timestamps
        all_runs_response = sb.table("scanner_runs").select("run_id, asof_ts").in_("run_id", all_timestamps[:10]).execute()
        recent_runs_response = sb.table("scanner_runs").select("run_id, asof_ts").in_("run_id", recent_timestamps[:10]).execute()
        
        if all_runs_response.data:
            all_dates = [run['asof_ts'] for run in all_runs_response.data]
            print(f"   All data date range: {min(all_dates)} to {max(all_dates)}")
        
        if recent_runs_response.data:
            recent_dates = [run['asof_ts'] for run in recent_runs_response.data]
            print(f"   Recent data date range: {min(recent_dates)} to {max(recent_dates)}")
        
        # 6. CONCLUSION
        print(f"\n6️⃣ CONCLUSION:")
        if abs(backtest_avg - frontend_avg) > 1.0:  # Significant difference
            print(f"   🚨 SIGNIFICANT DISCREPANCY FOUND!")
            print(f"   Possible causes:")
            print(f"   1. Different time periods (all vs recent data)")
            print(f"   2. Different regime size calculation methods")
            print(f"   3. Different data filtering (horizon_hours, etc.)")
            print(f"   4. Frontend pagination limiting data")
        else:
            print(f"   ✅ Discrepancy is within expected range")
            print(f"   Likely due to different time periods or sample sizes")
        
    except Exception as e:
        print(f"❌ Error investigating discrepancy: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    investigate_discrepancy()