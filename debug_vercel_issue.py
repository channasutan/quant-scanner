#!/usr/bin/env python3
"""
Debug Vercel Frontend Issue - Why LIGHT still shows negative returns
"""
import pandas as pd
from supabase import create_client

# Use public credentials (read-only access)
SUPABASE_URL = "https://ywgxccyervulrlulwzav.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3Z3hjY3llcnZ1bHJsdWx3emF2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwNTg3NjcsImV4cCI6MjA4MTYzNDc2N30.gmfIhuh6rcahuNeWhr6yTLlEh77rG57_Mw_COa8yZ94"

def debug_vercel_light_issue():
    """Debug why Vercel frontend still shows negative LIGHT returns"""
    
    sb = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    print("🔍 Debugging Vercel LIGHT Issue...")
    
    try:
        # Get recent LIGHT data (what frontend would show)
        dbSymbol = "LIGHT/USDT:USDT"
        
        # Get latest 10 runs with LIGHT data (same as frontend pagination)
        historical_runs_response = sb.table("scanner_runs").select("""
            run_id,
            asof_ts,
            universe_size,
            scanner_results!inner(
                symbol,
                scanner_score,
                rank_long,
                tier
            )
        """).eq("scanner_results.symbol", dbSymbol).order("asof_ts", desc=True).limit(10).execute()
        
        historical_runs = historical_runs_response.data
        
        if not historical_runs:
            print("❌ No LIGHT historical runs found")
            return
            
        print(f"📊 Found {len(historical_runs)} LIGHT historical runs")
        
        # Get evaluation data for these runs
        run_ids = [run['run_id'] for run in historical_runs]
        
        # Try horizon 12 first, fallback to 4
        eval_response = sb.table("scanner_eval").select("*").in_("run_id", run_ids).eq("symbol", dbSymbol).eq("horizon_hours", 12).execute()
        eval_data = eval_response.data
        
        if not eval_data:
            print("No data with horizon 12, trying horizon 4...")
            eval_response = sb.table("scanner_eval").select("*").in_("run_id", run_ids).eq("symbol", dbSymbol).eq("horizon_hours", 4).execute()
            eval_data = eval_response.data
        
        print(f"📊 Found {len(eval_data)} LIGHT evaluation records")
        
        # Process each run like the frontend does
        print(f"\n📋 LIGHT Frontend Processing (Recent 10 runs):")
        print(f"{'Run ID':<8} {'Tier':<6} {'Rank':<5} {'Regime':<7} {'Percentile':<10} {'Direction':<8} {'Raw%':<8} {'Strategy%':<10}")
        print("-" * 85)
        
        for run in historical_runs:
            result = run['scanner_results'][0]
            evaluation = next((e for e in eval_data if e['run_id'] == run['run_id']), None)
            
            # Calculate regime size (same as frontend)
            regime_size_response = sb.table("scanner_results").select("symbol", count="exact", head=True).eq("run_id", run['run_id']).eq("tier", result['tier']).execute()
            regime_size = regime_size_response.count or 1
            
            rank_in_regime = result['rank_long'] or 999
            percentile = rank_in_regime / regime_size
            
            # Frontend logic
            derived_direction = 'LONG' if percentile <= 0.5 else 'NEUTRAL'
            
            fwd_return = evaluation['fwd_return'] if evaluation else None
            
            # Strategy return calculation
            if fwd_return is not None:
                if derived_direction == 'LONG':
                    strategy_return = fwd_return  # Use actual return
                else:
                    strategy_return = 0  # No position
            else:
                strategy_return = None
            
            run_short = run['run_id'][:8]
            raw_pct = f"{fwd_return*100:+6.2f}%" if fwd_return is not None else "None"
            strategy_pct = f"{strategy_return*100:+6.2f}%" if strategy_return is not None else "None"
            
            print(f"{run_short:<8} {result['tier']:<6} {rank_in_regime:<5} {regime_size:<7} {percentile:<10.3f} {derived_direction:<8} {raw_pct:<8} {strategy_pct:<10}")
        
        # Check what the frontend performance calculation would show
        long_strategy_returns = []
        for run in historical_runs:
            result = run['scanner_results'][0]
            evaluation = next((e for e in eval_data if e['run_id'] == run['run_id']), None)
            
            if evaluation and evaluation['fwd_return'] is not None:
                # Calculate regime size
                regime_size_response = sb.table("scanner_results").select("symbol", count="exact", head=True).eq("run_id", run['run_id']).eq("tier", result['tier']).execute()
                regime_size = regime_size_response.count or 1
                
                rank_in_regime = result['rank_long'] or 999
                percentile = rank_in_regime / regime_size
                
                if percentile <= 0.5:  # LONG
                    long_strategy_returns.append(evaluation['fwd_return'] * 100)
        
        if long_strategy_returns:
            avg_strategy_return = sum(long_strategy_returns) / len(long_strategy_returns)
            profitable_count = len([r for r in long_strategy_returns if r > 0])
            win_rate = (profitable_count / len(long_strategy_returns)) * 100
            
            print(f"\n📊 Frontend Performance Calculation:")
            print(f"   LONG Strategy Returns: {long_strategy_returns}")
            print(f"   Average Strategy Return: {avg_strategy_return:+.3f}%")
            print(f"   Win Rate: {win_rate:.1f}%")
            print(f"   Profitable/Total: {profitable_count}/{len(long_strategy_returns)}")
        
        print(f"\n🔍 Possible Issues:")
        print(f"   1. Vercel deployment might be using cached/old code")
        print(f"   2. Environment variables might be different")
        print(f"   3. Build process might have failed silently")
        print(f"   4. Browser cache might be showing old version")
        
        print(f"\n💡 Solutions:")
        print(f"   1. Force redeploy on Vercel")
        print(f"   2. Clear browser cache and hard refresh")
        print(f"   3. Check Vercel build logs for errors")
        print(f"   4. Merge to main branch if Vercel is configured for main")
        
    except Exception as e:
        print(f"❌ Error debugging Vercel issue: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_vercel_light_issue()