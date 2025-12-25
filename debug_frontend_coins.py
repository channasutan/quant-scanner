#!/usr/bin/env python3
"""
Debug Frontend Coin Performance Issues
Specifically analyze SOL, LIGHT, RAVE and other coins showing negative performance
"""
import pandas as pd
import numpy as np
from supabase import create_client
from datetime import datetime

# Use public credentials (read-only access)
SUPABASE_URL = "https://ywgxccyervulrlulwzav.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3Z3hjY3llcnZ1bHJsdWx3emF2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwNTg3NjcsImV4cCI6MjA4MTYzNDc2N30.gmfIhuh6rcahuNeWhr6yTLlEh77rG57_Mw_COa8yZ94"

def debug_specific_coins():
    """Debug specific coins showing negative performance in frontend"""
    
    sb = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    # Coins to analyze
    target_coins = ['SOL', 'LIGHT', 'RAVE', 'BTC', 'ETH']
    
    print("🔍 Debugging Frontend Coin Performance Issues")
    print("="*80)
    
    for coin_symbol in target_coins:
        print(f"\n🎯 ANALYZING {coin_symbol}:")
        print("-" * 50)
        
        # Convert to database format
        db_symbol = f"{coin_symbol}/USDT:USDT"
        
        try:
            # Get historical runs for this coin
            hist_response = sb.table("scanner_runs").select("""
                run_id,
                asof_ts,
                universe_size,
                scanner_results!inner(
                    symbol,
                    scanner_score,
                    rank_long,
                    tier
                )
            """).eq('scanner_results.symbol', db_symbol).order('asof_ts', ascending=False).limit(20).execute()
            
            historical_runs = hist_response.data
            
            if not historical_runs:
                print(f"   ❌ No historical data found for {db_symbol}")
                continue
                
            print(f"   📊 Found {len(historical_runs)} historical runs")
            
            # Get evaluation data for these runs
            run_ids = [run['run_id'] for run in historical_runs]
            
            # Try horizon 12 first, then 4
            eval_response = sb.table("scanner_eval").select("*").in_("run_id", run_ids).eq("symbol", db_symbol).eq("horizon_hours", 12).execute()
            eval_data = eval_response.data
            
            if not eval_data:
                eval_response = sb.table("scanner_eval").select("*").in_("run_id", run_ids).eq("symbol", db_symbol).eq("horizon_hours", 4).execute()
                eval_data = eval_response.data
                print(f"   ⚠️  Using horizon 4 as fallback")
            else:
                print(f"   ✅ Using horizon 12")
                
            print(f"   📊 Found {len(eval_data)} evaluation records")
            
            # Process each historical event
            events = []
            for run in historical_runs:
                result = run['scanner_results'][0]
                evaluation = next((e for e in eval_data if e['run_id'] == run['run_id']), None)
                
                rank_in_regime = result['rank_long'] or 999
                
                # Calculate regime size (approximate)
                regime_size = 200  # Approximate
                percentile = rank_in_regime / regime_size
                
                # Determine direction - LONG ONLY
                derived_direction = 'LONG' if percentile <= 0.5 else 'NEUTRAL'
                
                # Get forward return
                fwd_return = evaluation['fwd_return'] if evaluation else None
                
                # Filter extreme outliers
                if fwd_return is not None and abs(fwd_return) > 0.5:
                    print(f"   ⚠️  Filtering extreme return: {fwd_return*100:.2f}%")
                    fwd_return = None
                
                events.append({
                    'timestamp': run['asof_ts'],
                    'run_id': run['run_id'],
                    'rank_in_regime': rank_in_regime,
                    'percentile': percentile,
                    'derived_direction': derived_direction,
                    'forward_return': fwd_return * 100 if fwd_return is not None else None,
                    'tier': result['tier'],
                    'has_eval': evaluation is not None
                })
            
            # Sort by timestamp ascending for cumulative calculation
            events.sort(key=lambda x: x['timestamp'])
            
            print(f"\n   📊 EVENT BREAKDOWN:")
            print(f"   {'Timestamp':<20} {'Rank':<5} {'%ile':<5} {'Dir':<8} {'Return':<8} {'Tier':<6}")
            print("   " + "-" * 65)
            
            # Calculate cumulative returns using frontend logic
            cumulative_multiplier = 1.0
            long_signals = 0
            total_long_return = 0
            
            for i, event in enumerate(events):
                if event['forward_return'] is not None and event['derived_direction'] == 'LONG':
                    signal_return = event['forward_return'] / 100
                    cumulative_multiplier *= (1 + signal_return)
                    long_signals += 1
                    total_long_return += event['forward_return']
                
                cumulative_return = (cumulative_multiplier - 1) * 100
                
                # Display event
                ts = datetime.fromisoformat(event['timestamp'].replace('Z', '+00:00')).strftime('%Y-%m-%d %H:%M')
                rank_str = str(event['rank_in_regime'])
                percentile_str = f"{event['percentile']:.2f}"
                direction_str = event['derived_direction']
                return_str = f"{event['forward_return']:+.2f}%" if event['forward_return'] is not None else "Pending"
                tier_str = event['tier']
                
                print(f"   {ts:<20} {rank_str:<5} {percentile_str:<5} {direction_str:<8} {return_str:<8} {tier_str:<6}")
            
            # Summary statistics
            final_cumulative = (cumulative_multiplier - 1) * 100
            avg_long_return = total_long_return / long_signals if long_signals > 0 else 0
            
            print(f"\n   📊 SUMMARY:")
            print(f"   Total Events: {len(events)}")
            print(f"   LONG Signals: {long_signals}")
            print(f"   Average LONG Return: {avg_long_return:+.3f}%")
            print(f"   Total Simple Return: {total_long_return:+.2f}%")
            print(f"   Final Cumulative Return: {final_cumulative:+.2f}%")
            
            # Check if this matches backend analysis
            if long_signals > 0:
                if final_cumulative < 0:
                    print(f"   ❌ NEGATIVE CUMULATIVE RETURN - This explains frontend issue!")
                else:
                    print(f"   ✅ Positive cumulative return")
            else:
                print(f"   ⚠️  No LONG signals found")
                
        except Exception as e:
            print(f"   ❌ Error analyzing {coin_symbol}: {e}")
    
    print(f"\n" + "="*80)
    print("💡 DEBUGGING CONCLUSIONS:")
    print("="*80)
    print("1. Check if coins have sufficient LONG signals (percentile ≤ 0.5)")
    print("2. Verify horizon consistency (12h vs 4h)")
    print("3. Confirm outlier filtering is not removing valid data")
    print("4. Ensure cumulative calculation matches backend logic")

if __name__ == "__main__":
    debug_specific_coins()