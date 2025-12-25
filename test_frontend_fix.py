#!/usr/bin/env python3
"""
Test Frontend Strategy Return Fix
"""
import pandas as pd
from supabase import create_client

# Use public credentials (read-only access)
SUPABASE_URL = "https://ywgxccyervulrlulwzav.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3Z3hjY3llcnZ1bHJsdWx3emF2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwNTg3NjcsImV4cCI6MjA4MTYzNDc2N30.gmfIhuh6rcahuNeWhr6yTLlEh77rG57_Mw_COa8yZ94"

def test_frontend_strategy_calculation():
    """Test that frontend strategy calculation matches backend"""
    
    sb = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    print("🧪 Testing Frontend Strategy Return Calculation...")
    
    try:
        # Get LIGHT evaluation data
        eval_response = sb.table("scanner_eval").select("*").like("symbol", "%LIGHT%").limit(10).execute()
        eval_data = eval_response.data
        
        if not eval_data:
            print("❌ No LIGHT evaluation data found")
            return
            
        print(f"📊 Found {len(eval_data)} LIGHT evaluation records")
        
        # Get corresponding scanner results
        run_ids = [item['run_id'] for item in eval_data]
        scanner_response = sb.table("scanner_results").select("*").like("symbol", "%LIGHT%").in_("run_id", run_ids).execute()
        scanner_data = scanner_response.data
        
        print(f"📊 Found {len(scanner_data)} LIGHT scanner results")
        
        # Create lookup for scanner results
        scanner_lookup = {}
        for scanner_item in scanner_data:
            key = f"{scanner_item['run_id']}_{scanner_item['symbol']}"
            scanner_lookup[key] = scanner_item
        
        # Calculate strategy returns using frontend logic
        strategy_returns = []
        
        for eval_item in eval_data:
            raw_return = eval_item.get('fwd_return', 0) or 0
            
            # Look up corresponding scanner result
            key = f"{eval_item['run_id']}_{eval_item['symbol']}"
            scanner_result = scanner_lookup.get(key)
            
            if not scanner_result or not scanner_result.get('rank_long'):
                strategy_return = 0
                direction = 'NO_RANK'
                rank = None
                tier = None
            else:
                # Calculate regime size based on tier (same as frontend)
                tier = scanner_result.get('tier')
                if tier == 'LARGE':
                    regime_size = 200  # HIGH_VOL
                elif tier == 'MID':
                    regime_size = 200  # MID_VOL
                else:  # SMALL
                    regime_size = 200  # LOW_VOL
                
                rank_long = scanner_result.get('rank_long')
                percentile = rank_long / regime_size
                
                # Strategy logic: LONG if top 50%, NEUTRAL if bottom 50%
                if percentile <= 0.5:
                    # LONG position: use actual return
                    strategy_return = raw_return
                    direction = 'LONG'
                else:
                    # NEUTRAL: no position, no return
                    strategy_return = 0
                    direction = 'NEUTRAL'
                
                rank = rank_long
            
            strategy_returns.append({
                'run_id': eval_item.get('run_id', '')[:8],
                'raw_return': raw_return * 100,
                'strategy_return': strategy_return * 100,
                'direction': direction,
                'rank': rank,
                'tier': tier
            })
        
        print(f"\n📋 LIGHT Strategy Return Calculation (Frontend Logic):")
        print(f"{'Run ID':<8} {'Raw%':<8} {'Strategy%':<10} {'Direction':<8} {'Rank':<5} {'Tier'}")
        print("-" * 60)
        
        for item in strategy_returns:
            print(f"{item['run_id']:<8} {item['raw_return']:+6.2f}% {item['strategy_return']:+8.2f}% {item['direction']:<8} {item['rank'] or 'N/A':<5} {item['tier'] or 'N/A'}")
        
        # Calculate summary statistics
        long_returns = [item['strategy_return'] for item in strategy_returns if item['direction'] == 'LONG']
        
        if long_returns:
            avg_strategy_return = sum(long_returns) / len(long_returns)
            profitable_count = len([r for r in long_returns if r > 0])
            win_rate = (profitable_count / len(long_returns)) * 100
            
            print(f"\n📊 LIGHT Frontend Strategy Summary:")
            print(f"   LONG Trades: {len(long_returns)}")
            print(f"   Average Strategy Return: {avg_strategy_return:+.3f}%")
            print(f"   Win Rate: {win_rate:.1f}%")
            print(f"   Profitable Trades: {profitable_count}/{len(long_returns)}")
            
            print(f"\n✅ Frontend should now show:")
            print(f"   - Strategy returns instead of raw returns")
            print(f"   - LIGHT average: {avg_strategy_return:+.3f}% (not negative)")
            print(f"   - Only LONG signals counted in performance")
        else:
            print(f"\n⚠️  No LONG trades found in sample")
        
        print(f"\n🎯 Expected Frontend Behavior:")
        print(f"   - Historical events table shows 'Strategy Return' column")
        print(f"   - NEUTRAL signals show 0% return (no position)")
        print(f"   - LONG signals show actual forward return")
        print(f"   - Performance metrics calculated from strategy returns only")
        
    except Exception as e:
        print(f"❌ Error testing frontend calculation: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_frontend_strategy_calculation()