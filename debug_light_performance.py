#!/usr/bin/env python3
"""
Debug LIGHT Performance Discrepancy
"""
import pandas as pd
from supabase import create_client

# Use public credentials (read-only access)
SUPABASE_URL = "https://ywgxccyervulrlulwzav.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3Z3hjY3llcnZ1bHJsdWx3emF2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwNTg3NjcsImV4cCI6MjA4MTYzNDc2N30.gmfIhuh6rcahuNeWhr6yTLlEh77rG57_Mw_COa8yZ94"

def debug_light_performance():
    """Debug LIGHT performance calculation"""
    
    sb = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    print("🔍 Debugging LIGHT Performance...")
    
    try:
        # Get LIGHT evaluation data
        eval_response = sb.table("scanner_eval").select("*").like("symbol", "%LIGHT%").execute()
        eval_data = pd.DataFrame(eval_response.data)
        
        if eval_data.empty:
            print("❌ No LIGHT evaluation data found")
            return
            
        print(f"📊 Found {len(eval_data)} LIGHT evaluation records")
        
        # Get all scanner results for LIGHT runs
        run_ids = eval_data['run_id'].unique()
        scanner_response = sb.table("scanner_results").select("*").like("symbol", "%LIGHT%").in_("run_id", run_ids.tolist()).execute()
        scanner_data = pd.DataFrame(scanner_response.data)
        
        print(f"📊 Found {len(scanner_data)} LIGHT scanner results")
        
        # Debug the merge issue
        print(f"📊 Eval data columns: {eval_data.columns.tolist()}")
        print(f"📊 Scanner data columns: {scanner_data.columns.tolist()}")
        
        # Check for overlapping run_ids
        eval_runs = set(eval_data['run_id'].unique())
        scanner_runs = set(scanner_data['run_id'].unique())
        common_runs = eval_runs.intersection(scanner_runs)
        print(f"📊 Common run_ids: {len(common_runs)}")
        
        # Merge evaluation with scanner results
        merged = eval_data.merge(scanner_data, on=['run_id', 'symbol'], how='inner', suffixes=('_eval', '_scanner'))
        
        print(f"📊 Merged data: {len(merged)} records")
        
        if merged.empty:
            print("❌ No merged data found")
            return
        
        print("\n" + "="*80)
        print("🔍 LIGHT PERFORMANCE DEBUG")
        print("="*80)
        
        # Show raw data
        print(f"\n📋 RAW LIGHT DATA (first 10 records):")
        print(f"{'Run ID':<8} {'Tier':<6} {'Rank':<5} {'Raw Return':<12} {'Direction Logic'}")
        print("-" * 70)
        
        strategy_returns = []
        
        for i, (_, row) in enumerate(merged.head(10).iterrows()):
            tier = row['tier']
            rank_long = row.get('rank_long_scanner', row.get('rank_long', None))
            fwd_return = row['fwd_return']
            
            # Calculate regime size (same logic as analysis)
            if tier == 'LARGE':
                approx_regime_size = 200  # HIGH_VOL
            elif tier == 'MID':
                approx_regime_size = 200  # MID_VOL  
            else:  # SMALL
                approx_regime_size = 200  # LOW_VOL
                
            if rank_long is not None:
                percentile = rank_long / approx_regime_size
                
                # Strategy logic: LONG if top 50%, SHORT if bottom 50%
                if percentile <= 0.5:  # LONG position
                    strategy_return = fwd_return
                    direction = 'LONG'
                else:  # SHORT position  
                    strategy_return = -fwd_return  # Invert for short
                    direction = 'SHORT'
            else:
                strategy_return = 0
                direction = 'NO_RANK'
                percentile = 0
                
            strategy_returns.append({
                'raw_return': fwd_return * 100,
                'strategy_return': strategy_return * 100,
                'direction': direction,
                'percentile': percentile,
                'rank_long': rank_long,
                'tier': tier
            })
            
            run_short = str(row['run_id'])[:8]
            direction_info = f"{direction} (rank {rank_long}, p={percentile:.2f})"
            print(f"{run_short:<8} {tier:<6} {rank_long or 'N/A':<5} {fwd_return*100:+10.2f}% {direction_info}")
        
        # Calculate overall statistics
        strategy_df = pd.DataFrame(strategy_returns)
        
        print(f"\n📊 LIGHT STRATEGY STATISTICS:")
        print(f"   Total Records Analyzed: {len(strategy_df)}")
        
        # Overall performance
        avg_strategy = strategy_df['strategy_return'].mean()
        avg_raw = strategy_df['raw_return'].mean()
        
        print(f"   Average Raw Return: {avg_raw:+.3f}%")
        print(f"   Average Strategy Return: {avg_strategy:+.3f}%")
        
        # By direction
        long_data = strategy_df[strategy_df['direction'] == 'LONG']
        short_data = strategy_df[strategy_df['direction'] == 'SHORT']
        
        if not long_data.empty:
            long_avg = long_data['strategy_return'].mean()
            long_count = len(long_data)
            long_win_rate = (long_data['strategy_return'] > 0).mean() * 100
            print(f"   LONG Trades: {long_count} | Avg: {long_avg:+.3f}% | Win Rate: {long_win_rate:.1f}%")
        
        if not short_data.empty:
            short_avg = short_data['strategy_return'].mean()
            short_count = len(short_data)
            short_win_rate = (short_data['strategy_return'] > 0).mean() * 100
            print(f"   SHORT Trades: {short_count} | Avg: {short_avg:+.3f}% | Win Rate: {short_win_rate:.1f}%")
        
        # Now analyze ALL LIGHT data
        print(f"\n📊 ANALYZING ALL {len(merged)} LIGHT RECORDS...")
        
        all_strategy_returns = []
        
        for _, row in merged.iterrows():
            tier = row['tier']
            rank_long = row.get('rank_long_scanner', row.get('rank_long', None))
            fwd_return = row['fwd_return']
            
            # Calculate regime size
            if tier == 'LARGE':
                approx_regime_size = 200
            elif tier == 'MID':
                approx_regime_size = 200  
            else:
                approx_regime_size = 200
                
            if rank_long is not None:
                percentile = rank_long / approx_regime_size
                
                if percentile <= 0.5:  # LONG position
                    strategy_return = fwd_return
                    direction = 'LONG'
                else:  # SHORT position  
                    strategy_return = -fwd_return
                    direction = 'SHORT'
            else:
                strategy_return = 0
                direction = 'NO_RANK'
                
            all_strategy_returns.append({
                'raw_return': fwd_return * 100,
                'strategy_return': strategy_return * 100,
                'direction': direction,
                'tier': tier
            })
        
        all_strategy_df = pd.DataFrame(all_strategy_returns)
        
        print(f"\n📊 COMPLETE LIGHT ANALYSIS:")
        print(f"   Total Trades: {len(all_strategy_df)}")
        
        # Overall
        all_avg_strategy = all_strategy_df['strategy_return'].mean()
        all_avg_raw = all_strategy_df['raw_return'].mean()
        
        print(f"   Average Raw Return: {all_avg_raw:+.3f}%")
        print(f"   Average Strategy Return: {all_avg_strategy:+.3f}%")
        
        # LONG-only analysis
        all_long_data = all_strategy_df[all_strategy_df['direction'] == 'LONG']
        
        if not all_long_data.empty:
            all_long_avg = all_long_data['strategy_return'].mean()
            all_long_count = len(all_long_data)
            all_long_win_rate = (all_long_data['strategy_return'] > 0).mean() * 100
            all_long_cum = all_long_data['strategy_return'].sum()
            
            print(f"\n🎯 LIGHT LONG-ONLY PERFORMANCE:")
            print(f"   LONG Trades: {all_long_count}")
            print(f"   Average LONG Return: {all_long_avg:+.3f}%")
            print(f"   LONG Win Rate: {all_long_win_rate:.1f}%")
            print(f"   Cumulative LONG Return: {all_long_cum:+.2f}%")
            
            # This should match the analysis result
            print(f"\n✅ VERIFICATION:")
            print(f"   Analysis showed: +2.136% avg, 40.8% win rate, 49 trades")
            print(f"   Debug shows: {all_long_avg:+.3f}% avg, {all_long_win_rate:.1f}% win rate, {all_long_count} trades")
        
        # Check what frontend might be showing
        print(f"\n🖥️  FRONTEND COMPARISON:")
        print(f"   Frontend shows negative returns for LIGHT")
        print(f"   But LONG-only strategy shows: {all_long_avg:+.3f}% average")
        print(f"   Possible issue: Frontend might be showing raw returns instead of strategy returns")
        print(f"   Or frontend might be using different time periods/data")
        
        # Show recent data that frontend might be displaying
        print(f"\n📅 RECENT LIGHT DATA (last 10 records):")
        recent_data = merged.tail(10)
        for _, row in recent_data.iterrows():
            print(f"   {row['run_id'][:8]} | Raw: {row['fwd_return']*100:+6.2f}% | Tier: {row['tier']} | Rank: {row.get('rank_long', 'N/A')}")
        
        print(f"\n" + "="*80)
        
    except Exception as e:
        print(f"❌ Error debugging LIGHT performance: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_light_performance()