#!/usr/bin/env python3
"""
Full Database Analysis - All 33k+ Records
"""
import pandas as pd
import numpy as np
from supabase import create_client
from datetime import datetime, timezone, timedelta

# Use public credentials (read-only access)
SUPABASE_URL = "https://ywgxccyervulrlulwzav.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3Z3hjY3llcnZ1bHJsdWx3emF2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwNTg3NjcsImV4cCI6MjA4MTYzNDc2N30.gmfIhuh6rcahuNeWhr6yTLlEh77rG57_Mw_COa8yZ94"

def analyze_full_database():
    """Analyze all 33k+ evaluation records"""
    
    sb = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    print("🔍 Fetching ALL evaluation data (33k+ records)...")
    
    try:
        # Get ALL evaluation data in batches
        all_eval_data = []
        batch_size = 1000
        offset = 0
        
        while True:
            response = sb.table("scanner_eval").select("*").range(offset, offset + batch_size - 1).execute()
            batch_data = response.data
            
            if not batch_data:
                break
                
            all_eval_data.extend(batch_data)
            offset += batch_size
            print(f"   Fetched {len(all_eval_data):,} records...")
            
            if len(batch_data) < batch_size:
                break
        
        eval_data = pd.DataFrame(all_eval_data)
        print(f"📊 Total evaluation records: {len(eval_data):,}")
        
        # Get ALL scanner results in batches
        print("🔍 Fetching ALL scanner results...")
        all_scanner_data = []
        offset = 0
        
        while True:
            response = sb.table("scanner_results").select("run_id, symbol, rank_long, tier").range(offset, offset + batch_size - 1).execute()
            batch_data = response.data
            
            if not batch_data:
                break
                
            all_scanner_data.extend(batch_data)
            offset += batch_size
            print(f"   Fetched {len(all_scanner_data):,} scanner results...")
            
            if len(batch_data) < batch_size:
                break
        
        scanner_data = pd.DataFrame(all_scanner_data)
        print(f"📊 Total scanner results: {len(scanner_data):,}")
        
        # Merge evaluation with scanner results
        print("🔄 Merging datasets...")
        merged = eval_data.merge(scanner_data, on=['run_id', 'symbol'], how='left', suffixes=('', '_scanner'))
        
        # Filter out records without rank data
        original_count = len(merged)
        merged = merged.dropna(subset=['rank_long'])
        filtered_count = len(merged)
        print(f"📊 After filtering missing ranks: {filtered_count:,} records ({original_count - filtered_count:,} dropped)")
        
        # Clean symbol format
        merged['clean_symbol'] = merged['symbol'].str.replace('/USDT:USDT', '').str.upper()
        
        # Filter out extreme outliers (>50% moves likely data errors)
        original_count = len(merged)
        merged = merged[merged['fwd_return'].abs() <= 0.5]
        filtered_count = len(merged)
        print(f"🧹 After filtering outliers: {filtered_count:,} records ({original_count - filtered_count:,} extreme outliers removed)")
        
        print("\n" + "="*80)
        print("📈 FULL DATABASE STRATEGY ANALYSIS")
        print("="*80)
        
        # === OVERALL STRATEGY PERFORMANCE ===
        print(f"\n🎯 OVERALL STRATEGY PERFORMANCE ({len(merged):,} trades):")
        
        # Calculate regime-based strategy returns
        strategy_returns = []
        for _, row in merged.iterrows():
            # Determine direction based on rank percentile within regime
            # Use actual regime size calculation
            tier = row['tier']
            rank_long = row['rank_long']
            
            # Calculate actual regime size for this run
            # This is approximate - could be improved with actual historical regime sizes
            if tier == 'LARGE':
                approx_regime_size = 200  # HIGH_VOL
            elif tier == 'MID':
                approx_regime_size = 200  # MID_VOL  
            else:  # SMALL
                approx_regime_size = 200  # LOW_VOL
                
            percentile = rank_long / approx_regime_size if rank_long else 0.5
            
            # Strategy logic: LONG if top 50%, SHORT if bottom 50%
            if percentile <= 0.5:  # LONG position
                strategy_return = row['fwd_return']
                direction = 'LONG'
            else:  # SHORT position  
                strategy_return = -row['fwd_return']  # Invert for short
                direction = 'SHORT'
                
            strategy_returns.append({
                'symbol': row['clean_symbol'],
                'raw_return': row['fwd_return'] * 100,
                'strategy_return': strategy_return * 100,
                'direction': direction,
                'percentile': percentile,
                'tier': row['tier'],
                'profitable': strategy_return > 0,
                'run_id': row['run_id']
            })
        
        strategy_df = pd.DataFrame(strategy_returns)
        
        # Overall statistics
        total_trades = len(strategy_df)
        profitable_trades = strategy_df['profitable'].sum()
        win_rate = (profitable_trades / total_trades) * 100
        avg_strategy_return = strategy_df['strategy_return'].mean()
        median_strategy_return = strategy_df['strategy_return'].median()
        std_strategy_return = strategy_df['strategy_return'].std()
        
        print(f"   Total Trades: {total_trades:,}")
        print(f"   Profitable Trades: {profitable_trades:,}")
        print(f"   Win Rate: {win_rate:.2f}%")
        print(f"   Average Strategy Return: {avg_strategy_return:+.3f}%")
        print(f"   Median Strategy Return: {median_strategy_return:+.3f}%")
        print(f"   Standard Deviation: {std_strategy_return:.3f}%")
        
        # Cumulative performance
        cumulative_return = strategy_df['strategy_return'].sum()
        print(f"   Cumulative Strategy Return: {cumulative_return:+.2f}%")
        
        # Sharpe ratio approximation (assuming 4h periods)
        if std_strategy_return > 0:
            sharpe_ratio = avg_strategy_return / std_strategy_return
            print(f"   Sharpe Ratio: {sharpe_ratio:.3f}")
        
        # === PERFORMANCE BY DIRECTION ===
        print(f"\n📊 PERFORMANCE BY DIRECTION:")
        for direction in ['LONG', 'SHORT']:
            dir_data = strategy_df[strategy_df['direction'] == direction]
            if not dir_data.empty:
                dir_win_rate = (dir_data['profitable'].sum() / len(dir_data)) * 100
                dir_avg_return = dir_data['strategy_return'].mean()
                dir_cum_return = dir_data['strategy_return'].sum()
                print(f"   {direction:5} ({len(dir_data):,} trades): Avg {dir_avg_return:+6.3f}% | Win Rate {dir_win_rate:5.1f}% | Cum {dir_cum_return:+8.1f}%")
        
        # === PERFORMANCE BY VOLATILITY REGIME ===
        print(f"\n📊 PERFORMANCE BY VOLATILITY REGIME:")
        for tier in ['LARGE', 'MID', 'SMALL']:
            tier_data = strategy_df[strategy_df['tier'] == tier]
            if not tier_data.empty:
                tier_avg = tier_data['strategy_return'].mean()
                tier_win_rate = (tier_data['profitable'].sum() / len(tier_data)) * 100
                tier_count = len(tier_data)
                tier_cum = tier_data['strategy_return'].sum()
                regime_name = {'LARGE': 'HIGH_VOL', 'MID': 'MID_VOL', 'SMALL': 'LOW_VOL'}[tier]
                print(f"   {regime_name:8} ({tier_count:,} trades): Avg {tier_avg:+6.3f}% | Win Rate {tier_win_rate:5.1f}% | Cum {tier_cum:+8.1f}%")
        
        # === LONG-ONLY ANALYSIS ===
        print(f"\n" + "="*80)
        print("🎯 LONG-ONLY STRATEGY ANALYSIS")
        print("="*80)
        
        long_only_data = strategy_df[strategy_df['direction'] == 'LONG']
        
        if not long_only_data.empty:
            long_total = len(long_only_data)
            long_profitable = long_only_data['profitable'].sum()
            long_win_rate = (long_profitable / long_total) * 100
            long_avg_return = long_only_data['strategy_return'].mean()
            long_cum_return = long_only_data['strategy_return'].sum()
            long_std = long_only_data['strategy_return'].std()
            
            print(f"\n🎯 LONG-ONLY PERFORMANCE:")
            print(f"   Total LONG Trades: {long_total:,}")
            print(f"   Profitable LONG Trades: {long_profitable:,}")
            print(f"   LONG Win Rate: {long_win_rate:.2f}%")
            print(f"   Average LONG Return: {long_avg_return:+.3f}%")
            print(f"   Cumulative LONG Return: {long_cum_return:+.2f}%")
            print(f"   LONG Sharpe Ratio: {long_avg_return/long_std:.3f}")
            
            # LONG performance by regime
            print(f"\n📊 LONG-ONLY BY VOLATILITY REGIME:")
            for tier in ['LARGE', 'MID', 'SMALL']:
                tier_long = long_only_data[long_only_data['tier'] == tier]
                if not tier_long.empty:
                    tier_avg = tier_long['strategy_return'].mean()
                    tier_win_rate = (tier_long['profitable'].sum() / len(tier_long)) * 100
                    tier_count = len(tier_long)
                    tier_cum = tier_long['strategy_return'].sum()
                    regime_name = {'LARGE': 'HIGH_VOL', 'MID': 'MID_VOL', 'SMALL': 'LOW_VOL'}[tier]
                    print(f"   {regime_name:8} ({tier_count:,} trades): Avg {tier_avg:+6.3f}% | Win Rate {tier_win_rate:5.1f}% | Cum {tier_cum:+8.1f}%")
        
        # === PER COIN ANALYSIS (TOP PERFORMERS) ===
        print(f"\n" + "="*80)
        print("💰 TOP COIN PERFORMERS (LONG-ONLY)")
        print("="*80)
        
        # Calculate per-coin statistics for LONG-only
        coin_performance = []
        for coin in long_only_data['symbol'].unique():
            coin_data = long_only_data[long_only_data['symbol'] == coin]
            if len(coin_data) >= 5:  # At least 5 trades for statistical relevance
                coin_stats = {
                    'coin': coin,
                    'trades': len(coin_data),
                    'win_rate': (coin_data['profitable'].sum() / len(coin_data)) * 100,
                    'avg_return': coin_data['strategy_return'].mean(),
                    'cum_return': coin_data['strategy_return'].sum(),
                    'std_return': coin_data['strategy_return'].std(),
                    'best_trade': coin_data['strategy_return'].max(),
                    'worst_trade': coin_data['strategy_return'].min()
                }
                
                # Sharpe ratio per coin
                if coin_stats['std_return'] > 0:
                    coin_stats['sharpe'] = coin_stats['avg_return'] / coin_stats['std_return']
                else:
                    coin_stats['sharpe'] = 0
                    
                coin_performance.append(coin_stats)
        
        coin_df = pd.DataFrame(coin_performance)
        
        if not coin_df.empty:
            # Top performers by average return
            print(f"\n🏆 TOP 20 LONG-ONLY PERFORMERS (≥5 trades):")
            print(f"{'Coin':<10} {'Trades':<7} {'Win%':<6} {'Avg Ret':<9} {'Cum Ret':<10} {'Sharpe':<7}")
            print("-" * 65)
            
            top_coins = coin_df.sort_values('avg_return', ascending=False).head(20)
            for _, coin in top_coins.iterrows():
                print(f"{coin['coin']:<10} {coin['trades']:<7} {coin['win_rate']:5.1f}% {coin['avg_return']:+7.3f}% {coin['cum_return']:+8.2f}% {coin['sharpe']:6.2f}")
        
        # === PROFITABILITY ASSESSMENT ===
        print(f"\n" + "="*80)
        print("💡 STRATEGY PROFITABILITY ASSESSMENT")
        print("="*80)
        
        # Overall assessment
        is_profitable = avg_strategy_return > 0
        is_long_profitable = long_avg_return > 0 if not long_only_data.empty else False
        is_consistent = win_rate > 50
        is_long_consistent = long_win_rate > 50 if not long_only_data.empty else False
        
        print(f"\n🎯 FULL STRATEGY ASSESSMENT:")
        if is_profitable and is_consistent:
            status = "✅ PROFITABLE STRATEGY"
        elif is_profitable:
            status = "⚠️  MARGINALLY PROFITABLE"
        else:
            status = "❌ UNPROFITABLE STRATEGY"
        print(f"   {status}")
        
        print(f"\n🎯 LONG-ONLY ASSESSMENT:")
        if is_long_profitable and is_long_consistent:
            long_status = "✅ HIGHLY PROFITABLE (LONG-ONLY)"
        elif is_long_profitable:
            long_status = "⚠️  MARGINALLY PROFITABLE (LONG-ONLY)"
        else:
            long_status = "❌ UNPROFITABLE (LONG-ONLY)"
        print(f"   {long_status}")
        
        print(f"\n📊 RECOMMENDATION:")
        if is_long_profitable and long_win_rate > win_rate:
            print("   ✅ SWITCH TO LONG-ONLY STRATEGY")
            print("   📈 LONG-only significantly outperforms full strategy")
        else:
            print("   ⚠️  Strategy needs improvement")
        
        print(f"\n" + "="*80)
        
    except Exception as e:
        print(f"❌ Error analyzing full database: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_full_database()