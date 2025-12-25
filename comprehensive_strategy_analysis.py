#!/usr/bin/env python3
"""
Comprehensive Strategy Analysis - Overall and Per Coin Performance
"""
import pandas as pd
import numpy as np
from supabase import create_client
from datetime import datetime, timezone, timedelta

# Use public credentials (read-only access)
SUPABASE_URL = "https://ywgxccyervulrlulwzav.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3Z3hjY3llcnZ1bHJsdWx3emF2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwNTg3NjcsImV4cCI6MjA4MTYzNDc2N30.gmfIhuh6rcahuNeWhr6yTLlEh77rG57_Mw_COa8yZ94"

def analyze_strategy_performance():
    """Comprehensive analysis of strategy performance"""
    
    sb = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    print("🔍 Fetching comprehensive strategy data...")
    
    try:
        # Get all evaluation data
        response = sb.table("scanner_eval").select("*").execute()
        eval_data = pd.DataFrame(response.data)
        
        if eval_data.empty:
            print("❌ No evaluation data found")
            return
            
        print(f"📊 Found {len(eval_data)} evaluation records")
        
        # Get scanner results for context
        run_ids = eval_data['run_id'].unique()
        scanner_response = sb.table("scanner_results").select("run_id, symbol, rank_long, tier").in_("run_id", run_ids.tolist()).execute()
        scanner_data = pd.DataFrame(scanner_response.data)
        
        # Merge evaluation with scanner results
        merged = eval_data.merge(scanner_data, on=['run_id', 'symbol'], how='left', suffixes=('', '_scanner'))
        
        # Debug: Check merge results
        print(f"📊 Merged data: {len(merged)} records")
        print(f"📊 Columns: {list(merged.columns)}")
        
        # Check for missing rank_long data
        missing_rank = merged['rank_long'].isna().sum()
        if missing_rank > 0:
            print(f"⚠️  Warning: {missing_rank} records missing rank_long data")
            merged = merged.dropna(subset=['rank_long'])
            print(f"📊 After filtering: {len(merged)} records")
        
        # Clean symbol format
        merged['clean_symbol'] = merged['symbol'].str.replace('/USDT:USDT', '').str.upper()
        
        # Filter out extreme outliers (>50% moves likely data errors)
        original_count = len(merged)
        merged = merged[merged['fwd_return'].abs() <= 0.5]
        filtered_count = len(merged)
        print(f"🧹 Filtered {original_count - filtered_count} extreme outliers (>50%)")
        
        print("\n" + "="*80)
        print("📈 COMPREHENSIVE STRATEGY ANALYSIS")
        print("="*80)
        
        # === OVERALL STRATEGY PERFORMANCE ===
        print(f"\n🎯 OVERALL STRATEGY PERFORMANCE:")
        
        # Calculate regime-based strategy returns
        strategy_returns = []
        for _, row in merged.iterrows():
            # Determine direction based on rank percentile within regime
            # Approximate regime size (could be improved with actual data)
            approx_regime_size = 180  # Rough estimate per tier
            rank_long = row.get('rank_long', None)
            
            if rank_long is None or pd.isna(rank_long):
                continue  # Skip records without rank data
                
            percentile = rank_long / approx_regime_size
            
            # Strategy logic: LONG if top 50%, SHORT if bottom 50%
            if percentile <= 0.5:  # LONG position
                strategy_return = row['fwd_return']
            else:  # SHORT position  
                strategy_return = -row['fwd_return']  # Invert for short
                
            strategy_returns.append({
                'symbol': row['clean_symbol'],
                'raw_return': row['fwd_return'] * 100,
                'strategy_return': strategy_return * 100,
                'direction': 'LONG' if percentile <= 0.5 else 'SHORT',
                'percentile': percentile,
                'tier': row['tier'],
                'profitable': strategy_return > 0
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
        
        # Sharpe ratio approximation
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
                print(f"   {direction:5} ({len(dir_data):4} trades): Avg {dir_avg_return:+6.3f}% | Win Rate {dir_win_rate:5.1f}%")
        
        # === PERFORMANCE BY VOLATILITY REGIME ===
        print(f"\n📊 PERFORMANCE BY VOLATILITY REGIME:")
        for tier in ['LARGE', 'MID', 'SMALL']:
            tier_data = strategy_df[strategy_df['tier'] == tier]
            if not tier_data.empty:
                tier_avg = tier_data['strategy_return'].mean()
                tier_win_rate = (tier_data['profitable'].sum() / len(tier_data)) * 100
                tier_count = len(tier_data)
                regime_name = {'LARGE': 'HIGH_VOL', 'MID': 'MID_VOL', 'SMALL': 'LOW_VOL'}[tier]
                print(f"   {regime_name:8} ({tier_count:4} trades): Avg {tier_avg:+6.3f}% | Win Rate {tier_win_rate:5.1f}%")
        
        # === PER COIN ANALYSIS ===
        print(f"\n" + "="*80)
        print("💰 PER COIN PERFORMANCE ANALYSIS")
        print("="*80)
        
        # Calculate per-coin statistics
        coin_performance = []
        for coin in strategy_df['symbol'].unique():
            coin_data = strategy_df[strategy_df['symbol'] == coin]
            if len(coin_data) >= 2:  # At least 2 trades for statistical relevance
                coin_stats = {
                    'coin': coin,
                    'trades': len(coin_data),
                    'win_rate': (coin_data['profitable'].sum() / len(coin_data)) * 100,
                    'avg_return': coin_data['strategy_return'].mean(),
                    'cum_return': coin_data['strategy_return'].sum(),
                    'std_return': coin_data['strategy_return'].std(),
                    'best_trade': coin_data['strategy_return'].max(),
                    'worst_trade': coin_data['strategy_return'].min(),
                    'long_trades': len(coin_data[coin_data['direction'] == 'LONG']),
                    'short_trades': len(coin_data[coin_data['direction'] == 'SHORT'])
                }
                
                # Sharpe ratio per coin
                if coin_stats['std_return'] > 0:
                    coin_stats['sharpe'] = coin_stats['avg_return'] / coin_stats['std_return']
                else:
                    coin_stats['sharpe'] = 0
                    
                coin_performance.append(coin_stats)
        
        coin_df = pd.DataFrame(coin_performance)
        
        if coin_df.empty:
            print("❌ No coins with sufficient trade history")
            return
        
        # Top performers by average return
        print(f"\n🏆 TOP 20 PERFORMING COINS (by Average Return):")
        print(f"{'Coin':<10} {'Trades':<7} {'Win%':<6} {'Avg Ret':<9} {'Cum Ret':<9} {'Sharpe':<7} {'L/S':<6}")
        print("-" * 70)
        
        top_coins = coin_df.sort_values('avg_return', ascending=False).head(20)
        for _, coin in top_coins.iterrows():
            ls_ratio = f"{coin['long_trades']}/{coin['short_trades']}"
            print(f"{coin['coin']:<10} {coin['trades']:<7} {coin['win_rate']:5.1f}% {coin['avg_return']:+7.3f}% {coin['cum_return']:+7.2f}% {coin['sharpe']:6.2f} {ls_ratio:<6}")
        
        # Bottom performers
        print(f"\n💔 BOTTOM 20 PERFORMING COINS (by Average Return):")
        print(f"{'Coin':<10} {'Trades':<7} {'Win%':<6} {'Avg Ret':<9} {'Cum Ret':<9} {'Sharpe':<7} {'L/S':<6}")
        print("-" * 70)
        
        bottom_coins = coin_df.sort_values('avg_return', ascending=True).head(20)
        for _, coin in bottom_coins.iterrows():
            ls_ratio = f"{coin['long_trades']}/{coin['short_trades']}"
            print(f"{coin['coin']:<10} {coin['trades']:<7} {coin['win_rate']:5.1f}% {coin['avg_return']:+7.3f}% {coin['cum_return']:+7.2f}% {coin['sharpe']:6.2f} {ls_ratio:<6}")
        
        # Most active coins
        print(f"\n📊 MOST ACTIVE COINS (by Trade Count):")
        print(f"{'Coin':<10} {'Trades':<7} {'Win%':<6} {'Avg Ret':<9} {'Cum Ret':<9} {'Sharpe':<7} {'L/S':<6}")
        print("-" * 70)
        
        active_coins = coin_df.sort_values('trades', ascending=False).head(20)
        for _, coin in active_coins.iterrows():
            ls_ratio = f"{coin['long_trades']}/{coin['short_trades']}"
            print(f"{coin['coin']:<10} {coin['trades']:<7} {coin['win_rate']:5.1f}% {coin['avg_return']:+7.3f}% {coin['cum_return']:+7.2f}% {coin['sharpe']:6.2f} {ls_ratio:<6}")
        
        # === PROFITABILITY ASSESSMENT ===
        print(f"\n" + "="*80)
        print("💡 STRATEGY PROFITABILITY ASSESSMENT")
        print("="*80)
        
        profitable_coins = len(coin_df[coin_df['avg_return'] > 0])
        total_coins = len(coin_df)
        profitable_coin_pct = (profitable_coins / total_coins) * 100
        
        print(f"\n📊 COIN-LEVEL STATISTICS:")
        print(f"   Total Coins Analyzed: {total_coins}")
        print(f"   Profitable Coins: {profitable_coins} ({profitable_coin_pct:.1f}%)")
        print(f"   Unprofitable Coins: {total_coins - profitable_coins} ({100 - profitable_coin_pct:.1f}%)")
        
        # Overall assessment
        is_profitable = avg_strategy_return > 0
        is_consistent = win_rate > 50
        has_good_coin_ratio = profitable_coin_pct > 50
        
        print(f"\n🎯 OVERALL ASSESSMENT:")
        if is_profitable and is_consistent and has_good_coin_ratio:
            status = "✅ HIGHLY PROFITABLE STRATEGY"
            assessment = "Strong positive returns, good win rate, and majority of coins profitable."
        elif is_profitable and is_consistent:
            status = "✅ PROFITABLE STRATEGY"
            assessment = "Positive returns with good win rate, but mixed coin performance."
        elif is_profitable:
            status = "⚠️  MARGINALLY PROFITABLE"
            assessment = "Positive returns but inconsistent performance."
        else:
            status = "❌ UNPROFITABLE STRATEGY"
            assessment = "Negative expected returns - strategy needs major improvements."
        
        print(f"   {status}")
        print(f"   {assessment}")
        
        # Risk metrics
        print(f"\n📊 RISK METRICS:")
        print(f"   Best Single Trade: {strategy_df['strategy_return'].max():+.2f}%")
        print(f"   Worst Single Trade: {strategy_df['strategy_return'].min():+.2f}%")
        print(f"   Volatility (Std Dev): {std_strategy_return:.2f}%")
        
        # Calculate maximum drawdown
        cumulative_returns = strategy_df['strategy_return'].cumsum()
        running_max = cumulative_returns.expanding().max()
        drawdowns = cumulative_returns - running_max
        max_drawdown = drawdowns.min()
        
        print(f"   Maximum Drawdown: {max_drawdown:.2f}%")
        
        print(f"\n" + "="*80)
        
    except Exception as e:
        print(f"❌ Error analyzing strategy performance: {e}")

if __name__ == "__main__":
    analyze_strategy_performance()