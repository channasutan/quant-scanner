#!/usr/bin/env python3
"""
Detailed Per-Coin Analysis with Overall Summary
"""
import pandas as pd
import numpy as np
from supabase import create_client
import sys

# Use public credentials (read-only access)
SUPABASE_URL = "https://ywgxccyervulrlulwzav.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3Z3hjY3llcnZ1bHJsdWx3emF2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwNTg3NjcsImV4cCI6MjA4MTYzNDc2N30.gmfIhuh6rcahuNeWhr6yTLlEh77rG57_Mw_COa8yZ94"

def analyze_detailed_coin_performance():
    """Detailed analysis per coin with LONG-ONLY strategy"""
    
    sb = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    print("🔍 Fetching detailed coin performance data...")
    
    try:
        # Get evaluation data in batches (first 5000 for faster analysis)
        eval_response = sb.table("scanner_eval").select("*").limit(5000).execute()
        eval_data = pd.DataFrame(eval_response.data)
        
        if eval_data.empty:
            print("❌ No evaluation data found")
            return
            
        print(f"📊 Analyzing {len(eval_data)} evaluation records")
        
        # Get scanner results for these runs
        run_ids = eval_data['run_id'].unique()
        scanner_response = sb.table("scanner_results").select("*").in_("run_id", run_ids.tolist()).execute()
        scanner_data = pd.DataFrame(scanner_response.data)
        
        print(f"📊 Scanner data columns: {list(scanner_data.columns) if not scanner_data.empty else 'No data'}")
        
        # Merge evaluation with scanner results
        merged = eval_data.merge(scanner_data, on=['run_id', 'symbol'], how='inner', suffixes=('_eval', '_scanner'))
        
        # Check if we have rank_long or need to use a different column
        if 'rank_long_scanner' in merged.columns:
            merged = merged.dropna(subset=['rank_long_scanner'])
            rank_column = 'rank_long_scanner'
        elif 'rank_long' in merged.columns:
            merged = merged.dropna(subset=['rank_long'])
            rank_column = 'rank_long'
        else:
            print(f"❌ Available columns: {list(merged.columns)}")
            print("❌ No rank column found")
            return
        
        # Clean symbol format
        merged['clean_symbol'] = merged['symbol'].str.replace('/USDT:USDT', '').str.upper()
        
        print(f"📊 Merged data: {len(merged)} records")
        
        # Calculate LONG-ONLY strategy performance per coin
        coin_performance = {}
        
        for _, row in merged.iterrows():
            symbol = row['clean_symbol']
            rank_value = row[rank_column]
            fwd_return = row['fwd_return']
            tier = row['tier']
            
            # Approximate regime size
            approx_regime_size = 200
            percentile = rank_value / approx_regime_size
            
            # LONG-ONLY: Only count top 50% as signals
            if percentile <= 0.5:  # LONG signal
                if symbol not in coin_performance:
                    coin_performance[symbol] = {
                        'signals': 0,
                        'returns': [],
                        'profitable': 0,
                        'tier_distribution': {}
                    }
                
                coin_performance[symbol]['signals'] += 1
                coin_performance[symbol]['returns'].append(fwd_return * 100)
                
                if fwd_return > 0:
                    coin_performance[symbol]['profitable'] += 1
                
                # Track tier distribution
                if tier not in coin_performance[symbol]['tier_distribution']:
                    coin_performance[symbol]['tier_distribution'][tier] = 0
                coin_performance[symbol]['tier_distribution'][tier] += 1
        
        # Calculate statistics for each coin
        coin_stats = []
        for symbol, data in coin_performance.items():
            if data['signals'] >= 2:  # At least 2 signals for analysis
                returns = np.array(data['returns'])
                
                stats = {
                    'symbol': symbol,
                    'signals': data['signals'],
                    'profitable': data['profitable'],
                    'win_rate': (data['profitable'] / data['signals']) * 100,
                    'avg_return': np.mean(returns),
                    'median_return': np.median(returns),
                    'std_return': np.std(returns),
                    'total_return': np.sum(returns),
                    'compound_return': (np.prod(1 + returns/100) - 1) * 100,
                    'best_return': np.max(returns),
                    'worst_return': np.min(returns),
                    'positive_returns': len(returns[returns > 0]),
                    'negative_returns': len(returns[returns <= 0]),
                    'main_tier': max(data['tier_distribution'], key=data['tier_distribution'].get)
                }
                
                # Sharpe ratio approximation
                if stats['std_return'] > 0:
                    stats['sharpe'] = stats['avg_return'] / stats['std_return']
                else:
                    stats['sharpe'] = 0
                
                coin_stats.append(stats)
        
        coin_df = pd.DataFrame(coin_stats)
        
        if coin_df.empty:
            print("❌ No coins with sufficient data")
            return
        
        print(f"\n" + "="*100)
        print("📈 DETAILED COIN PERFORMANCE ANALYSIS (LONG-ONLY STRATEGY)")
        print("="*100)
        
        # Overall summary first
        total_signals = coin_df['signals'].sum()
        total_profitable = coin_df['profitable'].sum()
        overall_win_rate = (total_profitable / total_signals) * 100
        
        # Calculate overall returns using compound method
        all_returns = []
        for _, coin in coin_df.iterrows():
            symbol = coin['symbol']
            returns = coin_performance[symbol]['returns']
            all_returns.extend(returns)
        
        overall_avg_return = np.mean(all_returns)
        overall_compound_return = (np.prod(1 + np.array(all_returns)/100) - 1) * 100
        overall_total_return = np.sum(all_returns)
        
        profitable_coins = len(coin_df[coin_df['avg_return'] > 0])
        total_coins = len(coin_df)
        
        print(f"\n🎯 OVERALL SUMMARY:")
        print(f"   Total Coins Analyzed: {total_coins}")
        print(f"   Total LONG Signals: {total_signals:,}")
        print(f"   Profitable Signals: {total_profitable:,}")
        print(f"   Overall Win Rate: {overall_win_rate:.2f}%")
        print(f"   Average Return per Signal: {overall_avg_return:+.3f}%")
        print(f"   Total Simple Return: {overall_total_return:+.2f}%")
        print(f"   Total Compound Return: {overall_compound_return:+.2f}%")
        print(f"   Profitable Coins: {profitable_coins}/{total_coins} ({profitable_coins/total_coins*100:.1f}%)")
        
        # Top performers
        print(f"\n🏆 TOP 20 PERFORMING COINS:")
        print(f"{'Symbol':<8} {'Signals':<8} {'Win%':<6} {'Avg Ret':<9} {'Compound':<10} {'Total':<9} {'Best':<8} {'Worst':<8} {'Tier':<6}")
        print("-" * 95)
        
        top_performers = coin_df.sort_values('avg_return', ascending=False).head(20)
        for _, coin in top_performers.iterrows():
            tier_name = {'LARGE': 'HIGH', 'MID': 'MID', 'SMALL': 'LOW'}[coin['main_tier']]
            print(f"{coin['symbol']:<8} {coin['signals']:<8} {coin['win_rate']:5.1f}% {coin['avg_return']:+7.3f}% {coin['compound_return']:+8.2f}% {coin['total_return']:+7.2f}% {coin['best_return']:+6.2f}% {coin['worst_return']:+6.2f}% {tier_name:<6}")
        
        # Bottom performers
        print(f"\n💔 BOTTOM 20 PERFORMING COINS:")
        print(f"{'Symbol':<8} {'Signals':<8} {'Win%':<6} {'Avg Ret':<9} {'Compound':<10} {'Total':<9} {'Best':<8} {'Worst':<8} {'Tier':<6}")
        print("-" * 95)
        
        bottom_performers = coin_df.sort_values('avg_return', ascending=True).head(20)
        for _, coin in bottom_performers.iterrows():
            tier_name = {'LARGE': 'HIGH', 'MID': 'MID', 'SMALL': 'LOW'}[coin['main_tier']]
            print(f"{coin['symbol']:<8} {coin['signals']:<8} {coin['win_rate']:5.1f}% {coin['avg_return']:+7.3f}% {coin['compound_return']:+8.2f}% {coin['total_return']:+7.2f}% {coin['best_return']:+6.2f}% {coin['worst_return']:+6.2f}% {tier_name:<6}")
        
        # Most active coins
        print(f"\n📊 MOST ACTIVE COINS (by Signal Count):")
        print(f"{'Symbol':<8} {'Signals':<8} {'Win%':<6} {'Avg Ret':<9} {'Compound':<10} {'Total':<9} {'Best':<8} {'Worst':<8} {'Tier':<6}")
        print("-" * 95)
        
        most_active = coin_df.sort_values('signals', ascending=False).head(20)
        for _, coin in most_active.iterrows():
            tier_name = {'LARGE': 'HIGH', 'MID': 'MID', 'SMALL': 'LOW'}[coin['main_tier']]
            print(f"{coin['symbol']:<8} {coin['signals']:<8} {coin['win_rate']:5.1f}% {coin['avg_return']:+7.3f}% {coin['compound_return']:+8.2f}% {coin['total_return']:+7.2f}% {coin['best_return']:+6.2f}% {coin['worst_return']:+6.2f}% {tier_name:<6}")
        
        # Performance by tier
        print(f"\n📊 PERFORMANCE BY VOLATILITY TIER:")
        for tier in ['LARGE', 'MID', 'SMALL']:
            tier_coins = coin_df[coin_df['main_tier'] == tier]
            if not tier_coins.empty:
                tier_signals = tier_coins['signals'].sum()
                tier_profitable = tier_coins['profitable'].sum()
                tier_win_rate = (tier_profitable / tier_signals) * 100
                tier_avg_return = tier_coins['avg_return'].mean()
                tier_name = {'LARGE': 'HIGH_VOL', 'MID': 'MID_VOL', 'SMALL': 'LOW_VOL'}[tier]
                profitable_tier_coins = len(tier_coins[tier_coins['avg_return'] > 0])
                total_tier_coins = len(tier_coins)
                
                print(f"   {tier_name:8}: {total_tier_coins:3} coins, {tier_signals:4} signals, {tier_win_rate:5.1f}% win rate, {tier_avg_return:+6.3f}% avg, {profitable_tier_coins}/{total_tier_coins} profitable")
        
        # Check specific coins mentioned
        specific_coins = ['SOL', 'BTC', 'ETH', 'LIGHT', 'RAVE']
        print(f"\n🎯 SPECIFIC COIN ANALYSIS:")
        for coin_symbol in specific_coins:
            coin_data = coin_df[coin_df['symbol'] == coin_symbol]
            if not coin_data.empty:
                coin = coin_data.iloc[0]
                tier_name = {'LARGE': 'HIGH_VOL', 'MID': 'MID_VOL', 'SMALL': 'LOW_VOL'}[coin['main_tier']]
                print(f"   {coin_symbol:8}: {coin['signals']:3} signals, {coin['win_rate']:5.1f}% win rate, {coin['avg_return']:+7.3f}% avg, {coin['compound_return']:+8.2f}% compound ({tier_name})")
            else:
                print(f"   {coin_symbol:8}: No LONG signals found")
        
        # Distribution analysis
        print(f"\n📊 RETURN DISTRIBUTION:")
        positive_coins = len(coin_df[coin_df['avg_return'] > 0])
        negative_coins = len(coin_df[coin_df['avg_return'] <= 0])
        
        print(f"   Profitable Coins: {positive_coins} ({positive_coins/total_coins*100:.1f}%)")
        print(f"   Unprofitable Coins: {negative_coins} ({negative_coins/total_coins*100:.1f}%)")
        
        # Win rate distribution
        high_win_rate = len(coin_df[coin_df['win_rate'] >= 60])
        medium_win_rate = len(coin_df[(coin_df['win_rate'] >= 40) & (coin_df['win_rate'] < 60)])
        low_win_rate = len(coin_df[coin_df['win_rate'] < 40])
        
        print(f"   High Win Rate (≥60%): {high_win_rate} coins")
        print(f"   Medium Win Rate (40-60%): {medium_win_rate} coins")
        print(f"   Low Win Rate (<40%): {low_win_rate} coins")
        
        print(f"\n" + "="*100)
        print("💡 ANALYSIS CONCLUSIONS:")
        print("="*100)
        
        if overall_win_rate > 50 and overall_avg_return > 0:
            conclusion = "✅ LONG-ONLY STRATEGY IS PROFITABLE"
        elif overall_avg_return > 0:
            conclusion = "⚠️  MARGINALLY PROFITABLE - Low win rate but positive returns"
        else:
            conclusion = "❌ STRATEGY IS UNPROFITABLE"
        
        print(f"\n{conclusion}")
        print(f"📊 Key Metrics:")
        print(f"   - {positive_coins}/{total_coins} coins are profitable ({positive_coins/total_coins*100:.1f}%)")
        print(f"   - Overall win rate: {overall_win_rate:.1f}%")
        print(f"   - Average return per signal: {overall_avg_return:+.3f}%")
        print(f"   - Compound return: {overall_compound_return:+.2f}%")
        
        if negative_coins > positive_coins:
            print(f"\n⚠️  WARNING: More coins are unprofitable ({negative_coins}) than profitable ({positive_coins})")
            print(f"   Consider filtering out consistently unprofitable coins")
        
        print(f"\n" + "="*100)
        
    except Exception as e:
        print(f"❌ Error analyzing coin performance: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    analyze_detailed_coin_performance()