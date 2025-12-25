#!/usr/bin/env python3
"""
Analyze individual coin performance from scanner backtest
"""
import pandas as pd
import numpy as np
from supabase import create_client
import sys

# Use public credentials (read-only access)
SUPABASE_URL = "https://ywgxccyervulrlulwzav.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3Z3hjY3llcnZ1bHJsdWx3emF2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwNTg3NjcsImV4cCI6MjA4MTYzNDc2N30.gmfIhuh6rcahuNeWhr6yTLlEh77rG57_Mw_COa8yZ94"

def analyze_coin_performance(coin_symbol=None):
    """Analyze performance for a specific coin or show top/bottom performers"""
    
    sb = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    print("🔍 Fetching coin-specific evaluation data...")
    
    try:
        # Get all evaluation data
        response = sb.table("scanner_eval").select("*").execute()
        eval_data = pd.DataFrame(response.data)
        
        if eval_data.empty:
            print("❌ No evaluation data found")
            return
            
        print(f"📊 Found {len(eval_data)} evaluation records")
        
        # Clean symbol format for analysis
        eval_data['clean_symbol'] = eval_data['symbol'].str.replace('/USDT:USDT', '').str.upper()
        
        # Debug: Show sample data
        print(f"Sample symbols: {eval_data['symbol'].head().tolist()}")
        print(f"Sample clean symbols: {eval_data['clean_symbol'].head().tolist()}")
        print(f"Unique coins count: {eval_data['clean_symbol'].nunique()}")
        
        if coin_symbol:
            # Analyze specific coin
            coin_symbol = coin_symbol.upper()
            coin_data = eval_data[eval_data['clean_symbol'] == coin_symbol]
            
            if coin_data.empty:
                print(f"❌ No data found for {coin_symbol}")
                print(f"Available coins: {sorted(eval_data['clean_symbol'].unique())[:20]}...")
                return
            
            print(f"\n" + "="*60)
            print(f"📈 {coin_symbol} PERFORMANCE ANALYSIS")
            print("="*60)
            
            total_signals = len(coin_data)
            avg_return = coin_data['fwd_return'].mean() * 100
            median_return = coin_data['fwd_return'].median() * 100
            std_return = coin_data['fwd_return'].std() * 100
            
            profitable_signals = (coin_data['fwd_return'] > 0).sum()
            win_rate = profitable_signals / total_signals * 100
            cumulative_return = coin_data['fwd_return'].sum() * 100
            
            print(f"\n🎯 {coin_symbol} PERFORMANCE:")
            print(f"   Total Signals: {total_signals}")
            print(f"   Average Return: {avg_return:+.3f}%")
            print(f"   Median Return: {median_return:+.3f}%")
            print(f"   Std Deviation: {std_return:.3f}%")
            print(f"   Win Rate: {win_rate:.1f}%")
            print(f"   Cumulative Return: {cumulative_return:+.3f}%")
            
            if std_return > 0:
                sharpe = avg_return / std_return
                print(f"   Sharpe Ratio: {sharpe:.3f}")
            
            print(f"   Best Signal: {coin_data['fwd_return'].max() * 100:+.3f}%")
            print(f"   Worst Signal: {coin_data['fwd_return'].min() * 100:+.3f}%")
            
            # Recent performance
            recent_data = coin_data.tail(10)  # Last 10 signals
            if len(recent_data) > 0:
                recent_avg = recent_data['fwd_return'].mean() * 100
                recent_win_rate = (recent_data['fwd_return'] > 0).mean() * 100
                print(f"\n🕒 RECENT PERFORMANCE (Last {len(recent_data)} signals):")
                print(f"   Average Return: {recent_avg:+.3f}%")
                print(f"   Win Rate: {recent_win_rate:.1f}%")
        
        else:
            # Show top and bottom performers
            print(f"Processing {eval_data['clean_symbol'].nunique()} unique coins...")
            
            coin_performance = []
            for coin in eval_data['clean_symbol'].unique():
                coin_data = eval_data[eval_data['clean_symbol'] == coin]
                if len(coin_data) >= 2:  # At least 2 signals
                    stats = {
                        'coin': coin,
                        'signals': len(coin_data),
                        'avg_return': coin_data['fwd_return'].mean(),
                        'cum_return': coin_data['fwd_return'].sum(),
                        'win_rate': (coin_data['fwd_return'] > 0).mean(),
                        'std_return': coin_data['fwd_return'].std(),
                        'best_return': coin_data['fwd_return'].max(),
                        'worst_return': coin_data['fwd_return'].min()
                    }
                    coin_performance.append(stats)
            
            df_performance = pd.DataFrame(coin_performance)
            
            if df_performance.empty:
                print("❌ No coin performance data available")
                return
            
            print(f"\n" + "="*80)
            print("📈 TOP & BOTTOM COIN PERFORMERS")
            print("="*80)
            
            # Top performers by average return
            print(f"\n🏆 TOP 15 PERFORMERS (by Average Return):")
            print(f"{'Coin':<8} {'Signals':<8} {'Avg Return':<12} {'Win Rate':<10} {'Cum Return':<12}")
            print("-" * 70)
            
            top_performers = df_performance.sort_values('avg_return', ascending=False).head(15)
            for _, row in top_performers.iterrows():
                print(f"{row['coin']:<8} {row['signals']:<8} {row['avg_return']*100:+8.3f}% {row['win_rate']*100:6.1f}% {row['cum_return']*100:+10.2f}%")
            
            # Bottom performers
            print(f"\n💔 BOTTOM 15 PERFORMERS (by Average Return):")
            print(f"{'Coin':<8} {'Signals':<8} {'Avg Return':<12} {'Win Rate':<10} {'Cum Return':<12}")
            print("-" * 70)
            
            bottom_performers = df_performance.sort_values('avg_return', ascending=True).head(15)
            for _, row in bottom_performers.iterrows():
                print(f"{row['coin']:<8} {row['signals']:<8} {row['avg_return']*100:+8.3f}% {row['win_rate']*100:6.1f}% {row['cum_return']*100:+10.2f}%")
            
            # Most active coins
            print(f"\n📊 MOST ACTIVE COINS (by Signal Count):")
            print(f"{'Coin':<8} {'Signals':<8} {'Avg Return':<12} {'Win Rate':<10} {'Cum Return':<12}")
            print("-" * 70)
            
            most_active = df_performance.sort_values('signals', ascending=False).head(15)
            for _, row in most_active.iterrows():
                print(f"{row['coin']:<8} {row['signals']:<8} {row['avg_return']*100:+8.3f}% {row['win_rate']*100:6.1f}% {row['cum_return']*100:+10.2f}%")
            
            # Check if RAVE is in the data
            rave_data = df_performance[df_performance['coin'] == 'RAVE']
            if not rave_data.empty:
                rave_stats = rave_data.iloc[0]
                print(f"\n🎯 RAVE PERFORMANCE:")
                print(f"   Signals: {rave_stats['signals']}")
                print(f"   Average Return: {rave_stats['avg_return']*100:+.3f}%")
                print(f"   Win Rate: {rave_stats['win_rate']*100:.1f}%")
                print(f"   Cumulative Return: {rave_stats['cum_return']*100:+.3f}%")
                print(f"   Best Signal: {rave_stats['best_return']*100:+.3f}%")
                print(f"   Worst Signal: {rave_stats['worst_return']*100:+.3f}%")
            else:
                print(f"\n❌ RAVE not found in evaluation data")
                r_coins = [coin for coin in df_performance['coin'] if coin.startswith('R')]
                print(f"Available coins starting with R: {r_coins}")
        
        print(f"\n" + "="*60)
        
    except Exception as e:
        print(f"❌ Error analyzing coin performance: {e}")

if __name__ == "__main__":
    coin = sys.argv[1] if len(sys.argv) > 1 else None
    analyze_coin_performance(coin)