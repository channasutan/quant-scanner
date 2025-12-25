#!/usr/bin/env python3
"""
Analyze scanner backtest performance using public Supabase API
"""
import os
import pandas as pd
import numpy as np
from supabase import create_client
from datetime import datetime, timezone, timedelta

# Use public credentials (read-only access)
SUPABASE_URL = "https://ywgxccyervulrlulwzav.supabase.co"
SUPABASE_ANON_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inl3Z3hjY3llcnZ1bHJsdWx3emF2Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjYwNTg3NjcsImV4cCI6MjA4MTYzNDc2N30.gmfIhuh6rcahuNeWhr6yTLlEh77rG57_Mw_COa8yZ94"

def analyze_backtest_performance():
    """Analyze overall backtest performance from scanner_eval table"""
    
    sb = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    print("🔍 Fetching backtest evaluation data...")
    
    # Get all evaluation data
    try:
        response = sb.table("scanner_eval").select("*").execute()
        eval_data = pd.DataFrame(response.data)
        
        if eval_data.empty:
            print("❌ No evaluation data found")
            return
            
        print(f"📊 Found {len(eval_data)} evaluation records")
        
        # Get scanner results for context
        run_ids = eval_data['run_id'].unique()
        print(f"🔄 Analyzing {len(run_ids)} unique runs")
        
        # Fetch scanner results for these runs
        scanner_response = sb.table("scanner_results").select("run_id, symbol, rank_long, tier").in_("run_id", run_ids.tolist()).execute()
        scanner_data = pd.DataFrame(scanner_response.data)
        
        # Merge evaluation with scanner results
        merged = eval_data.merge(scanner_data, on=['run_id', 'symbol'], how='left')
        
        print("\n" + "="*60)
        print("📈 BACKTEST PERFORMANCE ANALYSIS")
        print("="*60)
        
        # Overall Statistics
        total_signals = len(merged)
        avg_return = merged['fwd_return'].mean() * 100
        median_return = merged['fwd_return'].median() * 100
        std_return = merged['fwd_return'].std() * 100
        
        print(f"\n🎯 OVERALL PERFORMANCE:")
        print(f"   Total Signals: {total_signals:,}")
        print(f"   Average Return: {avg_return:.3f}%")
        print(f"   Median Return: {median_return:.3f}%")
        print(f"   Std Deviation: {std_return:.3f}%")
        
        # Win Rate Analysis
        profitable_signals = (merged['fwd_return'] > 0).sum()
        win_rate = profitable_signals / total_signals * 100
        
        print(f"\n🏆 WIN RATE ANALYSIS:")
        print(f"   Profitable Signals: {profitable_signals:,}")
        print(f"   Losing Signals: {total_signals - profitable_signals:,}")
        print(f"   Win Rate: {win_rate:.2f}%")
        
        # Cumulative Performance
        cumulative_return = merged['fwd_return'].sum() * 100
        print(f"\n💰 CUMULATIVE PERFORMANCE:")
        print(f"   Total Cumulative Return: {cumulative_return:.3f}%")
        
        # Performance by Volatility Regime
        print(f"\n📊 PERFORMANCE BY VOLATILITY REGIME:")
        for tier in ['LARGE', 'MID', 'SMALL']:
            tier_data = merged[merged['tier'] == tier]
            if not tier_data.empty:
                tier_avg = tier_data['fwd_return'].mean() * 100
                tier_win_rate = (tier_data['fwd_return'] > 0).mean() * 100
                tier_count = len(tier_data)
                regime_name = {'LARGE': 'HIGH_VOL', 'MID': 'MID_VOL', 'SMALL': 'LOW_VOL'}[tier]
                print(f"   {regime_name:8} ({tier_count:4} signals): Avg {tier_avg:+6.3f}% | Win Rate {tier_win_rate:5.1f}%")
        
        # Performance by Rank Quintiles
        print(f"\n🎯 PERFORMANCE BY RANK QUINTILES:")
        if 'rank_long' in merged.columns and merged['rank_long'].notna().any():
            try:
                merged['rank_quintile'] = pd.qcut(merged['rank_long'], q=5, labels=['Q1 (Top)', 'Q2', 'Q3', 'Q4', 'Q5 (Bottom)'])
                
                for quintile in ['Q1 (Top)', 'Q2', 'Q3', 'Q4', 'Q5 (Bottom)']:
                    q_data = merged[merged['rank_quintile'] == quintile]
                    if not q_data.empty:
                        q_avg = q_data['fwd_return'].mean() * 100
                        q_win_rate = (q_data['fwd_return'] > 0).mean() * 100
                        q_count = len(q_data)
                        print(f"   {quintile:12} ({q_count:4} signals): Avg {q_avg:+6.3f}% | Win Rate {q_win_rate:5.1f}%")
            except Exception as e:
                print(f"   Unable to analyze rank quintiles: {e}")
        else:
            print("   Rank data not available for quintile analysis")
        
        # Recent Performance (last 30 days)
        recent_runs = sb.table("scanner_runs").select("run_id, asof_ts").gte("asof_ts", (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()).execute()
        recent_run_ids = [r['run_id'] for r in recent_runs.data]
        
        recent_data = merged[merged['run_id'].isin(recent_run_ids)]
        if not recent_data.empty:
            recent_avg = recent_data['fwd_return'].mean() * 100
            recent_win_rate = (recent_data['fwd_return'] > 0).mean() * 100
            recent_count = len(recent_data)
            
            print(f"\n🕒 RECENT PERFORMANCE (Last 30 Days):")
            print(f"   Signals: {recent_count:,}")
            print(f"   Average Return: {recent_avg:+.3f}%")
            print(f"   Win Rate: {recent_win_rate:.2f}%")
        
        # Profitability Assessment
        print(f"\n" + "="*60)
        print("💡 PROFITABILITY ASSESSMENT")
        print("="*60)
        
        is_profitable = avg_return > 0
        is_consistent = win_rate > 50
        
        if is_profitable and is_consistent:
            status = "✅ PROFITABLE & CONSISTENT"
            assessment = "The scanner shows positive expected returns with good win rate."
        elif is_profitable and not is_consistent:
            status = "⚠️  PROFITABLE BUT INCONSISTENT"
            assessment = "Positive returns but low win rate - high risk/reward strategy."
        elif not is_profitable and is_consistent:
            status = "⚠️  CONSISTENT BUT UNPROFITABLE"
            assessment = "High win rate but negative expected returns - needs optimization."
        else:
            status = "❌ UNPROFITABLE & INCONSISTENT"
            assessment = "Poor performance on both metrics - requires major improvements."
        
        print(f"\n{status}")
        print(f"{assessment}")
        
        # Risk Metrics
        sharpe_approx = avg_return / std_return if std_return > 0 else 0
        print(f"\n📊 RISK METRICS:")
        print(f"   Approximate Sharpe Ratio: {sharpe_approx:.3f}")
        print(f"   Max Single Return: {merged['fwd_return'].max() * 100:+.3f}%")
        print(f"   Min Single Return: {merged['fwd_return'].min() * 100:+.3f}%")
        
        print(f"\n" + "="*60)
        
    except Exception as e:
        print(f"❌ Error analyzing performance: {e}")

if __name__ == "__main__":
    analyze_backtest_performance()