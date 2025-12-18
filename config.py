"""
Configuration constants for the real-time scanner service.
"""

# Time configuration
TIMEFRAME_HOURS = 4
TIMEFRAME = f"{TIMEFRAME_HOURS}h"

# Scanner parameters
TOP_K = 10
ADV_WINDOW = 30  # bars for liquidity ranking
SEED = 42

# Inference API
import os
INFERENCE_URL = os.getenv("INFERENCE_URL", "https://worker-production-b8c8.up.railway.app/api/infer")

# Exchange configuration
EXCHANGE = "toobit"
SYMBOL_SUFFIX = "/USDT:USDT"
OHLCV_LIMIT = 1000

# Liquidity tier thresholds (quantiles)
LARGE_TIER_THRESHOLD = 0.2
MID_TIER_THRESHOLD = 0.6