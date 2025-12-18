# Real-Time Scanner Service

A stateless crypto scanner that consumes an existing inference API to generate LONG/SHORT candidates.

## Architecture

```
Scanner (this repo) ---> Inference API (Railway)
```

## Features

- **Stateless**: No persistent storage, deterministic per run
- **Real-time safe**: Never uses partial candles or future data
- **Liquidity tiering**: LARGE/MID/SMALL tiers based on ADV quantiles
- **Cross-sectional ranking**: Robust z-score via MAD
- **Anti-leakage**: Strict temporal boundaries

## Data Flow

1. **Determine last closed bar** (4h timeframe, UTC)
2. **Fetch OHLCV** from Toobit exchange (*/USDT:USDT pairs)
3. **Feature engineering** (ema12, rv_24, adv)
4. **Call inference API** with feature payload
5. **Cross-sectional ranking** with liquidity tiering
6. **Output** top K LONG/SHORT per tier

## Usage

### Local Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run scanner
python scanner.py
```

### Production Deployment

This service is designed to be triggered by:
- Railway Cron (every 4h)
- GitHub Actions
- Manual execution

The scanner is idempotent and safe to run multiple times.

## Configuration

Edit `config.py` to modify:
- `TIMEFRAME_HOURS`: Bar timeframe (default: 4)
- `TOP_K`: Number of candidates per tier (default: 10)
- `INFERENCE_URL`: Inference API endpoint
- Liquidity tier thresholds

## Output Format

```json
{
  "timestamp": "2025-12-19T20:00:00+00:00",
  "universe_size": 150,
  "tiers": {
    "LARGE": {
      "long": [{"symbol": "BTC/USDT:USDT", "scanner_score": 2.45, "adv": 1000000}],
      "short": [{"symbol": "ETH/USDT:USDT", "scanner_score": -1.89, "adv": 800000}]
    },
    "MID": {...},
    "SMALL": {...}
  }
}
```

## Anti-Leakage Guarantees

- ✅ Never uses future bars
- ✅ Never computes features beyond LAST_CLOSED
- ✅ Fails if data lag > 1 bar
- ✅ No smoothing with future information

## Dependencies

- **ccxt**: Exchange data fetching
- **pandas**: Data manipulation
- **numpy**: Numerical operations
- **requests**: API calls
- **scipy**: Statistical functions
- **tqdm**: Progress bars