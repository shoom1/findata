# Quick Start: Loading Index Data

## Two-Step Process

### Step 1: Update Index Constituents (Membership Data)

Get the current list of stocks in each index from Wikipedia:

```bash
# Individual indices
python scripts/setup_database.py --update-index SP500
python scripts/setup_database.py --update-index DOW30
python scripts/setup_database.py --update-index NDX
python scripts/setup_database.py --update-index FTSE100
python scripts/setup_database.py --update-index DAX

# Or all at once
python scripts/setup_database.py --update-all-indices
```

This gives you: symbol names, company names, sectors, dates added to index.

### Step 2: Download Historical Price Data (OHLCV)

Download actual price history for each constituent:

```bash
# All constituents (full history from 2005)
python scripts/setup_database.py --load-index-data DOW30      # ~2.5 min
python scripts/setup_database.py --load-index-data DAX        # ~3.4 min
python scripts/setup_database.py --load-index-data FTSE100    # ~8.3 min
python scripts/setup_database.py --load-index-data NDX        # ~8.4 min
python scripts/setup_database.py --load-index-data SP500      # ~42 min

# Recent data only (faster)
python scripts/setup_database.py --load-index-data SP500 --index-start-date 2020-01-01

# Test with first 10 stocks
python scripts/setup_database.py --load-index-data SP500 --index-max-symbols 10
```

This gives you: daily OHLCV data in the `timeseries_data` table.

## Available Indices

| Code | Name | Constituents | Country |
|------|------|--------------|---------|
| SP500 | S&P 500 | 503 | US |
| DOW30 | Dow Jones Industrial Average | 30 | US |
| NDX | NASDAQ-100 | 101 | US |
| FTSE100 | FTSE 100 | 100 | GB (UK) |
| DAX | DAX | 41 | DE (Germany) |

## Check Status

```bash
# List all indices
python scripts/setup_database.py --list-indices

# Or via Python
python -c "from findata import DataClient; c = DataClient(); print(c.get_stats())"
```

## Use the Data

```python
from findata import DataClient

client = DataClient()

# Get DOW30 closing prices
dow30_symbols = ['AAPL', 'MSFT', 'JPM', 'V', 'WMT']  # etc.
prices = client.get_closes(dow30_symbols, start='2020-01-01')

# Or get all index constituents
sp500_constituents = client.get_index_constituents('SP500')
all_sp500_prices = client.get_closes(sp500_constituents['symbol'].tolist(), start='2020-01-01')
```

## Incremental Loading (SP500)

Since SP500 has 503 stocks (~42 min), load in batches:

```bash
# Batch 1: First 100
python scripts/setup_database.py --load-index-data SP500 --index-max-symbols 100

# Batch 2: Next 100 (skips first 100 automatically)
python scripts/setup_database.py --load-index-data SP500 --index-max-symbols 200

# Continue until all loaded...
```

The loader automatically skips symbols that already have data!

## Full Documentation

- **Detailed guide**: `notes/how_to_load_index_data.md`
- **Example code**: `examples/load_index_data_example.py`
- **Main docs**: `CLAUDE.md`

## Summary

```bash
# Complete workflow (5 indices)
python scripts/setup_database.py --update-all-indices           # ~30 seconds
python scripts/setup_database.py --load-index-data DOW30        # ~2.5 min
python scripts/setup_database.py --load-index-data DAX          # ~3.4 min
python scripts/setup_database.py --load-index-data FTSE100      # ~8.3 min
python scripts/setup_database.py --load-index-data NDX          # ~8.4 min
python scripts/setup_database.py --load-index-data SP500        # ~42 min
# Total: ~65 minutes for 775 stocks with 20 years of data
```

Now you're ready to run backtests and analytics!
