# Quick Start: Index Constituent Management

Manage equity index constituents (S&P 500, Dow 30, etc.) with temporal tracking and historical queries.

## CLI Commands

```bash
# Update S&P 500 from Wikipedia
python scripts/setup_database.py --update-index SP500

# Update Dow Jones
python scripts/setup_database.py --update-index DOW30

# Update all configured indices
python scripts/setup_database.py --update-all-indices

# List registered indices
python scripts/setup_database.py --list-indices
```

## DataClient API

### Get Current Constituents

```python
from findata import DataClient

client = DataClient()

# Get current S&P 500 members
sp500 = client.get_index_constituents('SP500')
print(f"Current S&P 500: {len(sp500)} companies")

# Access data
print(sp500[['symbol', 'company_name', 'sector']].head())
```

### Historical Queries

```python
# Get S&P 500 as of Jan 1, 2020
sp500_2020 = client.get_index_constituents('SP500', as_of_date='2020-01-01')

# Compare current vs historical
current_symbols = set(sp500['symbol'])
historical_symbols = set(sp500_2020['symbol'])

added = current_symbols - historical_symbols
removed = historical_symbols - current_symbols

print(f"Added since 2020: {len(added)}")
print(f"Removed since 2020: {len(removed)}")
```

### Check Membership

```python
# Was Tesla in S&P 500 in December 2020?
client.is_index_member('TSLA', 'SP500', date='2020-12-01')  # False
client.is_index_member('TSLA', 'SP500', date='2021-01-01')  # True

# Is Apple currently in S&P 500?
client.is_index_member('AAPL', 'SP500')  # True
```

### Track Changes

```python
# Get all composition changes in 2024
changes = client.get_index_changes('SP500',
                                   start_date='2024-01-01',
                                   end_date='2024-12-31')

# Filter additions and removals
additions = changes[changes['change_type'] == 'added']
removals = changes[changes['change_type'] == 'removed']

print(f"2024 additions: {len(additions)}")
print(f"2024 removals: {len(removals)}")
```

### List All Indices

```python
indices = client.list_indices()
print(indices[['index_code', 'index_name', 'last_updated']])
```

## Adding New Index

1. Create config file: `data/index_configs/nasdaq100.json`

```json
{
  "index_code": "NDX",
  "index_name": "NASDAQ-100",
  "description": "NASDAQ-100 Index - Top 100 Non-Financial NASDAQ Stocks",
  "country": "US",
  "asset_class": "equity",
  "data_source": "wikipedia",
  "url": "https://en.wikipedia.org/wiki/NASDAQ-100",

  "constituents_table": {
    "table_index": 2,
    "column_mapping": {
      "Ticker": "symbol",
      "Company": "company_name",
      "GICS Sector": "sector"
    }
  }
}
```

2. Update from Wikipedia:

```bash
python scripts/setup_database.py --update-index NDX
```

That's it! No code changes needed.

## Use Cases

### Portfolio Construction

```python
# Get all current S&P 500 stocks
sp500 = client.get_index_constituents('SP500')
symbols = sp500['symbol'].tolist()

# Get price data for all constituents
df = client.get_closes(symbols, start='2024-01-01')
```

### Backtesting with Index Rebalancing

```python
# Get index composition on rebalancing date
constituents_q1 = client.get_index_constituents('SP500', as_of_date='2024-01-01')
constituents_q2 = client.get_index_constituents('SP500', as_of_date='2024-04-01')

# Identify changes
q1_symbols = set(constituents_q1['symbol'])
q2_symbols = set(constituents_q2['symbol'])

# Rebalance portfolio
to_buy = q2_symbols - q1_symbols
to_sell = q1_symbols - q2_symbols
```

### Index Tracking Analysis

```python
# Get all changes in past year
changes = client.get_index_changes('SP500',
                                   start_date='2024-01-01')

# Analyze turnover
additions = changes[changes['change_type'] == 'added']
removals = changes[changes['change_type'] == 'removed']

turnover_rate = len(additions) / len(current_constituents)
print(f"Annual turnover: {turnover_rate:.1%}")
```

### Sector Analysis

```python
# Current sector breakdown
sp500 = client.get_index_constituents('SP500')
sector_counts = sp500['sector'].value_counts()

# Historical comparison
sp500_2020 = client.get_index_constituents('SP500', as_of_date='2020-01-01')
sector_counts_2020 = sp500_2020['sector'].value_counts()

# Compare
comparison = pd.DataFrame({
    '2020': sector_counts_2020,
    'Current': sector_counts,
    'Change': sector_counts - sector_counts_2020
})
```

## Database Schema

### indices table
- index_id, index_code, index_name
- description, country, asset_class
- data_source, created_at, last_updated

### index_constituents table
- constituent_id, index_id, symbol
- effective_date, end_date (NULL = active)
- company_name, sector, sub_industry
- date_added_to_index, extracted_at, data_source

## Run Examples

```bash
# Comprehensive example script
python examples/index_management_example.py
```

## More Information

- **Implementation Details**: `notes/20251208_index_management_implementation.md`
- **Architecture**: `CLAUDE.md` (Index Management section)
- **Examples**: `examples/index_management_example.py`
- **CLI Help**: `python scripts/setup_database.py --help`

## Current Configured Indices

- **SP500**: S&P 500 Index
- **DOW30**: Dow Jones Industrial Average

To add more, create config file in `data/index_configs/`.
