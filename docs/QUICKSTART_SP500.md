# Quick Start: S&P 500 Wikipedia Parser

Extract current S&P 500 constituents and historical changes from Wikipedia.

## Installation

```bash
cd findata
pip install -e .
```

## Quick Examples

### Get Current Constituents

```python
from findata.parsers import SP500WikipediaParser

parser = SP500WikipediaParser()
constituents = parser.get_current_constituents()

# View data
print(f"Total: {len(constituents)} companies")
print(constituents[['symbol', 'company_name', 'sector']].head(10))
```

### Get Historical Changes

```python
changes = parser.get_historical_changes()

# Recent additions
recent_adds = changes[changes['added_ticker'].notna()].head(5)
print(recent_adds[['date', 'added_ticker', 'added_company']])

# Recent removals
recent_removals = changes[changes['removed_ticker'].notna()].head(5)
print(recent_removals[['date', 'removed_ticker', 'removed_company']])
```

### Export Data

```python
# Export to CSV
parser.export_to_csv(output_dir="./data/sp500")

# Export to JSON
parser.export_to_json(output_dir="./data/sp500")
```

### Summary Statistics

```python
stats = parser.get_summary_stats()
print(f"Total constituents: {stats['total_constituents']}")
print(f"Sectors: {stats['sectors']}")
```

### Filter by Sector

```python
# Technology companies
tech = constituents[constituents['sector'] == 'Information Technology']
print(f"Tech companies: {len(tech)}")

# Financials
financials = constituents[constituents['sector'] == 'Financials']
```

### Companies Added Recently

```python
# Companies added in 2024
recent = constituents[constituents['date_added'].dt.year == 2024]
print(recent[['symbol', 'company_name', 'date_added']])
```

## Data Columns

### Constituents DataFrame
- `symbol`: Stock ticker (e.g., 'AAPL')
- `company_name`: Full company name
- `sector`: GICS sector
- `sub_industry`: GICS sub-industry
- `headquarters`: Company headquarters location
- `date_added`: Date added to S&P 500
- `cik`: Central Index Key
- `founded`: Year founded
- `extracted_at`: When data was extracted
- `source`: Data source ('wikipedia')

### Changes DataFrame
- `date`: Effective date of change
- `added_ticker`: Ticker of added company
- `added_company`: Name of added company
- `removed_ticker`: Ticker of removed company
- `removed_company`: Name of removed company
- `reason`: Reason for change
- `extracted_at`: When data was extracted
- `source`: Data source ('wikipedia')

## Run Complete Example

```bash
cd findata
python examples/sp500_wikipedia_example.py
```

## Run Tests

```bash
# Unit tests only
pytest tests/test_sp500_parser.py -v -m "not integration"

# All tests (including integration - requires internet)
pytest tests/test_sp500_parser.py -v
```

## Current Data (Dec 8, 2025)

- **Total Constituents:** 503 companies
- **Historical Changes:** 387 tracked changes
- **Top Sectors:**
  - Industrials: 79 companies
  - Financials: 75 companies
  - Information Technology: 70 companies

## More Information

- Full documentation: `notes/20251208_sp500_wikipedia_parser.md`
- Architecture: `CLAUDE.md` (Data Parsers section)
- Examples: `examples/sp500_wikipedia_example.py`
- Tests: `tests/test_sp500_parser.py`
