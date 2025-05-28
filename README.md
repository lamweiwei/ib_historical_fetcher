# IB Historical Data Fetcher

A robust tool for fetching 1-minute OHLCV historical data from Interactive Brokers TWS API.

## Features

- Fetches 1-minute bars from oldest to newest
- Processes one symbol at a time
- Respects TWS API rate limits (1 request per 10 seconds)
- Automatically resumes from last saved day
- Uses local timezone for CSV timestamps
- Organized, year-based CSV file output
- Contract metadata in separate CSV
- Skips weekends and exchange holidays
- Detects and skips already-fetched days
- Retries failed fetches with detailed logging
- Graceful exit on TWS disconnect
- Comprehensive progress and summary reporting

## Project Structure

```
ib_historical_fetcher/
├── config/
│   ├── config.yaml           # Symbols list, rate limits, exchange
│   └── contracts.csv         # Contract metadata
├── data/                     # Output directory
├── logs/                     # Log files
├── utils/                    # Helper modules
├── fetcher.py               # Main script
├── README.md
└── requirements.txt
```

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure your symbols in `config/config.yaml`

3. Add contract details in `config/contracts.csv`

4. Ensure TWS or IB Gateway is running and configured

## Usage

1. Start TWS or IB Gateway
2. Run the fetcher:
   ```bash
   python fetcher.py
   ```

## Configuration

### config.yaml
- `symbols`: List of symbols to fetch
- `rate_limit`: API rate limiting settings
- `calendar`: Exchange calendar settings
- `log_level`: Logging verbosity

### contracts.csv
- Contains contract metadata for each symbol
- Required fields: symbol, secType, exchange, currency

## Output

Data is organized by symbol and year:
```
data/
  └── AAPL/
        ├── 2022.csv
        ├── 2023.csv
        └── 2024.csv
```

Each CSV contains:
- Timestamp (local timezone)
- OHLCV data
- 390 rows per trading day

## Logging

- One timestamped log file per run
- Console and file logging
- Detailed error tracking
- Progress updates
- Final summaries

## Notes

- Automatically creates required directories
- Skips weekends and holidays
- Retries failed fetches up to 3 times
- Graceful Ctrl+C handling 