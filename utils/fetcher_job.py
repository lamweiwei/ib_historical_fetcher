import asyncio
import logging
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Any
import pandas as pd
import pandas_market_calendars as mcal
from ib_async import IB, Contract, BarData
from ib_async.client import Client
from ib_async.contract import Stock
import pytz

from .config_loader import get_config, Config
from .contract_resolver import get_contract_resolver, ContractSpec
from .storage import StorageHelper, StorageError

class FetcherError(Exception):
    """Base exception for fetcher-related errors."""
    pass

class FetcherJob:
    """Handles fetching historical data for a single symbol."""
    
    # Map of exchanges to their timezones
    EXCHANGE_TIMEZONES = {
        'SMART': 'US/Eastern',  # Default for US stocks
        'NYSE': 'US/Eastern',
        'NASDAQ': 'US/Eastern',
        'ARCA': 'US/Eastern',
        'CME': 'US/Central',    # For futures
        'ICE': 'US/Eastern',    # For futures
        'GLOBEX': 'US/Central', # For futures
    }
    
    def __init__(self, symbol: str):
        """
        Initialize the fetcher job.
        
        Args:
            symbol: The symbol to fetch data for
        """
        self.symbol = symbol.upper()
        self.config = get_config()
        self.contract_resolver = get_contract_resolver()
        self.storage = StorageHelper()
        
        # Get contract specification
        contract_spec = self.contract_resolver.get_contract(self.symbol)
        if not contract_spec:
            raise FetcherError(f"No contract specification found for {self.symbol}")
        self.contract_spec = contract_spec
        
        # Initialize calendar
        self.calendar = mcal.get_calendar(self.config.calendar.exchange)
        
        # Initialize IB client
        self.ib = IB()
        self.client = None
        
        # Initialize logging
        self.logger = logging.getLogger(f"FetcherJob.{self.symbol}")
        
        # Initialize cancellation flag
        self._cancelled = False
        
        # Set timezone based on exchange
        self.timezone = pytz.timezone(self.EXCHANGE_TIMEZONES.get(
            self.contract_spec.exchange,
            'US/Eastern'  # Default to US/Eastern if exchange not found
        ))
    
    def cancel(self) -> None:
        """Cancel the current fetch operation."""
        self._cancelled = True
        self.logger.info("Fetch operation cancelled")
    
    async def connect(self) -> None:
        """Connect to IB Gateway/TWS."""
        try:
            self.client = await self.ib.connectAsync('127.0.0.1', 7497, clientId=1)
            self.logger.info("Connected to IB Gateway/TWS")
        except Exception as e:
            raise FetcherError(f"Failed to connect to IB Gateway/TWS: {str(e)}")
    
    async def disconnect(self) -> None:
        """Disconnect from IB Gateway/TWS."""
        if self.client:
            await self.ib.disconnect()
            self.logger.info("Disconnected from IB Gateway/TWS")
    
    def _create_contract(self) -> Contract:
        """Create IB contract from specification."""
        return Stock(
            symbol=self.symbol,
            exchange=self.contract_spec.exchange,
            currency=self.contract_spec.currency
        )
    
    def _get_trading_days(self, start_date: date, end_date: date) -> List[date]:
        """
        Get list of valid trading days.
        
        Args:
            start_date: Start date
            end_date: End date
        
        Returns:
            List of trading days
        """
        schedule = self.calendar.schedule(start_date=start_date, end_date=end_date)
        return schedule.index.date.tolist()
    
    def _get_missing_dates(self, trading_days: List[date]) -> List[date]:
        """
        Get list of dates that need to be fetched.
        
        Args:
            trading_days: List of trading days
        
        Returns:
            List of dates that need to be fetched
        """
        existing_dates = set(self.storage.get_existing_dates(self.symbol))
        return [d for d in trading_days if d not in existing_dates]
    
    def _get_exchange_timezone(self) -> pytz.timezone:
        """Get the timezone for the contract's exchange."""
        return self.timezone
    
    def _format_datetime(self, dt: datetime) -> str:
        """
        Format datetime in exchange timezone.
        
        Args:
            dt: Datetime to format
        
        Returns:
            Formatted datetime string in exchange timezone with timezone specification
        """
        # Convert to exchange timezone
        exchange_dt = dt.astimezone(self._get_exchange_timezone())
        # Format as yyyymmdd HH:mm:ss with timezone
        return f"{exchange_dt.strftime('%Y%m%d %H:%M:%S')} {exchange_dt.tzinfo.zone}"
    
    async def _wait_with_countdown(self, seconds: int) -> None:
        """
        Wait for specified seconds with a countdown in logs.
        
        Args:
            seconds: Number of seconds to wait
        """
        for i in range(seconds, 0, -1):
            if i <= 5:  # Only show last 5 seconds to avoid log spam
                self.logger.info(f"{i}...")
            await asyncio.sleep(1)
    
    async def _fetch_bars(self, fetch_date: date, retries: int = 3) -> Optional[pd.DataFrame]:
        """
        Fetch bars for a single day with retry logic.
        
        Args:
            fetch_date: Date to fetch
            retries: Number of retry attempts
        
        Returns:
            DataFrame containing the bars, or None if fetch failed
        """
        if self._cancelled:
            return None
            
        contract = self._create_contract()
        
        for attempt in range(retries):
            if self._cancelled:
                return None
                
            try:
                # Wait for rate limit with countdown
                if attempt > 0:
                    await self._wait_with_countdown(self.config.rate_limit.seconds_between_requests)
                
                # Create end datetime in exchange timezone
                end_dt = datetime.combine(fetch_date, datetime.max.time())
                end_dt = self._get_exchange_timezone().localize(end_dt)
                
                # Log the request with timezone
                self.logger.info(f"Requesting bars for {self.symbol} with end time {self._format_datetime(end_dt)}")
                
                # Request bars with timezone-aware datetime
                bars = await self.ib.reqHistoricalDataAsync(
                    contract,
                    endDateTime=self._format_datetime(end_dt),
                    durationStr='1 D',
                    barSizeSetting='1 min',
                    whatToShow='TRADES',
                    useRTH=True
                )
                
                if not bars:
                    self.logger.warning(f"No bars returned for {fetch_date}")
                    continue
                
                # Convert to DataFrame
                df = pd.DataFrame([{
                    'timestamp': bar.date,
                    'open': bar.open,
                    'high': bar.high,
                    'low': bar.low,
                    'close': bar.close,
                    'volume': bar.volume
                } for bar in bars])
                
                # Validate bars
                if self.storage.validate_bars(df):
                    return df
                else:
                    self.logger.warning(f"Invalid bars for {fetch_date}")
                
            except Exception as e:
                self.logger.error(f"Error fetching bars for {fetch_date} (attempt {attempt + 1}/{retries}): {str(e)}")
                if attempt == retries - 1:
                    return None
        
        return None
    
    async def _find_earliest_available_date(self) -> Optional[date]:
        """
        Find the earliest available historical data date for the symbol.
        
        Returns:
            The earliest available date, or None if not found
        """
        contract = self._create_contract()
        
        try:
            # Request 20 years of data to find the earliest available date
            bars = await self.ib.reqHistoricalDataAsync(
                contract,
                endDateTime=self._format_datetime(datetime.now(self._get_exchange_timezone())),
                durationStr='20 Y',  # Request 20 years of data
                barSizeSetting='1 day',  # Use daily bars for efficiency
                whatToShow='TRADES',
                useRTH=True
            )
            
            if not bars:
                self.logger.warning(f"No historical data found for {self.symbol}")
                return None
            
            # Find the earliest date
            earliest_date = min(bar.date for bar in bars)
            self.logger.info(f"Earliest available data for {self.symbol}: {earliest_date}")
            return earliest_date
            
        except Exception as e:
            self.logger.error(f"Error finding earliest date for {self.symbol}: {str(e)}")
            return None
    
    async def run(self, start_date: Optional[date] = None, end_date: Optional[date] = None) -> Dict[str, Any]:
        """
        Run the fetcher job.
        
        Args:
            start_date: Start date (default: earliest available data)
            end_date: End date (default: today)
        
        Returns:
            Dictionary containing summary statistics
        """
        if not end_date:
            end_date = date.today()
            
        try:
            await self.connect()
            
            # Find earliest available date if not specified
            if not start_date:
                start_date = await self._find_earliest_available_date()
                if not start_date:
                    self.logger.error(f"Could not determine start date for {self.symbol}")
                    return {'status': 'error', 'error': 'Could not determine start date'}
            
            # Get trading days
            trading_days = self._get_trading_days(start_date, end_date)
            missing_dates = self._get_missing_dates(trading_days)
            
            if not missing_dates:
                self.logger.info(f"No missing dates for {self.symbol}")
                return {'status': 'complete', 'days_fetched': 0, 'total_days': len(trading_days)}
            
            # Fetch missing dates
            days_fetched = 0
            days_failed = 0
            
            for i, fetch_date in enumerate(missing_dates, 1):
                if self._cancelled:
                    self.logger.info("Fetch operation cancelled")
                    break
                    
                self.logger.info(f"Fetching {self.symbol} for {fetch_date} ({i}/{len(missing_dates)})")
                
                # Wait for rate limit with countdown
                if i > 1:
                    await self._wait_with_countdown(self.config.rate_limit.seconds_between_requests)
                
                # Fetch and save bars
                bars = await self._fetch_bars(fetch_date)
                if bars is not None:
                    try:
                        self.storage.save_bars(self.symbol, fetch_date, bars)
                        days_fetched += 1
                        self.logger.info(f"✅ {self.symbol} {fetch_date}: {len(bars)} bars")
                    except StorageError as e:
                        self.logger.error(f"Failed to save bars: {str(e)}")
                        days_failed += 1
                else:
                    if self._cancelled:
                        break
                    days_failed += 1
            
            return {
                'status': 'complete' if not self._cancelled else 'cancelled',
                'days_fetched': days_fetched,
                'days_failed': days_failed,
                'total_days': len(trading_days)
            }
            
        except Exception as e:
            self.logger.error(f"Fetcher job failed: {str(e)}")
            return {'status': 'error', 'error': str(e)}
        
        finally:
            await self.disconnect() 