import os
import pandas as pd
from pathlib import Path
from typing import Optional, List
from datetime import datetime, date

class StorageError(Exception):
    """Base exception for storage-related errors."""
    pass

class StorageHelper:
    """Helper class for managing CSV storage operations."""
    
    def __init__(self, base_dir: str = 'data'):
        """
        Initialize the storage helper.
        
        Args:
            base_dir: Base directory for data storage
        """
        self.base_dir = Path(base_dir)
        self._ensure_base_dir()
    
    def _ensure_base_dir(self) -> None:
        """Ensure the base directory exists."""
        self.base_dir.mkdir(parents=True, exist_ok=True)
    
    def _get_symbol_dir(self, symbol: str) -> Path:
        """Get the directory path for a symbol."""
        return self.base_dir / symbol.upper()
    
    def _ensure_symbol_dir(self, symbol: str) -> None:
        """Ensure the symbol directory exists."""
        self._get_symbol_dir(symbol).mkdir(parents=True, exist_ok=True)
    
    def get_existing_dates(self, symbol: str) -> List[date]:
        """
        Get list of dates that have been fetched for a symbol.
        
        Args:
            symbol: The symbol to check
        
        Returns:
            List of dates that have been fetched
        """
        symbol_dir = self._get_symbol_dir(symbol)
        if not symbol_dir.exists():
            return []
        
        dates = []
        for file_path in symbol_dir.glob('*.csv'):
            try:
                # Extract date from filename (YYYY-MM-DD.csv)
                date_str = file_path.stem
                file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                dates.append(file_date)
            except ValueError:
                continue
        
        return sorted(dates)
    
    def save_bars(self, symbol: str, date: date, bars: pd.DataFrame) -> None:
        """
        Save bars data to CSV file.
        
        Args:
            symbol: The symbol
            date: The date of the bars
            bars: DataFrame containing the bars data
        
        Raises:
            StorageError: If saving fails
        """
        try:
            self._ensure_symbol_dir(symbol)
            file_path = self._get_symbol_dir(symbol) / f"{date.strftime('%Y-%m-%d')}.csv"
            
            # Ensure DataFrame has correct columns
            required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            if not all(col in bars.columns for col in required_columns):
                raise StorageError(f"DataFrame missing required columns: {required_columns}")
            
            # Save to CSV
            bars.to_csv(file_path, index=False)
            
        except Exception as e:
            raise StorageError(f"Failed to save bars for {symbol} on {date}: {str(e)}")
    
    def read_bars(self, symbol: str, date: date) -> Optional[pd.DataFrame]:
        """
        Read bars data from CSV file.
        
        Args:
            symbol: The symbol
            date: The date of the bars
        
        Returns:
            DataFrame containing the bars data, or None if file doesn't exist
        """
        file_path = self._get_symbol_dir(symbol) / f"{date.strftime('%Y-%m-%d')}.csv"
        if not file_path.exists():
            return None
        
        try:
            return pd.read_csv(file_path)
        except Exception as e:
            raise StorageError(f"Failed to read bars for {symbol} on {date}: {str(e)}")
    
    def validate_bars(self, bars: pd.DataFrame, expected_rows: int = 390) -> bool:
        """
        Validate bars data.
        
        Args:
            bars: DataFrame containing the bars data
            expected_rows: Expected number of rows (default: 390 for 1-min bars)
        
        Returns:
            True if validation passes, False otherwise
        """
        if bars is None or len(bars) != expected_rows:
            return False
        
        required_columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        if not all(col in bars.columns for col in required_columns):
            return False
        
        # Check for missing values
        if bars[required_columns].isnull().any().any():
            return False
        
        # Check for negative values in OHLCV
        numeric_columns = ['open', 'high', 'low', 'close', 'volume']
        if (bars[numeric_columns] < 0).any().any():
            return False
        
        # Check high/low relationship
        if not (bars['high'] >= bars['low']).all():
            return False
        
        return True 