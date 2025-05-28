import csv
import os
from dataclasses import dataclass
from typing import Dict, Optional
from pathlib import Path
import pandas as pd

@dataclass
class ContractSpec:
    """Specification for an IB contract."""
    symbol: str
    sec_type: str
    exchange: str
    currency: str

class ContractError(Exception):
    """Base exception for contract-related errors."""
    pass

class ContractFileError(ContractError):
    """Raised when there are issues with the contracts file."""
    pass

class ContractValidationError(ContractError):
    """Raised when contract validation fails."""
    pass

class ContractResolver:
    """Handles loading and resolving contract specifications from contracts.csv."""
    
    REQUIRED_FIELDS = {'symbol', 'secType', 'exchange', 'currency'}
    
    def __init__(self, contracts_path: str = None):
        """
        Initialize the contract resolver.
        
        Args:
            contracts_path: Path to contracts.csv. If None, uses default location.
        """
        if contracts_path is None:
            contracts_path = os.path.join('config', 'contracts.csv')
        self.contracts_path = contracts_path
        self._contracts: Dict[str, ContractSpec] = {}
        self._load_contracts()
    
    def _load_contracts(self) -> None:
        """
        Load and validate contracts from CSV file.
        
        Raises:
            ContractFileError: If file cannot be read or parsed
            ContractValidationError: If contract validation fails
        """
        try:
            df = pd.read_csv(self.contracts_path)
        except FileNotFoundError:
            raise ContractFileError(f"Contracts file not found: {self.contracts_path}")
        except pd.errors.EmptyDataError:
            raise ContractFileError("Contracts file is empty")
        except pd.errors.ParserError as e:
            raise ContractFileError(f"Error parsing contracts file: {str(e)}")
        
        # Validate required fields
        missing_fields = self.REQUIRED_FIELDS - set(df.columns)
        if missing_fields:
            raise ContractValidationError(
                f"Missing required fields in contracts file: {', '.join(missing_fields)}"
            )
        
        # Validate and store contracts
        for _, row in df.iterrows():
            try:
                self._validate_contract_row(row)
                contract = ContractSpec(
                    symbol=row['symbol'].strip(),
                    sec_type=row['secType'].strip(),
                    exchange=row['exchange'].strip(),
                    currency=row['currency'].strip()
                )
                self._contracts[contract.symbol] = contract
            except Exception as e:
                raise ContractValidationError(
                    f"Invalid contract specification for symbol {row.get('symbol', 'UNKNOWN')}: {str(e)}"
                )
    
    def _validate_contract_row(self, row: pd.Series) -> None:
        """
        Validate a single contract row.
        
        Args:
            row: Pandas Series containing contract data
        
        Raises:
            ContractValidationError: If validation fails
        """
        # Check for empty or whitespace-only values
        for field in self.REQUIRED_FIELDS:
            value = row.get(field, '')
            if not isinstance(value, str) or not value.strip():
                raise ContractValidationError(f"Field '{field}' must be a non-empty string")
        
        # Validate security type
        valid_sec_types = {'STK', 'FUT', 'OPT', 'IND', 'CASH'}
        sec_type = row['secType'].strip().upper()
        if sec_type not in valid_sec_types:
            raise ContractValidationError(
                f"Invalid security type '{sec_type}'. Must be one of: {', '.join(valid_sec_types)}"
            )
        
        # Validate currency (basic check for 3-letter code)
        currency = row['currency'].strip().upper()
        if not currency.isalpha() or len(currency) != 3:
            raise ContractValidationError(
                f"Invalid currency code '{currency}'. Must be a 3-letter code."
            )
    
    def get_contract(self, symbol: str) -> Optional[ContractSpec]:
        """
        Get contract specification for a symbol.
        
        Args:
            symbol: The symbol to look up
        
        Returns:
            ContractSpec if found, None otherwise
        """
        return self._contracts.get(symbol.upper())
    
    def get_all_contracts(self) -> Dict[str, ContractSpec]:
        """
        Get all contract specifications.
        
        Returns:
            Dictionary mapping symbols to ContractSpec objects
        """
        return self._contracts.copy()
    
    def validate_symbols(self, symbols: list[str]) -> None:
        """
        Validate that all symbols have corresponding contract specifications.
        
        Args:
            symbols: List of symbols to validate
        
        Raises:
            ContractValidationError: If any symbol is missing a contract specification
        """
        missing_symbols = set(symbol.upper() for symbol in symbols) - set(self._contracts.keys())
        if missing_symbols:
            raise ContractValidationError(
                f"Missing contract specifications for symbols: {', '.join(missing_symbols)}"
            )

# Singleton instance
_contract_resolver: Optional[ContractResolver] = None

def get_contract_resolver() -> ContractResolver:
    """
    Get the contract resolver singleton.
    This function will create the resolver if it hasn't been created yet.
    """
    global _contract_resolver
    if _contract_resolver is None:
        _contract_resolver = ContractResolver()
    return _contract_resolver 