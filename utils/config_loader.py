import os
import yaml
from dataclasses import dataclass
from typing import List, Dict, Any
import logging
from pathlib import Path

@dataclass
class RateLimitConfig:
    seconds_between_requests: int

@dataclass
class CalendarConfig:
    exchange: str

@dataclass
class Config:
    symbols: List[str]
    rate_limit: RateLimitConfig
    calendar: CalendarConfig
    log_level: str

class ConfigError(Exception):
    """Base exception for configuration errors."""
    pass

class ConfigValidationError(ConfigError):
    """Raised when configuration validation fails."""
    pass

class ConfigFileError(ConfigError):
    """Raised when there are issues with the config file."""
    pass

def validate_rate_limit(config: Dict[str, Any]) -> RateLimitConfig:
    """Validate rate limit configuration."""
    if 'rate_limit' not in config:
        raise ConfigValidationError("Missing 'rate_limit' section in config")
    
    rate_limit = config['rate_limit']
    if 'seconds_between_requests' not in rate_limit:
        raise ConfigValidationError("Missing 'seconds_between_requests' in rate_limit config")
    
    seconds = rate_limit['seconds_between_requests']
    if not isinstance(seconds, int) or seconds <= 0:
        raise ConfigValidationError("'seconds_between_requests' must be a positive integer")
    
    return RateLimitConfig(seconds_between_requests=seconds)

def validate_calendar(config: Dict[str, Any]) -> CalendarConfig:
    """Validate calendar configuration."""
    if 'calendar' not in config:
        raise ConfigValidationError("Missing 'calendar' section in config")
    
    calendar = config['calendar']
    if 'exchange' not in calendar:
        raise ConfigValidationError("Missing 'exchange' in calendar config")
    
    exchange = calendar['exchange']
    if not isinstance(exchange, str) or not exchange.strip():
        raise ConfigValidationError("'exchange' must be a non-empty string")
    
    return CalendarConfig(exchange=exchange)

def validate_symbols(config: Dict[str, Any]) -> List[str]:
    """Validate symbols configuration."""
    if 'symbols' not in config:
        raise ConfigValidationError("Missing 'symbols' section in config")
    
    symbols = config['symbols']
    if not isinstance(symbols, list):
        raise ConfigValidationError("'symbols' must be a list")
    
    if not symbols:
        raise ConfigValidationError("'symbols' list cannot be empty")
    
    for symbol in symbols:
        if not isinstance(symbol, str) or not symbol.strip():
            raise ConfigValidationError("All symbols must be non-empty strings")
    
    return symbols

def validate_log_level(config: Dict[str, Any]) -> str:
    """Validate log level configuration."""
    if 'log_level' not in config:
        return 'INFO'  # Default to INFO if not specified
    
    log_level = config['log_level']
    if not isinstance(log_level, str):
        raise ConfigValidationError("'log_level' must be a string")
    
    valid_levels = {'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'}
    if log_level.upper() not in valid_levels:
        raise ConfigValidationError(f"Invalid log level. Must be one of: {', '.join(valid_levels)}")
    
    return log_level.upper()

def load_config(config_path: str = None) -> Config:
    """
    Load and validate configuration from YAML file.
    
    Args:
        config_path: Path to config file. If None, uses default location.
    
    Returns:
        Config object containing validated configuration.
    
    Raises:
        ConfigFileError: If config file cannot be read or parsed
        ConfigValidationError: If config validation fails
    """
    if config_path is None:
        config_path = os.path.join('config', 'config.yaml')
    
    try:
        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
    except FileNotFoundError:
        raise ConfigFileError(f"Config file not found: {config_path}")
    except yaml.YAMLError as e:
        raise ConfigFileError(f"Error parsing config file: {str(e)}")
    
    try:
        symbols = validate_symbols(config_dict)
        rate_limit = validate_rate_limit(config_dict)
        calendar = validate_calendar(config_dict)
        log_level = validate_log_level(config_dict)
        
        return Config(
            symbols=symbols,
            rate_limit=rate_limit,
            calendar=calendar,
            log_level=log_level
        )
    except ConfigValidationError as e:
        raise ConfigValidationError(f"Configuration validation failed: {str(e)}")

def get_config() -> Config:
    """
    Get the configuration singleton.
    This function will load the config if it hasn't been loaded yet.
    """
    if not hasattr(get_config, '_config'):
        get_config._config = load_config()
    return get_config._config 