#!/usr/bin/env python3
import asyncio
import logging
import signal
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

from utils.config_loader import get_config
from utils.contract_resolver import get_contract_resolver
from utils.fetcher_job import FetcherJob, FetcherError

# Global variables for shutdown handling
shutdown_requested = False
current_job: Optional[FetcherJob] = None

def setup_logging() -> logging.Logger:
    """Set up logging configuration."""
    # Create logs directory
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    # Create log filename with timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_file = log_dir / f'fetcher_{timestamp}.log'
    
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    return logging.getLogger('fetcher')

def signal_handler(signum, frame):
    """Handle shutdown signals."""
    global shutdown_requested, current_job
    if not shutdown_requested:
        logger = logging.getLogger('fetcher')
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        shutdown_requested = True
        if current_job:
            current_job.cancel()

async def run_symbol(symbol: str, logger: logging.Logger) -> Dict[str, Any]:
    """
    Run fetcher job for a single symbol.
    
    Args:
        symbol: The symbol to fetch
        logger: Logger instance
    
    Returns:
        Dictionary containing job results
    """
    global current_job
    try:
        logger.info(f"Starting fetch job for {symbol}")
        job = FetcherJob(symbol)
        current_job = job
        result = await job.run()
        current_job = None
        
        if result['status'] == 'error':
            logger.error(f"Error fetching {symbol}: {result.get('error', 'Unknown error')}")
        elif result['status'] == 'cancelled':
            logger.info(f"Fetch job for {symbol} was cancelled")
        else:
            logger.info(
                f"Completed {symbol}: {result['days_fetched']} days fetched, "
                f"{result['days_failed']} failed out of {result['total_days']} total days"
            )
        
        return result
    
    except FetcherError as e:
        logger.error(f"Fetcher error for {symbol}: {str(e)}")
        return {'status': 'error', 'error': str(e)}
    except Exception as e:
        logger.error(f"Unexpected error for {symbol}: {str(e)}")
        return {'status': 'error', 'error': str(e)}
    finally:
        current_job = None

async def main():
    """Main entry point."""
    # Set up logging
    logger = setup_logging()
    logger.info("Starting IB Historical Data Fetcher")
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Load configuration
        config = get_config()
        contract_resolver = get_contract_resolver()
        
        # Validate symbols against contracts
        try:
            contract_resolver.validate_symbols(config.symbols)
        except Exception as e:
            logger.error(f"Configuration validation failed: {str(e)}")
            return
        
        # Initialize results tracking
        results: Dict[str, Dict[str, Any]] = {}
        total_days_fetched = 0
        total_days_failed = 0
        total_days = 0
        
        # Process each symbol
        for symbol in config.symbols:
            if shutdown_requested:
                logger.info("Shutdown requested. Stopping after current symbol...")
                break
            
            result = await run_symbol(symbol, logger)
            results[symbol] = result
            
            if result['status'] == 'complete':
                total_days_fetched += result['days_fetched']
                total_days_failed += result['days_failed']
                total_days += result['total_days']
        
        # Print final summary
        logger.info("\nFinal Summary:")
        logger.info(f"Total symbols processed: {len(results)}")
        logger.info(f"Total days fetched: {total_days_fetched}")
        logger.info(f"Total days failed: {total_days_failed}")
        logger.info(f"Total trading days: {total_days}")
        
        # Print per-symbol summary
        logger.info("\nPer-Symbol Summary:")
        for symbol, result in results.items():
            if result['status'] == 'complete':
                logger.info(
                    f"{symbol}: {result['days_fetched']} fetched, "
                    f"{result['days_failed']} failed out of {result['total_days']} days"
                )
            elif result['status'] == 'cancelled':
                logger.info(f"{symbol}: Operation cancelled")
            else:
                logger.info(f"{symbol}: Failed - {result.get('error', 'Unknown error')}")
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        return
    finally:
        logger.info("IB Historical Data Fetcher finished")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown requested. Exiting...")
    except Exception as e:
        print(f"Fatal error: {str(e)}")
        sys.exit(1) 