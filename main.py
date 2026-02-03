#!/usr/bin/env python3
"""
Noon Scraper V2 - Main Entry Point
==================================

Modular scraper with:
- Environment-based configuration (.env)
- Class-based architecture
- Profiling and timing logs

Usage:
    python main.py

Configuration:
    Edit .env file to update cookies and settings

Output:
    - noon_products_scrapped_curl_cffi/       (raw data)
    - noon_products_scrapped_curl_cffi_dedup/ (deduplicated)
    - scraper_profiling.log                   (timing/profiling)
"""

import sys
from datetime import datetime

from scrapers import NoonScraperManager
from utils import logger


def print_banner():
    """Print startup banner."""
    print("""
================================================================================
     NOON SCRAPER V2 - MODULAR ARCHITECTURE
================================================================================

  Project Structure:
    config/
      - settings.py       : Environment configuration (.env loading)
    scrapers/
      - category_scraper.py : CategoryListScraper class
      - product_scraper.py  : ProductDetailScraper class
      - manager.py          : NoonScraperManager orchestration
    utils/
      - helpers.py        : Utilities and profiling
    main.py               : Entry point (this file)
    .env                  : Cookies and settings

  Features:
    - CSV input for category URLs
    - Two-folder output (raw + deduplicated)
    - Memory-efficient batch writing (every 500 products)
    - Profiling to .log file
    - Easy cookie updates via .env

================================================================================
    """)


def main():
    """Main entry point."""
    print_banner()

    # Log start time
    start_time = datetime.now()
    logger.info(f"Session started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Show current configuration
    logger.info("Loading configuration from .env...")

    try:
        # Initialize and run scraper
        manager = NoonScraperManager()

        # Run with all pages (set max_pages_per_category to limit)
        manager.run(max_pages_per_category=None)

    except KeyboardInterrupt:
        logger.warning("\nScraping interrupted by user (Ctrl+C)")
        sys.exit(1)

    except Exception as e:
        logger.error(f"\nFatal error: {e}")
        raise

    finally:
        # Log end time
        end_time = datetime.now()
        duration = end_time - start_time
        logger.info(f"\nSession ended at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Total duration: {duration}")


if __name__ == "__main__":
    main()
