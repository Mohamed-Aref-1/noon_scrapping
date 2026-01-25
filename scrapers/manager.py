"""
Scraper Manager Module
======================
Orchestrates the entire scraping workflow:
- Reads categories from CSV
- Scrapes each category (saves every 500 records)
- Scrapes product details sequentially (saves every 25 records)
- Creates audit tables (initialized at start, updated after each category)
- Deduplicates files
"""

import os
import time
import random
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional

from config import Config
from utils import (
    logger,
    profiler,
    profile_step,
    profile_function,
    ensure_directories,
    read_categories_from_csv,
    calculate_data_schema,
    reorder_dataframe_columns,
    extract_filename_from_url
)
from .category_scraper import CategoryListScraper
from .product_scraper import ProductDetailScraper


class NoonScraperManager:
    """
    Main manager class that orchestrates the scraping process.
    Handles CSV input, scraping, deduplication, and audit tables.

    Features:
    - Category scraper saves every 500 records
    - Product detail scraper saves every 25 records
    """

    # Configuration
    PRODUCT_BATCH_SIZE = 25  # Save product details every 25 records

    def __init__(self, input_csv: Optional[str] = None):
        self.input_csv = input_csv or Config.INPUT_CSV
        self.category_scraper = CategoryListScraper()
        self.product_scraper = ProductDetailScraper()
        self.scrape_results: List[dict] = []

        # Track current category for product details
        self.current_product_details_file: Optional[str] = None
        self.product_details_header_written: bool = False

        # Product detail batch accumulator
        self.product_detail_batch: List[Dict] = []

        ensure_directories()
        self._initialize_audit_tables()
        logger.info("NoonScraperManager initialized")
        logger.info(f"  - Category batch size: {Config.BATCH_SIZE}")
        logger.info(f"  - Product batch size: {self.PRODUCT_BATCH_SIZE}")

    def _initialize_audit_tables(self):
        """Initialize empty audit tables at startup."""
        audit_columns = ['filename', 'source_url', 'number_of_records', 'data_schema', 'status', 'last_updated']
        empty_df = pd.DataFrame(columns=audit_columns)

        # Initialize audit table for raw data
        raw_audit_path = os.path.join(Config.RAW_OUTPUT_FOLDER, 'audit_table.csv')
        empty_df.to_csv(raw_audit_path, index=False, encoding='utf-8')
        logger.info(f"Initialized audit table: {raw_audit_path}")

        # Initialize audit table for dedup data
        dedup_audit_path = os.path.join(Config.DEDUP_OUTPUT_FOLDER, 'audit_table.csv')
        empty_df.to_csv(dedup_audit_path, index=False, encoding='utf-8')
        logger.info(f"Initialized audit table: {dedup_audit_path}")

        # Initialize audit table for product details
        details_audit_path = os.path.join(Config.PRODUCT_DETAILS_FOLDER, 'audit_table.csv')
        empty_df.to_csv(details_audit_path, index=False, encoding='utf-8')
        logger.info(f"Initialized audit table: {details_audit_path}")

    def _on_batch_written(self, products: List[Dict], category_output_path: str):
        """
        Callback called after each batch of 500 is written to disk.
        Processes product details sequentially and saves every 25 records.
        """
        if not products:
            return

        logger.info(f"\n--- Processing product details for {len(products)} products ---")

        for idx, product in enumerate(products, 1):
            url_slug = product.get('url_slug', '')
            sku = product.get('sku', '')

            if not url_slug or not sku:
                logger.warning(f"Skipping product {idx}: missing url_slug or sku")
                continue

            try:
                # Fetch product details
                detail_rows = self.product_scraper.get_all_product_rows(url_slug, sku, product)

                if detail_rows:
                    self.product_detail_batch.extend(detail_rows)
                    logger.debug(f"Product {idx}/{len(products)}: {sku} - {len(detail_rows)} rows")

                    # Save every PRODUCT_BATCH_SIZE records
                    if len(self.product_detail_batch) >= self.PRODUCT_BATCH_SIZE:
                        self._flush_product_batch()
                else:
                    logger.warning(f"Product {idx}/{len(products)}: {sku} - no data")

                # Small delay between requests
                time.sleep(random.uniform(1.0, 2.0))

            except Exception as e:
                logger.error(f"Error fetching product {sku}: {e}")

            # Log progress every 25 products
            if idx % 25 == 0:
                logger.info(f"Progress: {idx}/{len(products)} products processed")

        logger.info(f"--- Completed product details for batch ---")

    def _flush_product_batch(self):
        """Write current batch to disk."""
        if not self.product_detail_batch or not self.current_product_details_file:
            return

        df = pd.DataFrame(self.product_detail_batch)

        mode = 'w' if not self.product_details_header_written else 'a'
        write_header = not self.product_details_header_written

        df.to_csv(self.current_product_details_file, mode=mode, header=write_header,
                  index=False, encoding='utf-8')
        self.product_details_header_written = True

        logger.info(f"Saved {len(self.product_detail_batch)} product detail rows to disk")
        self.product_detail_batch = []


    def _get_product_details_filename(self, category_filename: str) -> str:
        """Generate product details filename from category filename."""
        # Replace 'noon_' prefix with 'details_noon_'
        if category_filename.startswith('noon_'):
            return 'details_' + category_filename
        return 'details_' + category_filename

    @profile_function
    def run(self, max_pages_per_category: Optional[int] = None):
        """
        Main execution method.

        Flow:
        1. Read categories from CSV
        2. For each category:
           - Scrape category pages (saves every 500 records)
           - Process product details (saves every 25 records)
           - Deduplicate and update audit tables
        """
        # Start profiling session
        profiler.start_session()

        logger.info("\n" + "=" * 70)
        logger.info("NOON SCRAPER - STARTING")
        logger.info("=" * 70)
        logger.info(f"  Category batch size: {Config.BATCH_SIZE}")
        logger.info(f"  Product batch size: {self.PRODUCT_BATCH_SIZE}")
        logger.info("=" * 70 + "\n")

        # Log configuration
        Config.print_config()

        # Step 1: Read categories
        with profile_step("Step 1: Read categories from CSV"):
            categories = read_categories_from_csv(self.input_csv)

        if not categories:
            logger.error("No categories found. Exiting.")
            profiler.end_session()
            return

        logger.info(f"\nProcessing {len(categories)} categories...\n")

        # Step 2: Scrape each category with product details
        with profile_step("Step 2: Scrape all categories"):
            for i, category_url in enumerate(categories, 1):
                logger.info(f"\n[{i}/{len(categories)}] Starting category scrape...")

                # Reset product details tracking for this category
                self.product_details_header_written = False
                self.product_detail_batch = []

                # Set up product details file path BEFORE scraping starts
                category_filename = extract_filename_from_url(category_url)
                self.current_product_details_file = os.path.join(
                    Config.PRODUCT_DETAILS_FOLDER,
                    self._get_product_details_filename(category_filename)
                )
                logger.info(f"Product details file: {self.current_product_details_file}")

                with profile_step(f"Scrape category {i}"):
                    result = self.category_scraper.scrape_category(
                        category_url=category_url,
                        output_folder=Config.RAW_OUTPUT_FOLDER,
                        max_pages=max_pages_per_category,
                        on_batch_written=self._on_batch_written
                    )

                self.scrape_results.append(result)

                # Flush remaining product batch for this category
                if self.product_detail_batch:
                    self._flush_product_batch()

                # Deduplicate this category's files and update audit tables
                logger.info(f"\n--- Deduplicating and updating audit for category {i} ---")
                with profile_step(f"Deduplicate category {i}"):
                    self._deduplicate_category_files(result)
                    self._update_audit_table_for_category(result)

                # Small delay between categories
                if i < len(categories):
                    logger.info("Waiting before next category...")
                    time.sleep(3)

        # End profiling session
        profiler.end_session()

        # Final summary
        self._print_summary()

    def _deduplicate_category_files(self, result: Dict):
        """Deduplicate files for a single category."""
        filename = result.get('filename', '')
        if not filename:
            return

        # Deduplicate category listing file
        raw_path = os.path.join(Config.RAW_OUTPUT_FOLDER, filename)
        if os.path.exists(raw_path):
            dedup_filename = f"dedup_{filename}"
            dedup_path = os.path.join(Config.DEDUP_OUTPUT_FOLDER, dedup_filename)

            try:
                df = pd.read_csv(raw_path)
                original_count = len(df)
                df_dedup = df.drop_duplicates(keep='first')
                dedup_count = len(df_dedup)

                df_dedup.to_csv(dedup_path, index=False, encoding='utf-8')

                result['dedup_filename'] = dedup_filename
                result['dedup_records'] = dedup_count
                result['duplicates_removed'] = original_count - dedup_count

                logger.info(f"Category dedup: {filename} - {original_count} -> {dedup_count} records")
            except Exception as e:
                logger.error(f"Error deduplicating {filename}: {e}")

        # Deduplicate product details file
        details_filename = self._get_product_details_filename(filename)
        details_raw_path = os.path.join(Config.PRODUCT_DETAILS_FOLDER, details_filename)
        if os.path.exists(details_raw_path):
            details_dedup_filename = f"dedup_{details_filename}"
            details_dedup_path = os.path.join(Config.DEDUP_OUTPUT_FOLDER, details_dedup_filename)

            try:
                df = pd.read_csv(details_raw_path)
                original_count = len(df)
                df_dedup = df.drop_duplicates(keep='first')
                dedup_count = len(df_dedup)

                df_dedup.to_csv(details_dedup_path, index=False, encoding='utf-8')

                result['details_dedup_filename'] = details_dedup_filename
                result['details_dedup_records'] = dedup_count
                result['details_duplicates_removed'] = original_count - dedup_count

                logger.info(f"Details dedup: {details_filename} - {original_count} -> {dedup_count} records")
            except Exception as e:
                logger.error(f"Error deduplicating {details_filename}: {e}")

    def _update_audit_table_for_category(self, result: Dict):
        """Update all audit tables after a category is processed."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        filename = result.get('filename', '')
        source_url = result.get('source_url', '')

        # Update raw data audit table
        raw_path = os.path.join(Config.RAW_OUTPUT_FOLDER, filename)
        if os.path.exists(raw_path):
            self._append_to_audit_table(
                Config.RAW_OUTPUT_FOLDER,
                filename,
                source_url,
                raw_path,
                'completed',
                timestamp
            )

        # Update dedup audit table for category file
        dedup_filename = result.get('dedup_filename', '')
        if dedup_filename:
            dedup_path = os.path.join(Config.DEDUP_OUTPUT_FOLDER, dedup_filename)
            if os.path.exists(dedup_path):
                self._append_to_audit_table(
                    Config.DEDUP_OUTPUT_FOLDER,
                    dedup_filename,
                    source_url,
                    dedup_path,
                    'completed',
                    timestamp
                )

        # Update product details audit table
        details_filename = self._get_product_details_filename(filename)
        details_path = os.path.join(Config.PRODUCT_DETAILS_FOLDER, details_filename)
        if os.path.exists(details_path):
            self._append_to_audit_table(
                Config.PRODUCT_DETAILS_FOLDER,
                details_filename,
                source_url,
                details_path,
                'completed',
                timestamp
            )

        # Update dedup audit table for product details
        details_dedup_filename = result.get('details_dedup_filename', '')
        if details_dedup_filename:
            details_dedup_path = os.path.join(Config.DEDUP_OUTPUT_FOLDER, details_dedup_filename)
            if os.path.exists(details_dedup_path):
                self._append_to_audit_table(
                    Config.DEDUP_OUTPUT_FOLDER,
                    details_dedup_filename,
                    source_url,
                    details_dedup_path,
                    'completed',
                    timestamp
                )

        logger.info(f"Audit tables updated for: {filename}")

    def _append_to_audit_table(self, folder: str, filename: str, source_url: str,
                                file_path: str, status: str, timestamp: str):
        """Append a single entry to the audit table."""
        audit_path = os.path.join(folder, 'audit_table.csv')

        try:
            df = pd.read_csv(file_path)
            data_schema = calculate_data_schema(df)
            record_count = len(df)

            new_row = pd.DataFrame([{
                'filename': filename,
                'source_url': source_url,
                'number_of_records': record_count,
                'data_schema': data_schema,
                'status': status,
                'last_updated': timestamp
            }])

            # Append to existing audit table
            new_row.to_csv(audit_path, mode='a', header=False, index=False, encoding='utf-8')

        except Exception as e:
            logger.error(f"Error updating audit table for {filename}: {e}")

    def _print_summary(self):
        """Print final summary of scraping session."""
        logger.info("\n" + "=" * 70)
        logger.info("NOON SCRAPER - COMPLETE")
        logger.info("=" * 70)

        total_records = sum(r.get('number_of_records', 0) for r in self.scrape_results)
        total_dedup_records = sum(r.get('dedup_records', 0) for r in self.scrape_results)
        total_details_records = sum(r.get('details_dedup_records', 0) for r in self.scrape_results)
        successful = sum(1 for r in self.scrape_results if r.get('success'))

        logger.info(f"\nSummary:")
        logger.info(f"  - Categories processed: {len(self.scrape_results)}")
        logger.info(f"  - Successful: {successful}")
        logger.info(f"  - Failed: {len(self.scrape_results) - successful}")
        logger.info(f"  - Total category records scraped: {total_records}")
        logger.info(f"  - Total category records (dedup): {total_dedup_records}")
        logger.info(f"  - Total product details records: {total_details_records}")
        logger.info(f"  - Raw data folder: {Config.RAW_OUTPUT_FOLDER}/")
        logger.info(f"  - Product details folder: {Config.PRODUCT_DETAILS_FOLDER}/")
        logger.info(f"  - Deduplicated folder: {Config.DEDUP_OUTPUT_FOLDER}/")
        logger.info(f"  - Profiling log: {Config.LOG_FILE}")


    def scrape_single_product(self, url_slug: str, sku: str, category_data: Dict = None) -> List[dict]:
        """
        Convenience method to scrape a single product by SKU.

        Args:
            url_slug: Product URL slug
            sku: Product SKU
            category_data: Optional category data to merge

        Returns:
            List of product variant dictionaries
        """
        scraper = ProductDetailScraper()
        return scraper.get_all_product_rows(url_slug, sku, category_data)
