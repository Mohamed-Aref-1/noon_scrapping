"""
Scraper Manager Module
======================
Orchestrates the entire scraping workflow with INDEPENDENT scrapers:
- Category scraper: scrapes categories, deduplicates every 1000 records → noon_category_dedup
- Product scraper: reads from noon_category_dedup in chunks of 100, processes independently
"""

import os
import time
import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict, Optional
import threading
from queue import Queue

from config import Config
from utils import (
    profiler,
    profile_step,
    profile_function,
    ensure_directories,
    read_categories_from_csv,
    calculate_data_schema,
    extract_filename_from_url
)


# Generate timestamp for log files (once at module load)
_log_timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')


def setup_category_logger():
    """Setup logger for category API with timestamped log file."""
    log_logger = logging.getLogger('category_scraper')
    log_logger.setLevel(logging.INFO)
    log_logger.handlers = []

    log_filename = f'category_scraper_{_log_timestamp}.log'
    file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - [CATEGORY] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_format)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(file_format)

    log_logger.addHandler(file_handler)
    log_logger.addHandler(console_handler)
    return log_logger


def setup_product_details_logger():
    """Setup logger for product details API with timestamped log file."""
    log_logger = logging.getLogger('product_details_scraper')
    log_logger.setLevel(logging.INFO)
    log_logger.handlers = []

    log_filename = f'product_scraper_{_log_timestamp}.log'
    file_handler = logging.FileHandler(log_filename, mode='a', encoding='utf-8')
    file_handler.setLevel(logging.INFO)
    file_format = logging.Formatter(
        '%(asctime)s - %(levelname)s - [PRODUCT] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    file_handler.setFormatter(file_format)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(file_format)

    log_logger.addHandler(file_handler)
    log_logger.addHandler(console_handler)
    return log_logger


category_logger = setup_category_logger()
product_details_logger = setup_product_details_logger()

from .category_scraper import CategoryListScraper
from .product_scraper import ProductDetailScraper


class NoonScraperManager:
    """
    Manager with INDEPENDENT category and product scrapers.

    Category Scraper:
    - Scrapes categories from CSV
    - Saves raw data every 500 records to noon_category_raw
    - Deduplicates every 1000 records and appends to noon_category_dedup

    Product Scraper:
    - Runs in parallel, reads from noon_category_dedup
    - Processes 100 products at a time
    - Tracks progress to know which products have been processed
    """

    CATEGORY_DEDUP_BATCH_SIZE = 1000  # Deduplicate every 1000 records
    PRODUCT_CHUNK_SIZE = 100  # Read 100 products at a time from dedup
    PRODUCT_BATCH_SIZE = 500  # Save product details every 500 records

    def __init__(self, input_csv: Optional[str] = None):
        self.input_csv = input_csv or Config.INPUT_CSV
        self.category_scraper = CategoryListScraper()
        self.product_scraper = ProductDetailScraper()
        self.scrape_results: List[dict] = []

        # Category scraper state
        self.current_category_index = 0
        self.total_categories = 0
        self.current_category_name = ""
        self.category_dedup_buffer: List[Dict] = []  # Buffer for dedup
        self.current_dedup_file: Optional[str] = None
        self.dedup_header_written: Dict[str, bool] = {}

        # Product scraper state
        self.product_queue = Queue(maxsize=self.PRODUCT_CHUNK_SIZE + 10)
        self.product_processing_thread = None
        self.product_file_reader_thread = None
        self.processing_active = False
        self.products_processed = 0
        self.total_records_scrapped = 0
        self.current_product_details_file: Optional[str] = None

        # Progress tracking file (to know which products have been processed)
        self.progress_file = os.path.join(Config.PRODUCT_RAW_FOLDER, 'progress_tracker.csv')

        ensure_directories()
        self._initialize_audit_tables()

        category_logger.info("NoonScraperManager initialized")
        category_logger.info(f"  - Category batch size: {Config.BATCH_SIZE}")
        category_logger.info(f"  - Category dedup batch: {self.CATEGORY_DEDUP_BATCH_SIZE}")
        category_logger.info(f"  - Product chunk size: {self.PRODUCT_CHUNK_SIZE}")
        category_logger.info(f"  - Product save batch: {self.PRODUCT_BATCH_SIZE}")
        category_logger.info(f"  - Category log: category_scraper_{_log_timestamp}.log")
        category_logger.info(f"  - Product log: product_scraper_{_log_timestamp}.log")

    def _initialize_audit_tables(self):
        """Initialize empty audit tables at startup."""
        audit_columns = ['filename', 'source_url', 'number_of_records', 'data_schema', 'status', 'last_updated']
        empty_df = pd.DataFrame(columns=audit_columns)

        for folder in [Config.CATEGORY_RAW_FOLDER, Config.CATEGORY_DEDUP_FOLDER,
                       Config.PRODUCT_RAW_FOLDER, Config.PRODUCT_DEDUP_FOLDER]:
            audit_path = os.path.join(folder, 'audit_table.csv')
            empty_df.to_csv(audit_path, index=False, encoding='utf-8')

    def _get_product_details_filename(self, category_filename: str) -> str:
        """Generate product details filename from category filename."""
        if category_filename.startswith('dedup_'):
            return 'details_' + category_filename[6:]  # Remove 'dedup_' prefix
        if category_filename.startswith('noon_'):
            return 'details_' + category_filename
        return 'details_' + category_filename

    # =========================================================================
    # CATEGORY SCRAPER - Deduplicates every 1000 records
    # =========================================================================

    def _on_category_batch_written(self, products: List[Dict], raw_output_path: str):
        """
        Callback when category scraper writes a batch of 500 products.
        Accumulates products and deduplicates every 1000 records.
        """
        if not products:
            return

        # Add to dedup buffer
        self.category_dedup_buffer.extend(products)

        category_logger.info(
            f"[Category {self.current_category_index}/{self.total_categories}] "
            f"Raw batch written: {len(products)} products | Dedup buffer: {len(self.category_dedup_buffer)}"
        )

        # Deduplicate when buffer reaches 1000
        if len(self.category_dedup_buffer) >= self.CATEGORY_DEDUP_BATCH_SIZE:
            self._flush_dedup_buffer()

    def _flush_dedup_buffer(self):
        """Deduplicate and write the buffer to dedup file."""
        if not self.category_dedup_buffer or not self.current_dedup_file:
            return

        df = pd.DataFrame(self.category_dedup_buffer)
        original_count = len(df)

        # Deduplicate based on SKU
        df_dedup = df.drop_duplicates(subset=['sku'], keep='first')
        dedup_count = len(df_dedup)

        # Append to dedup file
        write_header = not self.dedup_header_written.get(self.current_dedup_file, False)
        mode = 'w' if write_header else 'a'

        df_dedup.to_csv(self.current_dedup_file, mode=mode, header=write_header,
                        index=False, encoding='utf-8')
        self.dedup_header_written[self.current_dedup_file] = True

        category_logger.info(
            f"[Category {self.current_category_index}/{self.total_categories}] "
            f"DEDUP: {original_count} → {dedup_count} records | Saved to {os.path.basename(self.current_dedup_file)}"
        )

        # Clear buffer
        self.category_dedup_buffer = []

    # =========================================================================
    # PRODUCT SCRAPER - Reads from dedup files independently
    # =========================================================================

    def _get_processed_skus(self, dedup_file: str) -> set:
        """Get set of SKUs that have already been processed for this file."""
        details_filename = self._get_product_details_filename(os.path.basename(dedup_file))
        details_path = os.path.join(Config.PRODUCT_RAW_FOLDER, details_filename)

        if not os.path.exists(details_path):
            return set()

        try:
            df = pd.read_csv(details_path, usecols=['sku'])
            return set(df['sku'].unique())
        except Exception as e:
            product_details_logger.error(f"Error reading processed SKUs: {e}")
            return set()

    def _product_file_reader(self, dedup_file: str, category_index: int, total_categories: int):
        """
        Background thread that reads from dedup file in chunks of 100.
        Adds products to the queue for processing.
        """
        try:
            # Wait a bit for some dedup data to be written
            time.sleep(5)

            category_name = os.path.basename(dedup_file).replace('.csv', '')
            product_details_logger.info(f"\n{'='*60}")
            product_details_logger.info(f"[Category {category_index}/{total_categories}] PRODUCT READER STARTED")
            product_details_logger.info(f"Reading from: {dedup_file}")
            product_details_logger.info(f"{'='*60}")

            # Set up output file
            details_filename = self._get_product_details_filename(os.path.basename(dedup_file))
            self.current_product_details_file = os.path.join(Config.PRODUCT_RAW_FOLDER, details_filename)

            last_read_position = 0
            processed_skus = self._get_processed_skus(dedup_file)
            product_details_logger.info(f"[Category {category_index}/{total_categories}] Already processed: {len(processed_skus)} SKUs")

            while self.processing_active:
                # Check if file exists and has new data
                if not os.path.exists(dedup_file):
                    time.sleep(2)
                    continue

                try:
                    # Read the entire file (it's being appended to)
                    df = pd.read_csv(dedup_file)
                    total_rows = len(df)

                    if total_rows <= last_read_position:
                        # No new data, wait and check again
                        time.sleep(2)
                        continue

                    # Get new rows
                    new_rows = df.iloc[last_read_position:].to_dict('records')

                    # Filter out already processed SKUs
                    unprocessed = [r for r in new_rows if r.get('sku') not in processed_skus]

                    if unprocessed:
                        product_details_logger.info(
                            f"[Category {category_index}/{total_categories}] "
                            f"Found {len(unprocessed)} new products (rows {last_read_position+1}-{total_rows})"
                        )

                        # Add to queue in chunks of 100
                        for i in range(0, len(unprocessed), self.PRODUCT_CHUNK_SIZE):
                            chunk = unprocessed[i:i + self.PRODUCT_CHUNK_SIZE]
                            for product in chunk:
                                queue_item = {
                                    'url_slug': product.get('url_slug', ''),
                                    'sku': product.get('sku', ''),
                                    'category_data': product,
                                    'output_file': self.current_product_details_file,
                                    'category_index': category_index,
                                    'total_categories': total_categories,
                                    'category_name': category_name,
                                }
                                self.product_queue.put(queue_item)
                                processed_skus.add(product.get('sku'))

                            product_details_logger.info(
                                f"[Category {category_index}/{total_categories}] "
                                f"Queued chunk of {len(chunk)} products | Queue: {self.product_queue.qsize()}"
                            )

                            # Wait for queue to be processed before adding more
                            while self.product_queue.qsize() > 10 and self.processing_active:
                                time.sleep(1)

                    last_read_position = total_rows

                except Exception as e:
                    product_details_logger.error(f"Error reading dedup file: {e}")
                    time.sleep(2)

                # Small delay before checking for more data
                time.sleep(1)

        except Exception as e:
            product_details_logger.error(f"Product file reader error: {e}")

    def _product_processor(self):
        """
        Background thread that processes products from the queue.
        """
        product_batches = {}
        file_headers_written = {}

        while self.processing_active or not self.product_queue.empty():
            try:
                item = self.product_queue.get(timeout=2)

                if item is None:
                    break

                url_slug = item['url_slug']
                sku = item['sku']
                category_data = item['category_data']
                output_file = item['output_file']
                category_index = item.get('category_index', 0)
                total_categories = item.get('total_categories', 0)

                if not url_slug or not sku:
                    self.product_queue.task_done()
                    continue

                # Process the product
                detail_rows = self.product_scraper.get_all_product_rows(url_slug, sku, category_data)

                if detail_rows:
                    if output_file not in product_batches:
                        product_batches[output_file] = []

                    product_batches[output_file].extend(detail_rows)
                    self.products_processed += 1
                    self.total_records_scrapped += len(detail_rows)

                    # Log progress every 10 products
                    if self.products_processed % 10 == 0:
                        product_details_logger.info(
                            f"[Category {category_index}/{total_categories}] "
                            f"Processed: {self.products_processed} | Records: {self.total_records_scrapped} | "
                            f"Queue: {self.product_queue.qsize()}"
                        )

                    # Save batch when it reaches size
                    if len(product_batches[output_file]) >= self.PRODUCT_BATCH_SIZE:
                        header_written = file_headers_written.get(output_file, False)
                        self._write_product_batch(product_batches[output_file], output_file, not header_written)
                        file_headers_written[output_file] = True
                        product_batches[output_file] = []

                        product_details_logger.info(
                            f"[Category {category_index}/{total_categories}] "
                            f"BATCH SAVED: {self.PRODUCT_BATCH_SIZE} records to {os.path.basename(output_file)}"
                        )

                self.product_queue.task_done()

            except Exception as e:
                if "Empty" not in str(e):
                    product_details_logger.error(f"Product processor error: {e}")
                continue

        # Flush remaining batches
        for file_path, batch in product_batches.items():
            if batch:
                header_written = file_headers_written.get(file_path, False)
                self._write_product_batch(batch, file_path, not header_written)
                product_details_logger.info(f"Final batch: {len(batch)} records to {os.path.basename(file_path)}")

    def _write_product_batch(self, batch, file_path, write_header):
        """Write a batch of products to file."""
        if not batch or not file_path:
            return

        df = pd.DataFrame(batch)
        mode = 'w' if write_header else 'a'
        df.to_csv(file_path, mode=mode, header=write_header, index=False, encoding='utf-8')

    def _start_product_scraper(self, dedup_file: str, category_index: int, total_categories: int):
        """Start the product scraper threads for a category."""
        self.processing_active = True

        # Start the file reader thread
        self.product_file_reader_thread = threading.Thread(
            target=self._product_file_reader,
            args=(dedup_file, category_index, total_categories)
        )
        self.product_file_reader_thread.daemon = True
        self.product_file_reader_thread.start()

        # Start the processor thread (if not already running)
        if self.product_processing_thread is None or not self.product_processing_thread.is_alive():
            self.product_processing_thread = threading.Thread(target=self._product_processor)
            self.product_processing_thread.daemon = True
            self.product_processing_thread.start()
            product_details_logger.info("Product processor thread started")

    def _stop_product_scraper(self):
        """Stop the product scraper threads."""
        self.processing_active = False

        # Wait for queue to empty
        if not self.product_queue.empty():
            product_details_logger.info(f"Waiting for {self.product_queue.qsize()} products in queue...")
            self.product_queue.join()

        # Signal threads to stop
        self.product_queue.put(None)

        if self.product_file_reader_thread:
            self.product_file_reader_thread.join(timeout=5)
        if self.product_processing_thread:
            self.product_processing_thread.join(timeout=10)

        product_details_logger.info("Product scraper stopped")

    # =========================================================================
    # MAIN RUN METHODS
    # =========================================================================

    @profile_function
    def run(self, max_pages_per_category: Optional[int] = None):
        """Main execution method - routes based on SCRAPER_MODE."""
        mode = Config.SCRAPER_MODE

        if mode == 'CATEGORY_ONLY':
            category_logger.info("Running in CATEGORY_ONLY mode")
            self.run_category_only(max_pages_per_category)
        elif mode == 'PRODUCTS_ONLY':
            category_logger.info("Running in PRODUCTS_ONLY mode")
            self.run_products_only()
        else:
            category_logger.info("Running in BOTH mode (independent scrapers)")
            self.run_both(max_pages_per_category)

    def run_both(self, max_pages_per_category: Optional[int] = None):
        """
        Run both scrapers TRULY INDEPENDENTLY:
        - Category scraper runs through all categories without waiting
        - Product scraper runs in background, processes all dedup files
        - Category scraper does NOT wait for product scraper between categories
        """
        profiler.start_session()

        category_logger.info("\n" + "=" * 70)
        category_logger.info("NOON SCRAPER - BOTH MODE (TRULY INDEPENDENT)")
        category_logger.info("=" * 70)
        category_logger.info("Category scraper will NOT wait for product scraper!")
        Config.print_config()

        categories = read_categories_from_csv(self.input_csv)
        if not categories:
            category_logger.error("No categories found. Exiting.")
            profiler.end_session()
            return

        self.total_categories = len(categories)
        category_logger.info(f"\nProcessing {len(categories)} categories...\n")

        # Collect all dedup files that will be created
        dedup_files_to_process = []

        # Start the product processor thread (runs continuously)
        self.processing_active = True
        self.product_processing_thread = threading.Thread(target=self._product_processor)
        self.product_processing_thread.daemon = True
        self.product_processing_thread.start()
        product_details_logger.info("Product processor thread started (will process all categories)")

        # Start the product file monitor thread (monitors all dedup files)
        self.product_file_monitor_thread = threading.Thread(
            target=self._product_file_monitor,
            args=(categories,)
        )
        self.product_file_monitor_thread.daemon = True
        self.product_file_monitor_thread.start()
        product_details_logger.info("Product file monitor thread started")

        # Run category scraper for ALL categories (no waiting for product scraper)
        for i, category_url in enumerate(categories, 1):
            self.current_category_index = i
            self.current_category_name = category_url.split('/')[-2] if category_url.endswith('/') else category_url.split('/')[-1]

            category_logger.info(f"\n{'='*70}")
            category_logger.info(f"[Category {i}/{len(categories)}] STARTING")
            category_logger.info(f"URL: {category_url}")
            category_logger.info(f"{'='*70}")

            # Set up dedup file for this category
            category_filename = extract_filename_from_url(category_url)
            dedup_filename = f"dedup_{category_filename}"
            self.current_dedup_file = os.path.join(Config.CATEGORY_DEDUP_FOLDER, dedup_filename)
            self.category_dedup_buffer = []
            dedup_files_to_process.append(self.current_dedup_file)

            # Run category scraper (writes to raw, triggers dedup callback)
            with profile_step(f"Scrape category {i}"):
                result = self.category_scraper.scrape_category(
                    category_url=category_url,
                    output_folder=Config.CATEGORY_RAW_FOLDER,
                    max_pages=max_pages_per_category,
                    on_batch_written=self._on_category_batch_written
                )

            self.scrape_results.append(result)

            # Flush any remaining dedup buffer
            if self.category_dedup_buffer:
                self._flush_dedup_buffer()

            category_logger.info(f"[Category {i}/{len(categories)}] Category scraping COMPLETE")
            category_logger.info(f"[Category {i}/{len(categories)}] Product scraper will continue in background...")

            # Update audit tables for category
            self._update_category_audit(result)

            if i < len(categories):
                category_logger.info(f"[Category {i}/{len(categories)}] Moving to next category in 3s...")
                time.sleep(3)

        # Category scraper is done - now wait for product scraper to finish
        category_logger.info("\n" + "=" * 70)
        category_logger.info("ALL CATEGORIES SCRAPED - Waiting for product scraper to finish...")
        category_logger.info("=" * 70)

        # Signal that no more categories will be added
        self.categories_complete = True

        # Wait for product scraper to finish all products
        while not self.product_queue.empty() or self.products_being_processed:
            remaining = self.product_queue.qsize()
            product_details_logger.info(f"Waiting for product scraper... Queue: {remaining} | Processed: {self.products_processed}")
            time.sleep(5)

        # Stop product scraper
        self._stop_product_scraper()

        # Update product audit tables
        self._update_product_audit()

        profiler.end_session()
        self._print_summary()

    def run_category_only(self, max_pages_per_category: Optional[int] = None):
        """Run only the category scraper."""
        profiler.start_session()

        category_logger.info("\n" + "=" * 70)
        category_logger.info("NOON SCRAPER - CATEGORY ONLY MODE")
        category_logger.info("=" * 70)
        Config.print_config()

        categories = read_categories_from_csv(self.input_csv)
        if not categories:
            category_logger.error("No categories found. Exiting.")
            profiler.end_session()
            return

        self.total_categories = len(categories)

        for i, category_url in enumerate(categories, 1):
            self.current_category_index = i
            self.current_category_name = category_url.split('/')[-2] if category_url.endswith('/') else category_url.split('/')[-1]

            category_logger.info(f"\n{'='*70}")
            category_logger.info(f"[Category {i}/{len(categories)}] STARTING")
            category_logger.info(f"URL: {category_url}")
            category_logger.info(f"{'='*70}")

            # Set up dedup file
            category_filename = extract_filename_from_url(category_url)
            dedup_filename = f"dedup_{category_filename}"
            self.current_dedup_file = os.path.join(Config.CATEGORY_DEDUP_FOLDER, dedup_filename)
            self.category_dedup_buffer = []

            with profile_step(f"Scrape category {i}"):
                result = self.category_scraper.scrape_category(
                    category_url=category_url,
                    output_folder=Config.CATEGORY_RAW_FOLDER,
                    max_pages=max_pages_per_category,
                    on_batch_written=self._on_category_batch_written
                )

            self.scrape_results.append(result)

            # Flush remaining dedup buffer
            if self.category_dedup_buffer:
                self._flush_dedup_buffer()

            category_logger.info(f"[Category {i}/{len(categories)}] COMPLETED")

            if i < len(categories):
                time.sleep(3)

        profiler.end_session()
        self._print_category_only_summary()

    def run_products_only(self):
        """Run only the product scraper on existing dedup files."""
        profiler.start_session()

        product_details_logger.info("\n" + "=" * 70)
        product_details_logger.info("NOON SCRAPER - PRODUCTS ONLY MODE")
        product_details_logger.info("=" * 70)
        Config.print_config()

        # Find dedup files that haven't been fully processed
        dedup_folder = Config.CATEGORY_DEDUP_FOLDER
        if not os.path.exists(dedup_folder):
            product_details_logger.error(f"Dedup folder not found: {dedup_folder}")
            profiler.end_session()
            return

        dedup_files = [f for f in os.listdir(dedup_folder)
                       if f.endswith('.csv') and f != 'audit_table.csv']

        if not dedup_files:
            product_details_logger.info("No dedup files found to process.")
            profiler.end_session()
            return

        self.total_categories = len(dedup_files)
        product_details_logger.info(f"\nFound {len(dedup_files)} dedup files to process:\n")

        self.processing_active = True

        # Start processor thread
        self.product_processing_thread = threading.Thread(target=self._product_processor)
        self.product_processing_thread.daemon = True
        self.product_processing_thread.start()

        for i, filename in enumerate(dedup_files, 1):
            self.current_category_index = i
            dedup_path = os.path.join(dedup_folder, filename)

            product_details_logger.info(f"\n{'='*60}")
            product_details_logger.info(f"[Category {i}/{len(dedup_files)}] Processing: {filename}")
            product_details_logger.info(f"{'='*60}")

            # Set up output file
            details_filename = self._get_product_details_filename(filename)
            self.current_product_details_file = os.path.join(Config.PRODUCT_RAW_FOLDER, details_filename)

            # Get already processed SKUs
            processed_skus = self._get_processed_skus(dedup_path)
            product_details_logger.info(f"[Category {i}/{len(dedup_files)}] Already processed: {len(processed_skus)} SKUs")

            # Read and process in chunks
            try:
                df = pd.read_csv(dedup_path)
                total_products = len(df)
                product_details_logger.info(f"[Category {i}/{len(dedup_files)}] Total products in file: {total_products}")

                # Filter unprocessed
                df_unprocessed = df[~df['sku'].isin(processed_skus)]
                unprocessed_count = len(df_unprocessed)
                product_details_logger.info(f"[Category {i}/{len(dedup_files)}] Unprocessed products: {unprocessed_count}")

                if unprocessed_count == 0:
                    product_details_logger.info(f"[Category {i}/{len(dedup_files)}] All products already processed. Skipping.")
                    continue

                # Process in chunks of 100
                for chunk_start in range(0, unprocessed_count, self.PRODUCT_CHUNK_SIZE):
                    chunk_end = min(chunk_start + self.PRODUCT_CHUNK_SIZE, unprocessed_count)
                    chunk = df_unprocessed.iloc[chunk_start:chunk_end].to_dict('records')

                    product_details_logger.info(
                        f"[Category {i}/{len(dedup_files)}] "
                        f"Processing chunk {chunk_start+1}-{chunk_end} of {unprocessed_count}"
                    )

                    for product in chunk:
                        queue_item = {
                            'url_slug': product.get('url_slug', ''),
                            'sku': product.get('sku', ''),
                            'category_data': product,
                            'output_file': self.current_product_details_file,
                            'category_index': i,
                            'total_categories': len(dedup_files),
                            'category_name': filename,
                        }
                        self.product_queue.put(queue_item)

                    # Wait for chunk to be processed
                    while self.product_queue.qsize() > 10:
                        time.sleep(1)

                product_details_logger.info(f"[Category {i}/{len(dedup_files)}] COMPLETED")

            except Exception as e:
                product_details_logger.error(f"Error processing {filename}: {e}")

        # Stop processing
        self._stop_product_scraper()
        profiler.end_session()

        product_details_logger.info("\n" + "=" * 70)
        product_details_logger.info("PRODUCTS ONLY MODE COMPLETE")
        product_details_logger.info(f"Total products processed: {self.products_processed}")
        product_details_logger.info(f"Total records scraped: {self.total_records_scrapped}")
        product_details_logger.info("=" * 70)

    def _update_audit_tables(self, result: Dict):
        """Update audit tables after processing a category."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        filename = result.get('filename', '')
        source_url = result.get('source_url', '')

        # Update category raw audit
        raw_path = os.path.join(Config.CATEGORY_RAW_FOLDER, filename)
        if os.path.exists(raw_path):
            self._append_to_audit_table(Config.CATEGORY_RAW_FOLDER, filename, source_url, raw_path, timestamp)

        # Update category dedup audit
        if self.current_dedup_file and os.path.exists(self.current_dedup_file):
            dedup_filename = os.path.basename(self.current_dedup_file)
            self._append_to_audit_table(Config.CATEGORY_DEDUP_FOLDER, dedup_filename, source_url,
                                        self.current_dedup_file, timestamp)

        # Update product raw audit
        if self.current_product_details_file and os.path.exists(self.current_product_details_file):
            details_filename = os.path.basename(self.current_product_details_file)
            self._append_to_audit_table(Config.PRODUCT_RAW_FOLDER, details_filename, source_url,
                                        self.current_product_details_file, timestamp)

    def _append_to_audit_table(self, folder: str, filename: str, source_url: str, file_path: str, timestamp: str):
        """Append entry to audit table."""
        audit_path = os.path.join(folder, 'audit_table.csv')
        try:
            df = pd.read_csv(file_path)
            new_row = pd.DataFrame([{
                'filename': filename,
                'source_url': source_url,
                'number_of_records': len(df),
                'data_schema': calculate_data_schema(df),
                'status': 'completed',
                'last_updated': timestamp
            }])
            new_row.to_csv(audit_path, mode='a', header=False, index=False, encoding='utf-8')
        except Exception as e:
            category_logger.error(f"Error updating audit table: {e}")

    def _print_summary(self):
        """Print final summary."""
        category_logger.info("\n" + "=" * 70)
        category_logger.info("NOON SCRAPER - COMPLETE")
        category_logger.info("=" * 70)

        total_records = sum(r.get('number_of_records', 0) for r in self.scrape_results)
        successful = sum(1 for r in self.scrape_results if r.get('success'))

        category_logger.info(f"\nCategory Scraper Summary:")
        category_logger.info(f"  - Categories processed: {len(self.scrape_results)}")
        category_logger.info(f"  - Successful: {successful}")
        category_logger.info(f"  - Total records: {total_records}")

        product_details_logger.info(f"\nProduct Scraper Summary:")
        product_details_logger.info(f"  - Products processed: {self.products_processed}")
        product_details_logger.info(f"  - Total records: {self.total_records_scrapped}")

    def _print_category_only_summary(self):
        """Print summary for category only mode."""
        category_logger.info("\n" + "=" * 70)
        category_logger.info("CATEGORY ONLY MODE COMPLETE")
        category_logger.info("=" * 70)

        total_records = sum(r.get('number_of_records', 0) for r in self.scrape_results)
        successful = sum(1 for r in self.scrape_results if r.get('success'))

        category_logger.info(f"\nSummary:")
        category_logger.info(f"  - Categories processed: {len(self.scrape_results)}")
        category_logger.info(f"  - Successful: {successful}")
        category_logger.info(f"  - Total records: {total_records}")
        category_logger.info(f"  - Raw folder: {Config.CATEGORY_RAW_FOLDER}/")
        category_logger.info(f"  - Dedup folder: {Config.CATEGORY_DEDUP_FOLDER}/")
