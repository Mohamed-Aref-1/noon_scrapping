"""
Category List Scraper Module
============================
Scrapes products from category listing API.
"""

import os
import time
import random
import pandas as pd
from typing import Dict, List, Optional, Tuple, Callable
from curl_cffi import requests

from config import Config
from utils import (
    logger,
    profile_step,
    profile_function,
    extract_filename_from_url,
    extract_category_path_from_url,
    reorder_dataframe_columns,
    image_key_to_url
)


class CategoryListScraper:
    """
    Scraper for category listing API.
    Gets product data from the category/search pages.
    """

    def __init__(self):
        self.base_url = Config.BASE_API_URL
        self.session = requests.Session()
        self.visitor_id = Config.VISITOR_ID
        self.base_cookies = Config.get_base_cookies()

        logger.info("CategoryListScraper initialized")
        logger.debug(f"Using VISITOR_ID: {self.visitor_id}")

    @profile_function
    def get_fresh_session(self) -> bool:
        """Establish fresh session with cookies."""
        try:
            logger.info("Getting fresh session...")

            with profile_step("HTTP request: session init"):
                self.session.get(
                    Config.BASE_SITE_URL,
                    headers={
                        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                    },
                    cookies=self.base_cookies,
                    impersonate=Config.CHROME_IMPERSONATE,
                    timeout=15
                )

            logger.info(f"Session established with {len(self.session.cookies)} cookies")
            time.sleep(2)
            return True

        except Exception as e:
            logger.warning(f"Session failed: {e}")
            return False

    def build_product_url(self, url_slug: str, sku: str, offer_code: str) -> str:
        """Build full product URL."""
        if url_slug and sku:
            return f"{Config.BASE_SITE_URL}{url_slug}/{sku}/p/?o={offer_code}"
        return ''

    def extract_all_attributes(self, product: Dict) -> Dict:
        """
        Extract ALL attributes from a product in the category listing.
        """
        flat = {}

        # Basic Info
        flat['sku'] = product.get('sku', '')
        flat['catalog_sku'] = product.get('catalog_sku', '')
        flat['offer_code'] = product.get('offer_code', '')
        flat['name'] = product.get('name', '')
        flat['brand'] = product.get('brand', '')
        flat['url_slug'] = product.get('url', '')

        # Build full product URL
        flat['product_url'] = self.build_product_url(
            flat['url_slug'],
            flat['sku'],
            flat['offer_code']
        )

        # Pricing
        flat['price'] = product.get('price', '')
        flat['sale_price'] = product.get('sale_price', '')
        flat['was_price'] = product.get('was_price', '')

        # Calculate discount percentage
        if flat['price'] and flat['sale_price']:
            try:
                discount = ((flat['price'] - flat['sale_price']) / flat['price']) * 100
                flat['discount_percentage'] = round(discount, 1)
            except Exception:
                flat['discount_percentage'] = ''
        else:
            flat['discount_percentage'] = ''

        # Images (up to 10)
        image_keys = product.get('image_keys', [])
        image_key = product.get('image_key', '')

        all_image_keys = []
        if image_keys:
            all_image_keys.extend(image_keys)
        elif image_key:
            all_image_keys.append(image_key)

        for i in range(10):
            if i < len(all_image_keys):
                img_key = all_image_keys[i]
                flat[f'image_{i+1}'] = image_key_to_url(img_key)
                flat[f'image_{i+1}_key'] = img_key
            else:
                flat[f'image_{i+1}'] = ''
                flat[f'image_{i+1}_key'] = ''

        # Ratings & Reviews
        flat['rating'] = product.get('rating', '')
        flat['reviews'] = product.get('reviews', '')

        product_rating = product.get('product_rating', {})
        if product_rating:
            flat['rating_value'] = product_rating.get('value', '')
            flat['rating_count'] = product_rating.get('count', '')
        else:
            flat['rating_value'] = ''
            flat['rating_count'] = ''

        # Stock & Availability
        flat['availability'] = product.get('availability', '')
        flat['is_buyable'] = product.get('is_buyable', '')
        flat['is_out_of_stock'] = product.get('is_out_of_stock', '')
        flat['stock_quantity'] = product.get('stock_quantity', '')

        # Badges & Flags
        flat['is_bestseller'] = product.get('is_bestseller', '')
        flat['is_express'] = product.get('is_express', '')
        flat['is_fashion'] = product.get('is_fashion', '')

        flags = product.get('flags', [])
        flat['flags'] = '|'.join(flags) if flags else ''
        flat['flags_count'] = len(flags)

        # Deals & Discounts
        deal_tag = product.get('deal_tag', {})
        if deal_tag:
            flat['deal_tag_text'] = deal_tag.get('text', '')
            flat['deal_tag_color'] = deal_tag.get('color', '')
        else:
            flat['deal_tag_text'] = ''
            flat['deal_tag_color'] = ''

        flat['discount_tag_code'] = product.get('discount_tag_code', '')
        flat['discount_tag_title'] = product.get('discount_tag_title', '')

        # Nudges
        nudges = product.get('nudges', [])
        if nudges:
            nudge_texts = [n.get('text', '') for n in nudges if n.get('text')]
            flat['nudges'] = '|'.join(nudge_texts)
            flat['nudges_count'] = len(nudges)

            if len(nudges) > 0:
                flat['nudge_1_text'] = nudges[0].get('text', '')
                flat['nudge_1_type'] = nudges[0].get('type', '')
        else:
            flat['nudges'] = ''
            flat['nudges_count'] = 0
            flat['nudge_1_text'] = ''
            flat['nudge_1_type'] = ''

        # Delivery
        flat['delivery_label'] = product.get('delivery_label', '')
        flat['estimated_delivery_date'] = product.get('estimated_delivery_date', '')
        flat['express_delivery'] = product.get('express_delivery', '')

        # Seller Info
        flat['seller_code'] = product.get('seller_code', '')
        flat['seller_name'] = product.get('seller_name', '')
        flat['sold_by'] = product.get('sold_by', '')

        # Category Info
        flat['category'] = product.get('category', '')
        flat['subcategory'] = product.get('subcategory', '')
        flat['catalog_key'] = product.get('catalog_key', '')

        # Product Specs
        flat['model_number'] = product.get('model_number', '')
        flat['model_name'] = product.get('model_name', '')
        flat['item_type'] = product.get('item_type', '')

        # Attributes (variants)
        attributes = product.get('attributes', [])
        if attributes:
            for attr in attributes:
                attr_name = attr.get('name', '').lower().replace(' ', '_')
                attr_value = attr.get('value', '')
                if attr_name and attr_value:
                    flat[f'attr_{attr_name}'] = attr_value

        # Variants
        variants = product.get('variants', [])
        flat['has_variants'] = len(variants) > 0
        flat['variant_count'] = len(variants)

        # Additional Fields
        flat['position'] = product.get('position', '')
        flat['rank'] = product.get('rank', '')
        flat['boost'] = product.get('boost', '')
        flat['parent_sku'] = product.get('parent_sku', '')
        flat['product_type'] = product.get('product_type', '')

        # Sponsor/Ad Info
        flat['is_sponsored'] = product.get('is_sponsored', '')
        flat['sponsor_id'] = product.get('sponsor_id', '')

        # Metadata
        flat['created_at'] = product.get('created_at', '')
        flat['updated_at'] = product.get('updated_at', '')

        # Catch remaining fields
        for key, value in product.items():
            if key not in flat and not isinstance(value, (dict, list)):
                flat[f'extra_{key}'] = value

        return flat

    def scrape_page(self, category_path: str, page: int) -> Dict:
        """Scrape a single page."""
        referer = f"{Config.BASE_SITE_URL}{category_path}?page={page}"
        headers = Config.get_request_headers(referer)

        try:
            with profile_step(f"HTTP request: page {page}"):
                response = self.session.get(
                    f"{self.base_url}{category_path}",
                    params={'page': str(page)},
                    headers=headers,
                    impersonate=Config.CHROME_IMPERSONATE,
                    timeout=Config.REQUEST_TIMEOUT
                )

            if response.status_code != 200:
                return {
                    'success': False,
                    'error': f'HTTP {response.status_code}',
                    'page': page
                }

            with profile_step(f"JSON parse: page {page}"):
                data = response.json()

            with profile_step(f"Extract attributes: page {page}"):
                products = []
                for hit in data.get('hits', []):
                    product = self.extract_all_attributes(hit)
                    products.append(product)

            return {
                'success': True,
                'page': page,
                'products': products,
                'total_hits': data.get('nbHits', 0),
                'total_pages': data.get('nbPages', 0),
            }

        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'page': page
            }

    def scrape_category(
        self,
        category_url: str,
        output_folder: str,
        max_pages: Optional[int] = None,
        delay: Optional[Tuple[float, float]] = None,
        on_batch_written: Optional[Callable[[List[Dict], str], None]] = None
    ) -> Dict:
        """
        Scrape a category and save to CSV with batch writing.

        Returns dict with filename, record_count, and success status.
        """
        if delay is None:
            delay = Config.get_delay_range()

        category_path = extract_category_path_from_url(category_url)
        filename = extract_filename_from_url(category_url)
        output_path = os.path.join(output_folder, filename)

        logger.info("=" * 70)
        logger.info(f"Scraping: {category_url}")
        logger.info(f"Output: {output_path}")
        logger.debug(f"Category path: {category_path}")
        logger.info("=" * 70)

        # Get fresh session
        self.get_fresh_session()

        # First request to get total pages
        logger.info("Analyzing category...")

        with profile_step("First page analysis"):
            first_page = self.scrape_page(category_path, 1)

        if not first_page['success']:
            logger.error(f"Failed: {first_page.get('error')}")
            return {
                'success': False,
                'filename': filename,
                'source_url': category_url,
                'number_of_records': 0,
                'error': first_page.get('error')
            }

        total_pages = first_page['total_pages']
        total_products_expected = first_page['total_hits']

        if max_pages:
            total_pages = min(total_pages, max_pages)

        logger.info(f"Found {total_products_expected} products across {total_pages} pages")
        logger.debug(f"Estimated scrape time: {total_pages * 3}s")

        # Initialize batch tracking
        batch_products = first_page['products']
        total_products_scraped = len(batch_products)
        failed_pages = []
        header_written = False

        # Check if we need to write first batch
        if len(batch_products) >= Config.BATCH_SIZE:
            with profile_step("Write batch to CSV"):
                self._write_batch(batch_products, output_path, write_header=True)
            header_written = True
            # Call the batch callback if provided
            if on_batch_written:
                on_batch_written(batch_products.copy(), output_path)
            batch_products = []
            logger.debug(f"Initial batch written ({Config.BATCH_SIZE} products)")

        logger.info(f"Page 1/{total_pages} - {len(first_page['products'])} products (Total: {total_products_scraped})")

        # Scrape remaining pages
        for page in range(2, total_pages + 1):
            # Random delay
            wait = random.uniform(*delay)
            logger.debug(f"Waiting {wait:.2f}s before page {page}")
            time.sleep(wait)

            result = self.scrape_page(category_path, page)

            if result['success']:
                batch_products.extend(result['products'])
                total_products_scraped += len(result['products'])
                logger.info(f"Page {page}/{total_pages} - {len(result['products'])} products (Total: {total_products_scraped})")

                # Write batch if threshold reached
                if len(batch_products) >= Config.BATCH_SIZE:
                    with profile_step("Write batch to CSV"):
                        self._write_batch(batch_products, output_path, write_header=not header_written)
                    header_written = True
                    # Call the batch callback if provided
                    if on_batch_written:
                        on_batch_written(batch_products.copy(), output_path)
                    batch_products = []
                    logger.info(f"Batch written to disk ({Config.BATCH_SIZE} products)")
            else:
                failed_pages.append(page)
                logger.error(f"Page {page}/{total_pages} - Failed: {result.get('error')}")

        # Write remaining products
        if batch_products:
            with profile_step("Write final batch to CSV"):
                self._write_batch(batch_products, output_path, write_header=not header_written)
            # Call the batch callback if provided
            if on_batch_written:
                on_batch_written(batch_products.copy(), output_path)
            logger.info(f"Final batch written ({len(batch_products)} products)")

        logger.info("=" * 70)
        logger.info(f"COMPLETE: {total_products_scraped} products scraped")
        if failed_pages:
            logger.warning(f"Failed pages: {failed_pages}")
        logger.info("=" * 70)

        return {
            'success': True,
            'filename': filename,
            'source_url': category_url,
            'number_of_records': total_products_scraped,
            'failed_pages': failed_pages
        }

    def _sanitize_for_csv(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Sanitize DataFrame values for CSV writing.
        Removes/replaces characters that can break CSV parsing.
        """
        for col in df.columns:
            if df[col].dtype == 'object':  # String columns
                df[col] = df[col].apply(lambda x: self._sanitize_value(x) if isinstance(x, str) else x)
        return df

    def _sanitize_value(self, value: str) -> str:
        """Sanitize a single string value for CSV."""
        if not value:
            return value
        # Replace newlines and carriage returns with spaces
        value = value.replace('\n', ' ').replace('\r', ' ')
        # Replace tabs with spaces
        value = value.replace('\t', ' ')
        # Remove null bytes
        value = value.replace('\x00', '')
        # Normalize multiple spaces to single space
        while '  ' in value:
            value = value.replace('  ', ' ')
        return value.strip()

    def _write_batch(self, products: List[Dict], output_path: str, write_header: bool):
        """Write a batch of products to CSV file."""
        if not products:
            return

        df = pd.DataFrame(products)
        df = reorder_dataframe_columns(df)

        # Sanitize all string values to prevent CSV parsing issues
        df = self._sanitize_for_csv(df)

        mode = 'w' if write_header else 'a'
        # Use QUOTE_ALL (quoting=1) to properly escape commas in fields
        df.to_csv(
            output_path,
            mode=mode,
            header=write_header,
            index=False,
            encoding='utf-8',
            quoting=1  # csv.QUOTE_ALL - prevents field count mismatch due to unescaped commas
        )

        logger.debug(f"Wrote {len(products)} products to {output_path} (header={write_header})")
