"""
Settings Module
===============
Loads configuration from .env file for easy cookie/config updates.
"""

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration class that loads all settings from .env"""

    # Visitor IDs (update in .env when expired)
    VISITOR_ID: str = os.getenv('VISITOR_ID', '8b76a0b9-1549-483d-b87b-2982d89f75a1')
    VISITOR_ID_ALT: str = os.getenv('VISITOR_ID_ALT', '18329ffc-072d-4f43-b878-1674a68b278c')

    # Locale and Region
    LOCALE: str = os.getenv('LOCALE', 'en-ae')
    REGION: str = os.getenv('REGION', 'ecom')
    COUNTRY: str = os.getenv('COUNTRY', 'ae')

    # Location Headers
    LAT: str = os.getenv('LAT', '251998495')
    LNG: str = os.getenv('LNG', '552715985')
    ZONE_CODE: str = os.getenv('ZONE_CODE', 'AE_DXB-S14')
    ROCKET_ZONE_CODE: str = os.getenv('ROCKET_ZONE_CODE', 'W00068765A')

    # Scraper Settings
    BATCH_SIZE: int = int(os.getenv('BATCH_SIZE', '500'))
    REQUEST_DELAY_MIN: float = float(os.getenv('REQUEST_DELAY_MIN', '2'))
    REQUEST_DELAY_MAX: float = float(os.getenv('REQUEST_DELAY_MAX', '4'))
    REQUEST_TIMEOUT: int = int(os.getenv('REQUEST_TIMEOUT', '20'))

    # Chrome Impersonation
    CHROME_IMPERSONATE: str = os.getenv('CHROME_IMPERSONATE', 'chrome110')
    CHROME_IMPERSONATE_DETAIL: str = os.getenv('CHROME_IMPERSONATE_DETAIL', 'chrome120')

    # Output Folders
    RAW_OUTPUT_FOLDER: str = os.getenv('RAW_OUTPUT_FOLDER', 'noon_products_scrapped_curl_cffi')
    DEDUP_OUTPUT_FOLDER: str = os.getenv('DEDUP_OUTPUT_FOLDER', 'noon_products_scrapped_curl_cffi_dedup')
    PRODUCT_DETAILS_FOLDER: str = os.getenv('PRODUCT_DETAILS_FOLDER', 'noon_product_details')

    # Input/Output Files
    INPUT_CSV: str = os.getenv('INPUT_CSV', 'categories_to_scrape.csv')
    LOG_FILE: str = os.getenv('LOG_FILE', 'scraper_profiling.log')

    # API Base URLs
    BASE_API_URL: str = "https://www.noon.com/_vs/nc/mp-customer-catalog-api/api/v3/u/"
    BASE_SITE_URL: str = "https://www.noon.com/uae-en/"

    @classmethod
    def get_base_cookies(cls) -> dict:
        """Get base cookies dictionary."""
        return {
            'visitor_id': cls.VISITOR_ID,
            'visitorId': cls.VISITOR_ID_ALT,
            'x-available-ae': cls.REGION,
            'nloc': cls.LOCALE,
        }

    @classmethod
    def get_request_headers(cls, referer: str = '') -> dict:
        """Get standard request headers."""
        headers = {
            'accept': 'application/json, text/plain, */*',
            'accept-language': 'en-US,en;q=0.9',
            'cache-control': 'no-cache, max-age=0, must-revalidate, no-store',
            'sec-ch-ua': '"Google Chrome";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Linux"',
            'sec-fetch-dest': 'empty',
            'sec-fetch-mode': 'cors',
            'sec-fetch-site': 'same-origin',
            'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36',
            'x-locale': cls.LOCALE,
            'x-mp-country': cls.COUNTRY,
            'x-platform': 'web',
            'x-lat': cls.LAT,
            'x-lng': cls.LNG,
            'x-ecom-zonecode': cls.ZONE_CODE,
            'x-rocket-enabled': 'true',
            'x-rocket-zonecode': cls.ROCKET_ZONE_CODE,
            'x-border-enabled': 'true',
            'x-visitor-id': cls.VISITOR_ID,
        }

        if referer:
            headers['referer'] = referer

        return headers

    @classmethod
    def get_delay_range(cls) -> tuple:
        """Get delay range tuple."""
        return (cls.REQUEST_DELAY_MIN, cls.REQUEST_DELAY_MAX)

    @classmethod
    def print_config(cls):
        """Print current configuration for debugging."""
        print("\n" + "=" * 50)
        print("CURRENT CONFIGURATION")
        print("=" * 50)
        print(f"VISITOR_ID: {cls.VISITOR_ID}")
        print(f"VISITOR_ID_ALT: {cls.VISITOR_ID_ALT}")
        print(f"LOCALE: {cls.LOCALE}")
        print(f"COUNTRY: {cls.COUNTRY}")
        print(f"ZONE_CODE: {cls.ZONE_CODE}")
        print(f"BATCH_SIZE: {cls.BATCH_SIZE}")
        print(f"REQUEST_TIMEOUT: {cls.REQUEST_TIMEOUT}s")
        print(f"DELAY: {cls.REQUEST_DELAY_MIN}-{cls.REQUEST_DELAY_MAX}s")
        print(f"RAW_OUTPUT_FOLDER: {cls.RAW_OUTPUT_FOLDER}")
        print(f"DEDUP_OUTPUT_FOLDER: {cls.DEDUP_OUTPUT_FOLDER}")
        print(f"PRODUCT_DETAILS_FOLDER: {cls.PRODUCT_DETAILS_FOLDER}")
        print("=" * 50 + "\n")
