#!/usr/bin/env python3
"""
Test script for ProductDetailScraper
====================================
Tests the product detail scraping - returns ALL variant rows.
Each row contains: ALL category API fields + ALL product detail fields.
"""

import json
import sys
import pandas as pd
from scrapers.product_scraper import ProductDetailScraper


def get_sample_category_data(url_slug: str, sku: str) -> dict:
    """
    Simulates ALL fields that come from the Category Listing API.
    These are the exact fields extracted by CategoryListScraper.extract_all_attributes()
    """
    return {
        # Basic Info
        'sku': sku,
        'catalog_sku': sku,
        'offer_code': 'abc123',
        'name': 'Cross Strap Flat Bedroom Slippers Brown',
        'brand': 'Joychic',
        'url_slug': url_slug,
        'product_url': f'https://www.noon.com/uae-en/{url_slug}/{sku}/p/?o=abc123',

        # Pricing
        'price': 29.0,
        'sale_price': 19.0,
        'was_price': 35.0,
        'discount_percentage': 34.5,

        # Images (up to 10)
        'image_1': 'https://f.nooncdn.com/p/v1626291744/N49023142V_1.jpg',
        'image_1_key': 'v1626291744/N49023142V_1',
        'image_2': 'https://f.nooncdn.com/p/v1626291744/N49023142V_2.jpg',
        'image_2_key': 'v1626291744/N49023142V_2',
        'image_3': '',
        'image_3_key': '',
        'image_4': '',
        'image_4_key': '',
        'image_5': '',
        'image_5_key': '',
        'image_6': '',
        'image_6_key': '',
        'image_7': '',
        'image_7_key': '',
        'image_8': '',
        'image_8_key': '',
        'image_9': '',
        'image_9_key': '',
        'image_10': '',
        'image_10_key': '',

        # Ratings & Reviews
        'rating': 4.2,
        'reviews': 156,
        'rating_value': 4.2,
        'rating_count': 156,

        # Stock & Availability
        'availability': 'in_stock',
        'is_buyable': True,
        'is_out_of_stock': False,
        'stock_quantity': '',

        # Badges & Flags
        'is_bestseller': False,
        'is_express': True,
        'is_fashion': True,
        'flags': 'express|fashion',
        'flags_count': 2,

        # Deals & Discounts
        'deal_tag_text': '',
        'deal_tag_color': '',
        'discount_tag_code': '',
        'discount_tag_title': '',

        # Nudges
        'nudges': '',
        'nudges_count': 0,
        'nudge_1_text': '',
        'nudge_1_type': '',

        # Delivery
        'delivery_label': 'Get it tomorrow',
        'estimated_delivery_date': '2026-01-26',
        'express_delivery': True,

        # Seller Info
        'seller_code': 'seller123',
        'seller_name': 'Fashion Store',
        'sold_by': 'Fashion Store',

        # Category Info
        'category': 'fashion',
        'subcategory': 'womens-shoes',
        'catalog_key': 'fashion-womens-shoes',

        # Product Specs
        'model_number': 'CLS0219',
        'model_name': 'CLS0219',
        'item_type': 'slippers',

        # Variants
        'has_variants': True,
        'variant_count': 5,

        # Additional Fields
        'position': 1,
        'rank': 100,
        'boost': '',
        'parent_sku': sku,
        'product_type': 'fashion',

        # Sponsor/Ad Info
        'is_sponsored': False,
        'sponsor_id': '',

        # Metadata
        'created_at': '',
        'updated_at': '',
    }


def test_single_product(url_slug: str, sku: str):
    """Test scraping a single product - should return multiple rows."""
    print("\n" + "=" * 70)
    print("Testing ProductDetailScraper - All Variant Rows")
    print("=" * 70)
    print(f"URL Slug: {url_slug}")
    print(f"SKU: {sku}")
    print("=" * 70 + "\n")

    scraper = ProductDetailScraper()

    # Test get_product (raw API response)
    print("[1] Testing get_product() - Raw API response...")
    raw_data = scraper.get_product(url_slug, sku)

    if raw_data:
        print(f"    ✓ Got raw API response")
        print(f"    Top-level keys: {list(raw_data.keys())}")

        if 'product' in raw_data:
            product = raw_data['product']
            variants = product.get('variants', [])
            print(f"    Product has {len(variants)} variants")

        # Save raw response for inspection
        with open('test_raw_response.json', 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, indent=2, ensure_ascii=False)
        print(f"    Saved raw response to: test_raw_response.json")
    else:
        print(f"    ✗ Failed to get raw API response")
        return False

    # Get FULL category data (all fields from category API)
    category_data = get_sample_category_data(url_slug, sku)
    print(f"\n[2] Category API data has {len(category_data)} fields")

    # Test extract_all_product_rows
    print("\n[3] Testing extract_all_product_rows()...")
    all_rows = scraper.extract_all_product_rows(raw_data, category_data)

    if all_rows:
        print(f"    ✓ Extracted {len(all_rows)} variant rows")

        # Show first row structure
        first_row = all_rows[0]
        print(f"\n    First row has {len(first_row)} total fields")

        # Count category vs detail fields
        category_fields = [k for k in first_row.keys() if not k.startswith('detail_')]
        detail_fields = [k for k in first_row.keys() if k.startswith('detail_')]
        print(f"    - Category API fields: {len(category_fields)}")
        print(f"    - Product Detail API fields: {len(detail_fields)}")

        # Show sample category fields
        print(f"\n    Sample Category API fields in output:")
        for key in ['sku', 'name', 'brand', 'price', 'product_url', 'image_1', 'rating', 'availability']:
            val = first_row.get(key, 'N/A')
            if isinstance(val, str) and len(val) > 50:
                val = val[:50] + "..."
            print(f"      - {key}: {val}")

        # Show sample detail fields
        print(f"\n    Sample Product Detail API fields in output:")
        for key in ['detail_product_title', 'detail_variant_sku', 'detail_size', 'detail_price',
                    'detail_stock', 'detail_store_name', 'detail_seller_rating', 'detail_breadcrumbs']:
            val = first_row.get(key, 'N/A')
            if isinstance(val, str) and len(val) > 50:
                val = val[:50] + "..."
            print(f"      - {key}: {val}")

        # Save all rows as CSV
        df = pd.DataFrame(all_rows)
        df.to_csv('test_all_variants.csv', index=False, encoding='utf-8')
        print(f"\n    Saved all {len(all_rows)} rows to: test_all_variants.csv")
        print(f"    Total columns: {len(df.columns)}")

        # Also save column list
        with open('test_columns.txt', 'w') as f:
            f.write("=== CATEGORY API FIELDS ===\n")
            for col in sorted(category_fields):
                f.write(f"{col}\n")
            f.write("\n=== PRODUCT DETAIL API FIELDS ===\n")
            for col in sorted(detail_fields):
                f.write(f"{col}\n")
        print(f"    Saved column list to: test_columns.txt")
    else:
        print(f"    ✗ Failed to extract product rows")
        return False

    print("\n" + "=" * 70)
    print(f"SUCCESS! Generated {len(all_rows)} rows from 1 product")
    print(f"Each row contains:")
    print(f"  - {len(category_fields)} fields from Category API")
    print(f"  - {len(detail_fields)} fields from Product Detail API")
    print(f"  - {len(first_row)} total fields")
    print("=" * 70 + "\n")
    return True


def main():
    # Default test product (same as product_details.py)
    default_url_slug = "cross-strap-flat-bedroom-slippers-brown"
    default_sku = "N49023142V"

    if len(sys.argv) >= 3:
        url_slug = sys.argv[1]
        sku = sys.argv[2]
    else:
        url_slug = default_url_slug
        sku = default_sku
        print("Usage: python test_product_scraper.py <url_slug> <sku>")
        print(f"Using default test product: {sku}\n")

    success = test_single_product(url_slug, sku)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
