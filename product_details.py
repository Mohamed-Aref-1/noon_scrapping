from curl_cffi import requests
import json
import pandas as pd
import time

def get_product(url_slug, sku):
    session = requests.Session()
    
    page_url = f'https://www.noon.com/uae-en/{url_slug}/{sku}/p/'
    session.get(page_url, impersonate="chrome120")
    
    headers = {
        'accept': 'application/json',
        'x-locale': 'en-ae',
        'x-mp-country': 'ae',
        'referer': page_url,
    }
    
    api_url = f'https://www.noon.com/_vs/nc/mp-customer-catalog-api/api/v3/u/{url_slug}/{sku}/p/'
    r = session.get(api_url, headers=headers, impersonate="chrome120")
    
    if r.status_code == 200:
        return r.json()
    return None

def image_key_to_url(image_key):
    if not image_key:
        return None
    # Works for both v....../SKU_1 and pzsku/... formats seen in your data
    return f"https://f.nooncdn.com/p/{image_key}.jpg"

def extract_all_product_data(data):
    products = []
    
    product = data['product']
    
    # Get specifications
    specs = {}
    for spec in product.get('specifications', []):
        specs[spec['code']] = spec['value']
    
    breadcrumb_path = ' > '.join([b['name'] for b in product.get('breadcrumbs', [])])
    features = ' | '.join(product.get('feature_bullets', []))
    images = product.get('image_keys', [])
    
    # Extract color variants ── now with full https image_url per color
    color_variants = []
    groups = product.get('groups', [])
    for group in groups:
        if group.get('code') == 'color':
            for option in group.get('options', []):
                color_variants.append({
                    'color': option.get('name'),
                    'sku': option.get('sku'),
                    'available': option.get('is_available') == 1,
                    'url': option.get('url'),
                    'image_url': image_key_to_url(option.get('image_key')),  # ← added
                })
    
    # Extract Frequently Bought Together ── already includes image_url
    fbt_products = []
    for fbt in product.get('fbt_offers', []):
        fbt_products.append({
            'sku': fbt.get('sku'),
            'title': fbt.get('title'),
            'brand': fbt.get('brand'),
            'price': fbt.get('price'),
            'sale_price': fbt.get('sale_price'),
            'image_url': image_key_to_url(fbt.get('image_key')),  # full https url
        })
    
    base_info = {
        'config_sku': product['sku'],
        'product_title': product['product_title'],
        'brand': product.get('brand'),
        'feature_bullets': features,
        'breadcrumbs': breadcrumb_path,
        'category_code': product.get('category_code'),
        'all_specifications_json': json.dumps(specs),
        'image_count': len(images),
        'image_1': image_key_to_url(images[0]) if len(images) > 0 else None,
        'image_2': image_key_to_url(images[1]) if len(images) > 1 else None,
        'image_3': image_key_to_url(images[2]) if len(images) > 2 else None,
        'image_4': image_key_to_url(images[3]) if len(images) > 3 else None,
        'image_5': image_key_to_url(images[4]) if len(images) > 4 else None,
        'all_images_json': json.dumps([image_key_to_url(img) for img in images]),
        'brand_rating': product.get('brand_rating', {}).get('value'),
        'is_collection_eligible': product.get('is_collection_eligible'),
        
        # Color variants (now with image_url inside each item)
        'available_colors': ', '.join([c['color'] for c in color_variants]),
        'color_variants_json': json.dumps(color_variants),
        
        # Frequently Bought Together (with image_url inside each item)
        'fbt_count': len(fbt_products),
        'fbt_products_json': json.dumps(fbt_products),
    }
    
    for variant in product.get('variants', []):
        variant_sku = variant.get('sku')
        variant_size = variant.get('variant')
        
        offers = variant.get('offers', [])
        
        if not offers:
            variant_row = base_info.copy()
            variant_row.update({'variant_sku': variant_sku, 'size': variant_size})
            products.append(variant_row)
            continue
        
        for offer in offers:
            variant_row = base_info.copy()
            
            variant_row['variant_sku'] = variant_sku
            variant_row['size'] = variant_size
            variant_row['offer_code'] = offer.get('offer_code')
            variant_row['offer_sku'] = offer.get('sku')
            variant_row['price'] = offer.get('price')
            variant_row['currency'] = offer.get('currency', 'AED')
            variant_row['sale_price'] = offer.get('sale_price')
            variant_row['stock'] = offer.get('stock')
            variant_row['is_buyable'] = offer.get('is_buyable')
            variant_row['is_bestseller'] = offer.get('is_bestseller')
            variant_row['store_name'] = offer.get('store_name')
            variant_row['partner_code'] = offer.get('partner_code')
            
            seller_ratings = offer.get('partner_ratings_sellerlab', {})
            variant_row['seller_rating'] = seller_ratings.get('partner_rating')
            variant_row['seller_rating_count'] = seller_ratings.get('num_of_rating')
            variant_row['seller_positive_rating'] = seller_ratings.get('positive_seller_rating')
            variant_row['seller_as_described_rate'] = seller_ratings.get('as_described_rate')
            
            variant_row['estimated_delivery'] = offer.get('estimated_delivery')
            variant_row['estimated_delivery_date'] = offer.get('estimated_delivery_date')
            variant_row['shipping_fee_message'] = offer.get('shipping_fee_message')
            
            flags = offer.get('flags', [])
            variant_row['is_marketplace'] = 'marketplace' in flags
            variant_row['is_global'] = 'global' in flags
            variant_row['is_free_delivery'] = 'free_delivery_eligible' in flags
            variant_row['flags_json'] = json.dumps(flags)
            variant_row['bnpl_available'] = bool(offer.get('bnplBanners', []))
            variant_row['cashback_available'] = bool(offer.get('cobrand_cashback_data', {}))
            
            products.append(variant_row)
    
    return products

# Test
if __name__ == "__main__":
    sku = "N49023142V"
    data = get_product("cross-strap-flat-bedroom-slippers-brown", sku)
    
    if data:
        # Save full JSON response
        with open(f'product_response_{sku}.json', 'w') as f:
            json.dump(data, f, indent=2)
        print(f"✓ Saved JSON")
        
        # Extract and save to CSV
        products = extract_all_product_data(data)
        df = pd.DataFrame(products)
        
        print(f"Rows: {len(products)}")
        print(f"Columns: {len(df.columns)}")
        
        df.to_csv(f'noon_product_{sku}.csv', index=False)
        print(f"✓ Saved CSV")