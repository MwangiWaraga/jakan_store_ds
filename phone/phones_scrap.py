import time
import random
import datetime
from typing import List, Dict

import cloudscraper
from bs4 import BeautifulSoup
from google.cloud import bigquery

# Assumptions:
# - Install required packages: pip install cloudscraper beautifulsoup4 google-cloud-bigquery lxml
# - Set up Google Cloud credentials: export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
# - BigQuery table must exist with the schema: brand (STRING), title (STRING), price_clean (INTEGER), stock_status (STRING), specs_content (STRING), scraped_at (TIMESTAMP)
# - Image URL is extracted but not uploaded as per the specified schema

def get_page(scraper, url: str, retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            response = scraper.get(url)
            if response.status_code == 200:
                return response.text
            print(f"Attempt {attempt+1}: Status code {response.status_code} for {url}")
        except Exception as e:
            print(f"Attempt {attempt+1}: Error fetching {url}: {e}")
        time.sleep(random.uniform(2, 5))  # Exponential backoff-like delay
    return None

def extract_product_urls(scraper, base_url: str, category_path: str) -> List[str]:
    product_urls = []
    current_url = base_url + category_path
    while True:
        html = get_page(scraper, current_url)
        if not html:
            break
        soup = BeautifulSoup(html, 'lxml')
        grid = soup.find('ul', class_='products')
        if not grid:
            break
        for li in grid.find_all('li', class_='product'):
            a = li.find('a', class_='woocommerce-loop-product__link')
            if a:
                product_urls.append(a['href'])
        next_link = soup.find('a', {'class': 'next'})
        if next_link:
            current_url = next_link['href']
        else:
            break
        time.sleep(random.uniform(1, 3))  # Polite delay between pages
    return product_urls

def extract_product_data(scraper, product_url: str, brand: str) -> Dict | None:
    html = get_page(scraper, product_url)
    if not html:
        return None
    soup = BeautifulSoup(html, 'lxml')
    
    # Title
    title_elem = soup.find('h1', class_='product_title')
    title = title_elem.text.strip() if title_elem else ''
    
    # Price
    price_elem = soup.find('p', class_='price')
    if price_elem:
        ins = price_elem.find('ins')
        amount_span = (ins or price_elem).find('span', class_='woocommerce-Price-amount')
        price_text = amount_span.text.strip() if amount_span else ''
        price_clean_str = price_text.replace('KSh', '').replace(',', '').replace(' ', '').strip()
        try:
            price_clean = int(price_clean_str)
        except ValueError:
            price_clean = None
    else:
        price_clean = None
    
    # Stock Status
    stock_elem = soup.find('p', class_='stock')
    if stock_elem:
        classes = stock_elem.get('class', [])
        if 'out-of-stock' in classes:
            stock_status = 'Sold Out'
        elif 'in-stock' in classes:
            stock_status = 'In Stock'
        else:
            stock_status = stock_elem.text.strip()
    else:
        # Fallback: check for add to cart button or other indicators
        add_to_cart = soup.find('button', {'name': 'add-to-cart'})
        stock_status = 'In Stock' if add_to_cart and not add_to_cart.get('disabled') else 'Sold Out'
    
    # Specs
    short_desc = soup.find('div', class_='woocommerce-product-details__short-description')
    specs_content = ''
    if short_desc:
        lis = short_desc.find_all('li')
        specs_content = ' | '.join(li.text.strip() for li in lis)
    
    # Image URL (extracted but not used in upload)
    image_div = soup.find('div', class_='woocommerce-product-gallery__image')
    image_url = image_div.find('a')['href'] if image_div and image_div.find('a') else ''
    print(f"Extracted image URL for {title}: {image_url}")  # For logging
    
    # Scraped at
    scraped_at = datetime.datetime.now(datetime.timezone.utc)
    
    if not title or price_clean is None:
        return None  # Skip invalid data
    
    return {
        'brand': brand,
        'title': title,
        'price_clean': price_clean,
        'stock_status': stock_status,
        'specs_content': specs_content,
        'scraped_at': scraped_at
    }

def main():
    base_url = 'https://www.phoneplacekenya.com'
    categories = {
        '/product-category/smartphones/tecno-phones/': 'Tecno',
        '/product-category/smartphones/infinix-phones-in-kenya/': 'Infinix',
        '/product-category/smartphones/itel/': 'Itel'
    }
    
    scraper = cloudscraper.create_scraper()  # Bypasses Cloudflare
    
    all_data = []
    for cat_path, brand in categories.items():
        print(f"Scraping category: {brand}")
        product_urls = extract_product_urls(scraper, base_url, cat_path)
        for url in product_urls:
            data = extract_product_data(scraper, url, brand)
            if data:
                all_data.append(data)
            time.sleep(random.uniform(1, 3))  # Polite delay between products
    
    if not all_data:
        print("No data scraped.")
        return
    
    # Upload to BigQuery
    client = bigquery.Client()
    table_id = 'your-project.your_dataset.your_table'  # Replace with your BigQuery table ID
    
    # Batch insert (BigQuery handles up to 10,000 rows per insert, but for safety, chunk if needed)
    errors = client.insert_rows_json(table_id, all_data)
    if errors:
        print(f"Errors occurred while inserting rows: {errors}")
    else:
        print(f"Successfully inserted {len(all_data)} rows.")

if __name__ == '__main__':
    main()