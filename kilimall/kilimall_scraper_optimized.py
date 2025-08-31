# kilimall_scraper_optimized.py
# OPTIMIZED Kilimall scraper - Uses best pagination strategies to get maximum products
# Based on analysis: Gets 80%+ of all products with minimal requests

import os, time, random, logging
from typing import List, Dict, Set
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlencode, urlparse, parse_qsl, urlunparse

# ── CONFIG ───────────────────────────────────────────────────────────────
BASE_URL = "https://www.kilimall.co.ke"
STORES = [
    {"name": "JAKAN PHONE STORE", "path": "/store/JAKAN-PHONE-STORE"},
    # Add more stores here
]
SHEET_ID  = "18QRcbrEq2T-iaNQICu535J2u_cPFzQxCY-GRcDMt49o"
SHEET_TAB = "kilimall_optimized"

REQUEST_TIMEOUT = 20
RETRY_COUNT = 3
REQUEST_DELAY_RANGE = (1.0, 2.0)

# Optimized strategies based on analysis - only the most effective ones
PAGINATION_STRATEGIES = [
    # Strategy name, URL pattern, max pages
    ("pageNo", "?pageNo={}", 4),
    ("price_desc", "?sort=price_desc&page={}", 3), 
    ("sales", "?sort=sales&page={}", 3),
    ("pageNum", "?pageNum={}", 4),
    ("offset", "?offset={}", [0, 32, 64, 96]),  # Special case: use list
]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Nairobi time
try:
    from zoneinfo import ZoneInfo
    NAIR_OBS = ZoneInfo("Africa/Nairobi")
except Exception:
    from datetime import timezone, timedelta
    NAIR_OBS = timezone(timedelta(hours=3))

HEADER = ["ts", "store", "product_url", "title", "price"]

# ── SHEETS ───────────────────────────────────────────────────────────────
import gspread
from google.oauth2.service_account import Credentials

def get_sheets_client():
    scopes = ["https://www.googleapis.com/auth/spreadsheets","https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=scopes)
    return gspread.authorize(creds)

def ensure_worksheet(sh):
    try:
        ws = sh.worksheet(SHEET_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_TAB, rows=1000, cols=len(HEADER)+2)
        ws.append_row(HEADER, value_input_option="RAW")
        return ws
    if not ws.row_values(1): 
        ws.append_row(HEADER, value_input_option="RAW")
    return ws

def append_rows(ws, rows: List[List]):
    if not rows: return
    try: 
        ws.append_rows(rows, value_input_option="RAW")
    except Exception:
        for r in rows: 
            ws.append_row(r, value_input_option="RAW")

# ── UTILS ────────────────────────────────────────────────────────────────
def ts_now_iso():
    from datetime import datetime
    return datetime.now(NAIR_OBS).strftime("%Y-%m-%d %H:%M:%S")

def sleep_politely(): 
    time.sleep(random.uniform(*REQUEST_DELAY_RANGE))

def absolute_url(href: str) -> str: 
    return urljoin(BASE_URL, href or "")

def fetch(url: str) -> str:
    """Fetch with retries"""
    for attempt in range(RETRY_COUNT):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.text
            if 400 <= resp.status_code < 500:
                return ""  # Client error, don't retry
        except requests.RequestException as ex:
            logging.warning(f"Fetch error (attempt {attempt+1}): {ex}")
        if attempt < RETRY_COUNT - 1:
            sleep_politely()
    return ""

def parse_products(html: str) -> List[Dict]:
    """Extract products from HTML"""
    if not html:
        return []
        
    soup = BeautifulSoup(html, "html.parser")
    products = []
    
    # Find product links
    for link in soup.select('a[href^="/listing/"]'):
        href = link.get('href', '').strip()
        if not href:
            continue
            
        product_url = absolute_url(href)
        
        # Find product container (walk up the tree)
        container = link
        for _ in range(5):
            if not container.parent:
                break
            container = container.parent
            # Look for common product container indicators
            classes = ' '.join(container.get('class', [])).lower()
            if any(indicator in classes for indicator in ['product', 'item', 'listing', 'goods']):
                break
        
        # Extract title
        title = ""
        # Try title attribute first
        title = link.get('title', '') or link.get('aria-label', '')
        # Try text content
        if not title:
            title = link.get_text(strip=True)
        # Try nearby title elements
        if not title:
            title_elem = container.select_one('.product-title, .title, [class*=title], [class*=name]')
            if title_elem:
                title = title_elem.get_text(strip=True)
        
        # Extract price
        price = ""
        price_elem = container.select_one('.product-price, .price, [class*=price]')
        if price_elem:
            price = price_elem.get_text(strip=True)
        
        if title:  # Only add if we have a title
            products.append({
                "product_url": product_url,
                "title": title,
                "price": price
            })
    
    return products

# ── MAIN SCRAPER ─────────────────────────────────────────────────────────
def scrape_store_optimized(store_name: str, store_path: str, ts: str) -> List[List]:
    """Optimized scraper using best strategies"""
    store_url = absolute_url(store_path)
    logging.info(f"[{store_name}] Starting optimized scrape")
    
    all_products: Set[str] = set()  # Track by URL to avoid duplicates
    rows: List[List] = []
    
    for strategy_name, url_pattern, max_pages_or_values in PAGINATION_STRATEGIES:
        logging.info(f"[{store_name}] Strategy: {strategy_name}")
        
        if isinstance(max_pages_or_values, list):
            # Special case: offset strategy with predefined values
            values = max_pages_or_values
        else:
            # Regular pagination: 1, 2, 3, ...
            values = list(range(1, max_pages_or_values + 1))
        
        strategy_found = 0
        
        for value in values:
            if strategy_name == "offset":
                url = store_url + url_pattern.format(value)
            else:
                url = store_url + url_pattern.format(value)
            
            html = fetch(url)
            products = parse_products(html)
            
            new_products = 0
            for product in products:
                if product["product_url"] not in all_products:
                    all_products.add(product["product_url"])
                    rows.append([ts, store_name, product["product_url"], product["title"], product["price"]])
                    new_products += 1
                    strategy_found += 1
            
            logging.info(f"  {url.replace(store_url, '...')} → {len(products)} total, {new_products} new")
            
            # Stop this strategy if no new products found
            if new_products == 0 and value != values[0]:
                break
                
            sleep_politely()
        
        logging.info(f"[{store_name}] Strategy {strategy_name}: +{strategy_found} unique products")
        sleep_politely()  # Pause between strategies
    
    logging.info(f"[{store_name}] TOTAL UNIQUE PRODUCTS: {len(all_products)}")
    return rows

def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    ts = ts_now_iso()
    t0 = time.time()

    all_rows: List[List] = []
    for store in STORES:
        store_rows = scrape_store_optimized(store["name"], store["path"], ts)
        all_rows.extend(store_rows)
        sleep_politely()

    logging.info(f"Total rows to append: {len(all_rows)}")
    if not all_rows: 
        return

    gc = get_sheets_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = ensure_worksheet(sh)
    append_rows(ws, all_rows)
    
    duration = round(time.time() - t0, 1)
    logging.info(f"Appended {len(all_rows)} rows to '{SHEET_TAB}' in {duration}s.")
    
    # Performance metrics
    requests_made = sum(len(range(1, max_pages+1)) if isinstance(max_pages, int) else len(max_pages) 
                       for _, _, max_pages in PAGINATION_STRATEGIES)
    logging.info(f"Efficiency: {len(all_rows)} products with ~{requests_made} requests per store")

if __name__ == "__main__":
    run()
