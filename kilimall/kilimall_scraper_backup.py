# kilimall_scraper_multi.py
# Scrape multiple Kilimall stores (listing pages only) → Google Sheets (append).
# Columns: ts, store, product_url, title, price (raw "KSh ...")

import os, re, time, random, logging
from typing import List, Dict, Optional, Tuple, Callable
from urllib.parse import urljoin, urlencode, urlparse, parse_qsl, urlunparse
import requests
from bs4 import BeautifulSoup

# ── CONFIG ───────────────────────────────────────────────────────────────
BASE_URL = "https://www.kilimall.co.ke"
STORES = [
    {"name": "JAKAN PHONE STORE", "path": "/store/JAKAN-PHONE-STORE"},
    # {"name": "STORE 2", "path": "/store/STORE-2"},
]
SHEET_ID  = "18QRcbrEq2T-iaNQICu535J2u_cPFzQxCY-GRcDMt49o"
SHEET_TAB = "kilimall"

REQUEST_TIMEOUT = 20
RETRY_COUNT = 3
REQUEST_DELAY_RANGE = (1.0, 1.8)
MAX_PAGES = 30  # Increased to ensure we can get all 86 items

USER_AGENT = "Mozilla/5.0 (compatible; KilimallStoreScraper/2.2; +learning-project) PythonRequests"
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-KE,en;q=0.8",
    "Connection": "close",
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
    scopes = ["https://www.googleapis.com/auth/spreadsheets",
              "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=scopes
    )
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
        for r in rows: ws.append_row(r, value_input_option="RAW")

# ── UTILS ────────────────────────────────────────────────────────────────
def ts_now_iso():
    from datetime import datetime
    return datetime.now(NAIR_OBS).strftime("%Y-%m-%d %H:%M:%S")

def sleep_politely():
    time.sleep(random.uniform(*REQUEST_DELAY_RANGE))

def absolute_url(href: str) -> str:
    return urljoin(BASE_URL, href or "")

def with_query_param(url: str, key: str, val: str) -> str:
    u = urlparse(url)
    q = dict(parse_qsl(u.query, keep_blank_values=True))
    q[key] = val
    return urlunparse((u.scheme, u.netloc, u.path, u.params, urlencode(q), u.fragment))

def fetch(url: str) -> Optional[str]:
    for _ in range(RETRY_COUNT):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200 and "text/html" in r.headers.get("Content-Type",""):
                return r.text
            if 400 <= r.status_code < 500:
                return None
        except requests.RequestException as ex:
            logging.warning(f"Fetch error for {url}: {ex}")
        sleep_politely()
    return None

# ── PARSING ──────────────────────────────────────────────────────────────
PRICE_RE = re.compile(r"KSh\s*[\d,]+(?:\.\d+)?", re.IGNORECASE)

def extract_price_from_container(container: BeautifulSoup) -> str:
    for sel in [".price", ".goods-price", ".km-price", "[class*=price]", "[class*=Price]"]:
        el = container.select_one(sel)
        if el:
            m = PRICE_RE.search(" ".join(el.stripped_strings))
            if m: return m.group(0).strip()
    m = PRICE_RE.search(" ".join(container.stripped_strings))
    return m.group(0).strip() if m else ""

def extract_title_from_container(container: BeautifulSoup, a_tag: BeautifulSoup) -> str:
    for sel in ("[class*=title]","[class*=Title]","[class*=name]","[class*=Name]"):
        el = container.select_one(sel)
        if el:
            t = " ".join(el.stripped_strings)
            t = PRICE_RE.sub("", t).strip()
            if t: return t
    title_attr = (a_tag.get("title") or a_tag.get("aria-label") or "").strip()
    if title_attr: return PRICE_RE.sub("", title_attr).strip()
    return PRICE_RE.sub("", " ".join(a_tag.stripped_strings)).strip()

def find_card_for_anchor(a: BeautifulSoup) -> BeautifulSoup:
    node = a
    for _ in range(6):
        if not node or node.name == "body": break
        cls = " ".join(node.get("class", [])).lower()
        if ("goods" in cls or "product" in cls or node.name in ("li","div")) and extract_price_from_container(node):
            return node
        node = node.parent
    return a

def parse_store_page(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    items, seen = [], set()
    for a in soup.select('a[href^="/listing/"]'):
        href = a.get("href","").strip()
        if not href or href in seen: continue
        seen.add(href)
        product_url = absolute_url(href)
        card  = find_card_for_anchor(a)
        price = extract_price_from_container(card)
        title = extract_title_from_container(card, a)
        if not title: continue
        items.append({"product_url": product_url, "title": title, "price": price})
    return items

# ── PAGINATION: PARAM PROBING ────────────────────────────────────────────
PARAM_CANDIDATES = [
    "page", "p", "pageNo", "pageNum", "current", "currentPage",
    "curPage", "pn", "index", "page_index", "pageIndex",
    "start", "offset", "from", "begin", "skip", "limit"
]

def detect_page_builder(store_url: str, seen_urls: set) -> Optional[Callable[[int], str]]:
    """Try common query param names; return a function build_url(page_n) if any works."""
    for key in PARAM_CANDIDATES:
        if key in ["start", "offset", "from", "begin", "skip"]:
            # For offset-based pagination, use multiples of 32
            test_url = with_query_param(store_url, key, "32")
        else:
            test_url = with_query_param(store_url, key, "2")
            
        html = fetch(test_url)
        if not html: continue
        items = parse_store_page(html)
        new = [it for it in items if it["product_url"] not in seen_urls]
        logging.info(f"Probe {key}={'32' if key in ['start', 'offset', 'from', 'begin', 'skip'] else '2'} → {len(items)} items ({len(new)} new)")
        if new:
            if key in ["start", "offset", "from", "begin", "skip"]:
                return lambda n, k=key: with_query_param(store_url, k, str((n-1)*32))
            else:
                return lambda n, k=key: with_query_param(store_url, k, str(n))
    
    # Try with sorting parameters to get different results
    for sort_param in ["sort", "sortBy", "orderBy"]:
        for sort_value in ["price_asc", "price_desc", "name", "newest", "rating"]:
            test_url = with_query_param(with_query_param(store_url, sort_param, sort_value), "page", "2")
            html = fetch(test_url)
            if not html: continue
            items = parse_store_page(html)
            new = [it for it in items if it["product_url"] not in seen_urls]
            if len(new) > 5:  # More lenient threshold
                logging.info(f"Probe {sort_param}={sort_value}&page=2 → {len(items)} items ({len(new)} new)")
                return lambda n: with_query_param(with_query_param(store_url, sort_param, sort_value), "page", str(n))
    
    return None

# ── SCRAPE ONE STORE ────────────────────────────────────────────────────
def scrape_store(store_name: str, store_path: str, ts: str) -> List[List]:
    store_url = absolute_url(store_path)
    logging.info(f"[{store_name}] Fetch {store_url}")

    rows: List[List] = []
    all_seen_products = set()

    # Strategy: Try multiple sorting views to get all products
    sort_strategies = [
        "",  # default/no sort
        "?sort=price_asc",
        "?sort=price_desc", 
        "?sort=newest",
        "?sort=rating",
        "?orderBy=price",
        "?orderBy=sales",
    ]

    for sort_suffix in sort_strategies:
        strategy_url = store_url + sort_suffix
        strategy_name = sort_suffix.replace("?", "") or "default"
        
        # Page 1 of this strategy
        html1 = fetch(strategy_url)
        if not html1:
            logging.warning(f"[{store_name}] failed to load {strategy_name} page 1")
            continue

        items1 = parse_store_page(html1)
        new_items_count = 0
        for it in items1:
            if it["product_url"] not in all_seen_products:
                rows.append([ts, store_name, it["product_url"], it["title"], it["price"]])
                all_seen_products.add(it["product_url"])
                new_items_count += 1

        logging.info(f"[{store_name}] {strategy_name} page 1 → {len(items1)} items ({new_items_count} new)")
        
        # Try pagination for this strategy if we got new items
        if new_items_count > 0:
            builder = detect_page_builder(strategy_url, all_seen_products)
            if builder:
                for page in range(2, min(MAX_PAGES//len(sort_strategies) + 1, 10)):  # Limit pages per strategy
                    url = builder(page)
                    html = fetch(url)
                    if not html:
                        break
                    items = parse_store_page(html)
                    new_added = 0
                    for it in items:
                        if it["product_url"] not in all_seen_products:
                            rows.append([ts, store_name, it["product_url"], it["title"], it["price"]])
                            all_seen_products.add(it["product_url"])
                            new_added += 1
                    logging.info(f"[{store_name}] {strategy_name} page {page} → {len(items)} items ({new_added} new)")
                    if new_added == 0:
                        break
                    sleep_politely()
        
        # Brief pause between strategies
        sleep_politely()

    logging.info(f"[{store_name}] TOTAL UNIQUE PRODUCTS: {len(all_seen_products)}")
    return rows

# ── RUN ─────────────────────────────────────────────────────────────────
def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    ts = ts_now_iso(); t0 = time.time()

    all_rows: List[List] = []
    for s in STORES:
        all_rows.extend(scrape_store(s["name"].strip(), s["path"].strip(), ts))
        sleep_politely()

    logging.info(f"Total rows to append: {len(all_rows)}")
    if not all_rows: return
    gc = get_sheets_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = ensure_worksheet(sh)
    append_rows(ws, all_rows)
    logging.info(f"Appended {len(all_rows)} rows to '{SHEET_TAB}' in {round(time.time()-t0,1)}s.")

if __name__ == "__main__":
    run()
