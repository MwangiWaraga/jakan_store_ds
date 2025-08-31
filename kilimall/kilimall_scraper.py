# kilimall_scraper_multi.py
# Scrape multiple Kilimall store pages (listing only; no product-page fetches)
# Appends rows to Google Sheets: ts, store, product_url, title, price (raw “KSh …”)

import os
import re
import time
import random
import logging
from typing import List, Dict, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

# ───────────────────────── CONFIG ─────────────────────────
BASE_URL = "https://www.kilimall.co.ke"

# Add/modify your stores here
STORES = [
    {"name": "JAKAN PHONE STORE", "path": "/store/JAKAN-PHONE-STORE"},
    # {"name": "Another Store", "path": "/store/ANOTHER-STORE"},
    # {"name": "Third Store",   "path": "/store/THIRD-STORE"},
]

SHEET_ID  = "18QRcbrEq2T-iaNQICu535J2u_cPFzQxCY-GRcDMt49o"   # <— REQUIRED
SHEET_TAB = "kilimall"

REQUEST_TIMEOUT = 20
RETRY_COUNT = 3
REQUEST_DELAY_RANGE = (1.0, 1.8)  # polite jitter
MAX_PAGES = 12                    # safety cap for pagination attempts

USER_AGENT = (
    "Mozilla/5.0 (compatible; KilimallStoreScraper/1.1; +learning-project) "
    "PythonRequests"
)
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

# ───────────────────────── UTILS ─────────────────────────
def ts_now_iso() -> str:
    from datetime import datetime
    return datetime.now(NAIR_OBS).strftime("%Y-%m-%d %H:%M:%S")

def sleep_politely():
    time.sleep(random.uniform(*REQUEST_DELAY_RANGE))

def fetch(url: str) -> Optional[str]:
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            ctype = resp.headers.get("Content-Type", "")
            if resp.status_code == 200 and "text/html" in ctype:
                return resp.text
            logging.warning(f"[{resp.status_code}] Non-HTML or error for {url}")
            if 400 <= resp.status_code < 500:
                return None
        except requests.RequestException as ex:
            logging.warning(f"Request error (attempt {attempt}) for {url}: {ex}")
        sleep_politely()
    return None

def absolute_url(href: str) -> str:
    return urljoin(BASE_URL, href or "")

# Price like "KSh 7,500"
PRICE_RE = re.compile(r"KSh\s*[\d,]+(?:\.\d+)?", re.IGNORECASE)

def extract_price_from_container(container: BeautifulSoup) -> str:
    # Try common price containers first
    for sel in [
        ".price", ".goods-price", ".km-price", "[class*=price]", "[class*=Price]",
    ]:
        el = container.select_one(sel)
        if el:
            m = PRICE_RE.search(" ".join(el.stripped_strings))
            if m:
                return m.group(0).strip()
    # Fallback: scan the container text
    m = PRICE_RE.search(" ".join(container.stripped_strings))
    return m.group(0).strip() if m else ""

def clean_title_text(text: str) -> str:
    # Remove embedded price from title if present
    t = " ".join(text.split())
    return PRICE_RE.sub("", t).strip()

# ───────────────────── PARSING (STORE LISTING) ─────────────────────
def parse_store_page(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    items: List[Dict] = []
    seen = set()

    # Strategy:
    # 1) Find anchors to listing pages (robots allows /listing/, disallows /product*, /item*).
    # 2) For each anchor, walk up a few ancestors to find a price node.
    for a in soup.select('a[href^="/listing/"]'):
        href = a.get("href", "").strip()
        if not href or href in seen:
            continue
        seen.add(href)

        product_url = absolute_url(href)

        # Title candidates
        title_attr = (a.get("title") or a.get("aria-label") or "").strip()
        a_text = " ".join(a.stripped_strings)
        title_text = title_attr or a_text

        # Try to find price near the anchor by checking ancestors
        price = ""
        node = a
        for _ in range(4):  # look up a few levels
            if not node:
                break
            price = extract_price_from_container(node)
            if price:
                break
            node = node.parent

        title = clean_title_text(title_text)

        if not title:  # guardrail
            continue

        items.append({
            "product_url": product_url,
            "title": title,
            "price": price,
        })

    return items

# ───────────────────────── SHEETS ─────────────────────────
def get_sheets_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=scopes
    )
    return gspread.authorize(creds)

def ensure_worksheet(sh) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(SHEET_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_TAB, rows=1000, cols=len(HEADER) + 2)
        ws.append_row(HEADER, value_input_option="RAW")
        return ws
    if not ws.row_values(1):
        ws.append_row(HEADER, value_input_option="RAW")
    return ws

def append_rows(ws, rows: List[List]):
    if not rows:
        return
    try:
        ws.append_rows(rows, value_input_option="RAW")
    except Exception:
        for r in rows:
            ws.append_row(r, value_input_option="RAW")

# ───────────────────────── RUN ─────────────────────────
def scrape_store(store_name: str, store_path: str, ts: str) -> List[List]:
    """Scrape one store; return rows ready for Sheets."""
    store_url = absolute_url(store_path)
    logging.info(f"[{store_name}] Fetch {store_url}")
    html = fetch(store_url)
    if not html:
        logging.warning(f"[{store_name}] Failed to load page 1")
        return []

    all_items = parse_store_page(html)
    logging.info(f"[{store_name}] Page 1: {len(all_items)} items")

    # Try simple pagination (?page=2, ?p=2, ?pageNum=2)
    seen_urls = {i["product_url"] for i in all_items}
    for param in ("page", "p", "pageNum"):
        for page in range(2, MAX_PAGES + 1):
            url = f"{store_url}?{param}={page}"
            logging.info(f"[{store_name}] Fetch {url}")
            html = fetch(url)
            if not html:
                break
            new_items = [
                it for it in parse_store_page(html)
                if it["product_url"] not in seen_urls
            ]
            if not new_items:
                break
            all_items.extend(new_items)
            seen_urls.update(it["product_url"] for it in new_items)
            sleep_politely()

    # Rows for Sheets
    return [[ts, store_name, it["product_url"], it["title"], it["price"]] for it in all_items]

def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    ts = ts_now_iso()
    t0 = time.time()

    rows: List[List] = []
    for s in STORES:
        name = s["name"].strip()
        path = s["path"].strip()
        rows.extend(scrape_store(name, path, ts))
        sleep_politely()

    # Write once per run
    logging.info(f"Total rows to append: {len(rows)}")
    if not rows:
        return

    gc = get_sheets_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = ensure_worksheet(sh)
    append_rows(ws, rows)

    logging.info(f"Appended {len(rows)} rows to '{SHEET_TAB}' in {round(time.time()-t0,1)}s.")

if __name__ == "__main__":
    run()
