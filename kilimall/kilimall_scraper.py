# kilimall_scraper.py
# Scrape specific Kilimall store pages and append product snapshots to Google Sheets.
# Fields saved: updated_at, store_name, product_title, product_url, price

import os
import re
import time
import math
import random
import logging
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

# ───────────────────────── CONFIG ─────────────────────────
BASE_URL = "https://www.kilimall.co.ke"

# Map of stores to scrape: {slug: human_readable_name}
# Add more slugs as needed
STORES: Dict[str, str] = {
    "JAKAN-PHONE-STORE": "Jakan Phone Store",
    # "ORAIMO-OFFICIAL-STORE": "Oraimo Official Store",
}

SHEET_ID = "18QRcbrEq2T-iaNQICu535J2u_cPFzQxCY-GRcDMt49o"  # same sheet, or change
SHEET_TAB = "kilimall"                                # new tab for kilimall

# polite crawling
REQUEST_TIMEOUT = 20
RETRY_COUNT = 3
REQUEST_DELAY_RANGE = (1.0, 1.8)
MAX_PAGES_PER_STORE = 120  # safety cap

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0 Safari/537.36"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-KE,en;q=0.8",
    "Connection": "close",
}

# Nairobi timestamp
try:
    from zoneinfo import ZoneInfo  # Py3.9+
    NAIR_OBS = ZoneInfo("Africa/Nairobi")
except Exception:
    from datetime import timezone, timedelta
    NAIR_OBS = timezone(timedelta(hours=3))

HEADER = [
    "updated_at",
    "store_name",
    "product_title",
    "product_url",
    "price",
]

# ───────────────────────── UTILS ─────────────────────────
def ts_now_iso() -> str:
    from datetime import datetime
    return datetime.now(NAIR_OBS).strftime("%Y-%m-%d %H:%M:%S")

def sleep_politely():
    time.sleep(random.uniform(*REQUEST_DELAY_RANGE))

def fetch(url: str) -> Optional[str]:
    """GET with retries + polite delay."""
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200 and "text/html" in r.headers.get("Content-Type", ""):
                return r.text
            logging.warning(f"[{r.status_code}] Non-HTML/err for {url}")
            if 400 <= r.status_code < 500:
                return None
        except requests.RequestException as ex:
            logging.warning(f"Request error (attempt {attempt}) for {url}: {ex}")
        sleep_politely()
    return None

def ensure_ws(sh) -> gspread.Worksheet:
    try:
        ws = sh.worksheet(SHEET_TAB)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=SHEET_TAB, rows=1000, cols=len(HEADER) + 2)
        ws.append_row(HEADER, value_input_option="RAW")
        return ws

    first = ws.row_values(1)
    if not first:
        ws.append_row(HEADER, value_input_option="RAW")
    return ws

def get_sheets_client():
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=scopes
    )
    return gspread.authorize(creds)

def append_rows(ws, rows: List[List]):
    if not rows:
        return
    try:
        ws.append_rows(rows, value_input_option="RAW")
    except Exception:
        for r in rows:
            ws.append_row(r, value_input_option="RAW")

# ───────────────────── PARSING HELPERS ───────────────────
_STORE_ID_RE = re.compile(r"/store/(\d+)")
_PRODUCTS_COUNT_RE = re.compile(r"Products:\s*(\d+)", re.I)
_PRICE_RE = re.compile(r"KSh\s*[\d,]+", re.I)

def extract_store_id(html: str) -> Optional[str]:
    """
    Try to find a numeric /store/<id> reference in the HTML.
    Many Kilimall store pages embed /store/<id> in links/scripts.
    """
    m = _STORE_ID_RE.search(html or "")
    return m.group(1) if m else None

def total_products_from_html(html: str) -> Optional[int]:
    m = _PRODUCTS_COUNT_RE.search(html or "")
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None

def parse_product_tiles(html: str) -> List[Dict]:
    """
    Very robust parser: find all product anchors pointing to /listing/<id>.
    For each anchor, try to extract a title; find the closest 'KSh <price>' inside
    the same tile container.
    """
    soup = BeautifulSoup(html or "", "html.parser")
    out = []
    seen = set()

    for a in soup.select('a[href^="/listing/"]'):
        href = a.get("href", "").strip()
        if not href or href in seen:
            continue
        seen.add(href)

        product_url = urljoin(BASE_URL, href)

        # Best-effort title
        title = (a.get("title") or a.get_text(" ", strip=True) or "").strip()
        # Climb up to a likely tile container to find price nearby
        price_txt = ""
        node = a
        for _ in range(6):  # climb a few levels only
            if not node:
                break
            text = " ".join(list(node.stripped_strings))
            m = _PRICE_RE.search(text)
            if m:
                price_txt = m.group(0)
                break
            node = node.parent

        out.append({
            "product_title": title,
            "product_url": product_url,
            "price": price_txt,
        })

    return out

# ───────────────────── PAGINATION STRATEGIES ───────────────────
def subpage_url(store_id: str, page: int) -> List[str]:
    """
    Heuristic: Kilimall uses a 'sub-page' endpoint to load the product grid via AJAX.
    We try a few param names until one returns new items.
    """
    base = f"{BASE_URL}/new/store/sub-page/{store_id}"
    candidates = [
        f"{base}?typeName=All+Products&pageNum={page}",
        f"{base}?typeName=All+Products&page={page}",
        f"{base}?typeName=All+Products&pageNo={page}",
        f"{base}?pageNum={page}&typeName=All+Products",
    ]
    return candidates

def slug_page_url(slug: str, page: int) -> List[str]:
    base = f"{BASE_URL}/store/{slug}"
    return [
        f"{base}?page={page}",
        f"{base}&page={page}",
        f"{base}?pageNum={page}",
    ]

def scrape_store(slug: str, store_name: str) -> List[Dict]:
    """
    Scrape one store (all pages). Returns list of product dicts.
    """
    logging.info(f"Store: {store_name} ({slug})")

    start_url = f"{BASE_URL}/store/{slug}"
    html = fetch(start_url)
    if not html:
        logging.warning(f"Failed to load first page: {start_url}")
        return []

    first_items = parse_product_tiles(html)
    tp = total_products_from_html(html) or 0
    per_page = max(len(first_items), 1)
    # Use first page size to estimate total pages
    pages_est = max(1, math.ceil(tp / per_page))
    pages_est = min(pages_est, MAX_PAGES_PER_STORE)

    logging.info(f"Found ~{tp} products; first page items={len(first_items)}; pages≈{pages_est}")

    all_items = first_items[:]
    seen_urls = {x["product_url"] for x in all_items}

    # Prefer AJAX sub-page route if we can find a store id
    store_id = extract_store_id(html)

    # Fetch remaining pages
    for page in range(2, pages_est + 1):
        html_page = None

        # Try sub-page endpoints first (desktop pagination via AJAX)
        if store_id:
            for candidate in subpage_url(store_id, page):
                html_page = fetch(candidate)
                if not html_page:
                    continue
                items = parse_product_tiles(html_page)
                new_items = [x for x in items if x["product_url"] not in seen_urls]
                if new_items:
                    logging.info(f"Page {page} via sub-page worked -> {len(new_items)} new")
                    all_items.extend(new_items)
                    seen_urls.update(x["product_url"] for x in new_items)
                    break  # go to next page
                else:
                    html_page = None  # try next candidate
        # Fallback: try direct page params on the slug URL
        if html_page is None:
            fetched = False
            for candidate in slug_page_url(slug, page):
                html_page = fetch(candidate)
                if not html_page:
                    continue
                items = parse_product_tiles(html_page)
                new_items = [x for x in items if x["product_url"] not in seen_urls]
                if new_items:
                    logging.info(f"Page {page} via slug?page=... worked -> {len(new_items)} new")
                    all_items.extend(new_items)
                    seen_urls.update(x["product_url"] for x in new_items)
                    fetched = True
                    break
            if not fetched:
                logging.info(f"Stopping at page {page}: no new items via any strategy")
                break

        sleep_politely()

    # Attach store name for output
    for x in all_items:
        x["store_name"] = store_name

    return all_items

# ───────────────────────── RUN ─────────────────────────
def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    t0 = time.time()
    ts = ts_now_iso()

    everything: List[Dict] = []
    for slug, store_name in STORES.items():
        items = scrape_store(slug, store_name)
        logging.info(f"{store_name}: {len(items)} items scraped")
        everything.extend(items)

    logging.info(f"Total rows: {len(everything)}")

    # Prepare rows
    rows = []
    for it in everything:
        rows.append([
            ts,
            it.get("store_name", ""),
            it.get("product_title", ""),
            it.get("product_url", ""),
            it.get("price", ""),
        ])

    # Write to Google Sheets
    gc = get_sheets_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = ensure_ws(sh)
    append_rows(ws, rows)

    logging.info(f"Appended {len(rows)} rows to '{SHEET_TAB}'. Done in {round(time.time()-t0, 1)}s.")

if __name__ == "__main__":
    run()
