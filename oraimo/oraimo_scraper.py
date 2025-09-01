# oraimo_scraper.py
# Scrape collection pages on https://ke.oraimo.com and append snapshots to Google Sheets.
# Prices are kept AS-IS (e.g., "KES 2,700"); no numeric cleaning.

import os
import time
import random
import logging
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup

# Google Sheets
import gspread
from google.oauth2.service_account import Credentials

# ───────────────────────── CONFIG ─────────────────────────
BASE_URL = "https://ke.oraimo.com"
CATEGORY_SLUGS = [
    "audio",
    "power",
    "smart-office",
    "personal-care",
    "home-appliances",
]

# CATEGORY_SLUGS = [
#     "power-station",
#     "iphone-accessories",
#     "wall-chargers",
#     "car-chargers",
#     "cables",
#     "wireless-stereo-earbuds",
#     "wireless-neckband-headphones",
#     "wireless-over-ear-headphones",
#     "open-ear-headphones",
#     "wireless-speakers",
#     "wired-earphones",
#     "smart-watches",
#     "smart-lighting",
#     "mouse-keyboards",
#     "mi-fi",
#     "smart-plug",
#     "camera-accessories",
#     "grooming-series",
#     "oral-care",
#     "hair-styling-tools",
#     "blenders",
#     "electric-kettles",
#     "kitchen-appliances",
#     "humidifiers",
#     "vacuums",
#     "light-bulbs",
#     "irons",
#     "baby-feeding",
#     "fans",
#     "power-banks",
# ]


SHEET_ID = "18QRcbrEq2T-iaNQICu535J2u_cPFzQxCY-GRcDMt49o"     # <-- <<< REQUIRED
SHEET_TAB = "raw"
 
# polite crawling
REQUEST_TIMEOUT = 20
RETRY_COUNT = 3
REQUEST_DELAY_RANGE = (1.0, 1.8)  # seconds (random jitter)
MAX_PAGES_PER_COLLECTION = 60     # safety cap

# HTTP headers
USER_AGENT = (
    "Mozilla/5.0 (compatible; PriceTracker/1.0; +learning-project) "
    "PythonRequests"
)
HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-KE,en;q=0.8",
    "Connection": "close",
}

# Nairobi timestamp (zoneinfo if available; fallback to UTC+3)
try:
    from zoneinfo import ZoneInfo  # Py3.9+
    NAIR_OBS = ZoneInfo("Africa/Nairobi")
except Exception:
    from datetime import timezone, timedelta
    NAIR_OBS = timezone(timedelta(hours=3))

# Sheet columns
HEADER = [
    "ts",
    "category",
    "product_url",
    "title",
    "short_description",
    "price_now",
    "price_was",
    "currency",
    "main_image_url",
    "ean",
    "model",
    "stock_status",
    "slug",
]

CURRENCY = "KES"

# ───────────────────────── UTILS ─────────────────────────
def ts_now_iso() -> str:
    # YYYY-MM-DD HH:MM:SS (Nairobi)
    from datetime import datetime
    return datetime.now(NAIR_OBS).strftime("%Y-%m-%d %H:%M:%S")

def sleep_politely():
    time.sleep(random.uniform(*REQUEST_DELAY_RANGE))

def absolute_url(href: str) -> str:
    if not href:
        return ""
    return urljoin(BASE_URL, href)

def extract_slug(product_url: str) -> str:
    try:
        path = urlparse(product_url).path  # /product/<slug>
        if "/product/" in path:
            return path.split("/product/", 1)[1].strip("/").split("/")[0]
        return path.strip("/")
    except Exception:
        return ""

def extract_ean_from_url(href: str) -> Optional[str]:
    try:
        q = parse_qs(urlparse(href).query)
        ean = q.get("ean", [])
        if ean:
            return ean[0]
    except Exception:
        pass
    return None

def first_text(root, selectors) -> str:
    """Return text for the first selector that matches with non-empty text (stripped)."""
    for sel in selectors:
        el = root.select_one(sel)
        if el:
            txt = el.get_text(strip=True)
            if txt:
                return txt
    return ""

def fetch(url: str) -> Optional[str]:
    """GET with retries + polite delay."""
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

# ───────────────────── PARSING (COLLECTION) ─────────────────────
def parse_tile(div) -> Optional[Dict]:
    """
    Parse one product tile: div.js_product.site-product
    """
    try:
        # anchor to the product page
        a = div.select_one('a[href^="/product/"]')
        if not a:
            return None

        href = a.get("href", "").strip()
        product_url = absolute_url(href)
        slug = extract_slug(product_url)

        # Prefer the full title from data-name, else anchor text
        title = a.get("data-name") or a.get_text(strip=True)

        # model (SKU)
        model = (a.get("data-sku") or "").strip()

        # EAN from URL query
        ean = extract_ean_from_url(href) or ""

        # main image (handle lazy-load src/data-src/srcset)
        img = div.select_one(".product-picture-wrap img")
        main_img = ""
        if img:
            main_img = img.get("src") or img.get("data-src") or ""
            if not main_img and img.get("srcset"):
                # take the first candidate from srcset
                main_img = img.get("srcset").split(",")[0].split()[0]
            main_img = absolute_url(main_img)

        # short description: join the "feature points"
        short_points = []
        for pp in div.select("div.product-points p.product-point"):
            spans = pp.find_all("span")
            if spans:
                txt = spans[-1].get_text(strip=True)
                if txt:
                    short_points.append(txt)
        short_desc = ", ".join(short_points)

        # prices (keep AS-IS; robust selectors + fallback to data-price)
        price_now_txt = first_text(div, [
            ".product-desc .product-price span",
            "p.product-price span",
            ".product-price span",
        ])
        price_was_txt = first_text(div, [
            ".product-desc .product-price del",
            "p.product-price del",
            ".product-price del",
        ])

        if not price_now_txt:
            # fallback: sometimes price may be in data attributes
            price_now_txt = a.get("data-price") or ""
            if not price_now_txt:
                btn = div.select_one("a.js_add_to_cart")
                if btn:
                    price_now_txt = btn.get("data-price") or ""

        # stock status
        tile_text = div.get_text(" ", strip=True).lower()
        if "out of stock" in tile_text:
            stock_status = "OutOfStock"
        elif div.select_one("a.js_add_to_cart"):
            stock_status = "InStock"
        else:
            stock_status = "Unknown"

        return {
            "product_url": product_url,
            "title": title,
            "short_description": short_desc,
            "price_now": price_now_txt or "",
            "price_was": price_was_txt or "",
            "currency": CURRENCY,
            "main_image_url": main_img,
            "ean": ean,
            "model": model,
            "stock_status": stock_status,
            "slug": slug,
        }
    except Exception as ex:
        logging.exception(f"Tile parse failed: {ex}")
        return None

def parse_collection(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    tiles = soup.select("div.js_product.site-product")
    out = []
    for div in tiles:
        item = parse_tile(div)
        if item:
            out.append(item)
    return out

# ───────────────────────── SHEETS ─────────────────────────
def get_sheets_client():
    # Uses GOOGLE_APPLICATION_CREDENTIALS env var
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

    first_row = ws.row_values(1)
    if not first_row:
        ws.append_row(HEADER, value_input_option="RAW")
    elif first_row != HEADER:
        # If header differs, we won't rewrite it automatically (to avoid clobbering).
        logging.warning("Sheet header differs from expected; appending rows under existing header.")
    return ws

def append_rows(ws, rows: List[List]):
    if not rows:
        return
    try:
        ws.append_rows(rows, value_input_option="RAW")
    except Exception:
        # Fallback if append_rows not available
        for r in rows:
            ws.append_row(r, value_input_option="RAW")

# ───────────────────────── RUN ─────────────────────────
def scrape_category(slug: str) -> List[Dict]:
    """Scrape all pages of a collection and return product dicts."""
    all_items: List[Dict] = []
    seen_urls = set()

    for page in range(1, MAX_PAGES_PER_COLLECTION + 1):
        url = f"{BASE_URL}/collections/{slug}?page={page}"
        logging.info(f"Fetching {url}")
        html = fetch(url)
        if not html:
            logging.info(f"Stopping: no HTML for page {page} of {slug}")
            break

        items = parse_collection(html)
        if not items:
            logging.info(f"Stopping: zero tiles on page {page} of {slug}")
            break

        # de-dup by product_url within a category (some sites repeat tiles)
        new_items = [x for x in items if x["product_url"] not in seen_urls]
        for x in new_items:
            x["category"] = slug.replace("-", " ").title()

        all_items.extend(new_items)
        seen_urls.update(x["product_url"] for x in new_items)

        sleep_politely()
        if len(new_items) == 0:
            break

    return all_items

def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    t0 = time.time()
    ts = ts_now_iso()

    # scrape all categories
    everything: List[Dict] = []
    for slug in CATEGORY_SLUGS:
        items = scrape_category(slug)
        logging.info(f"{slug}: {len(items)} items")
        everything.extend(items)

    logging.info(f"Total products scraped: {len(everything)}")

    # rows for Google Sheets
    rows = []
    for it in everything:
        rows.append([
            ts,
            it.get("category", ""),
            it.get("product_url", ""),
            it.get("title", ""),
            it.get("short_description", ""),
            it.get("price_now", ""),
            it.get("price_was", ""),
            it.get("currency", CURRENCY),
            it.get("main_image_url", ""),
            it.get("ean", ""),
            it.get("model", ""),
            it.get("stock_status", ""),
            it.get("slug", ""),
        ])

    # write to Google Sheets
    gc = get_sheets_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = ensure_worksheet(sh)
    append_rows(ws, rows)

    logging.info(f"Appended {len(rows)} rows to '{SHEET_TAB}'.")
    logging.info(f"Done in {round(time.time()-t0, 1)}s")

if __name__ == "__main__":
    run()
