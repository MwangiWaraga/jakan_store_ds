import os
import time
import random
import logging
import re
from typing import List, Dict, Optional
from urllib.parse import urljoin, urlparse, parse_qs
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict

# ───────────────────────── BIGQUERY CONFIG ─────────────────────────
GCP_PROJECT_ID = "jakan-group"          # <-- <<< REQUIRED
BQ_DATASET     = "core"    # will be created if missing
BQ_TABLE       = "oraimo_products_raw_bqt"        # will be created if missing
BQ_LOCATION    = "europe-west1"         # match your dataset region

# ───────────────────────── SCRAPER CONFIG ─────────────────────────
BASE_URL = "https://ke.oraimo.com"
CATEGORY_SLUGS = [
    "audio",
    "power",
    "smart-office",
    "personal-care",
    "home-appliances",
]

REQUEST_TIMEOUT = 20
RETRY_COUNT = 3
REQUEST_DELAY_RANGE = (1.0, 1.8)
MAX_PAGES_PER_COLLECTION = 60

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

# Nairobi timestamp setup
try:
    from zoneinfo import ZoneInfo
    NAIR_OBS = ZoneInfo("Africa/Nairobi")
except Exception:
    from datetime import timezone as dt_timezone, timedelta
    NAIR_OBS = dt_timezone(timedelta(hours=3))

CURRENCY = "KES"

# ───────────────────────── UTILS ─────────────────────────
def ts_now_utc_fmt() -> str:
    """Return current UTC time formatted as 'YYYY-MM-DD HH:MM:SS'."""
    # We grab UTC directly, no need to convert from Nairobi first
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def sleep_politely():
    time.sleep(random.uniform(*REQUEST_DELAY_RANGE))

def absolute_url(href: str) -> str:
    if not href:
        return ""
    return urljoin(BASE_URL, href)

def extract_slug(product_url: str) -> str:
    try:
        path = urlparse(product_url).path
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
    for sel in selectors:
        el = root.select_one(sel)
        if el:
            txt = el.get_text(strip=True)
            if txt:
                return txt
    return ""

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

# ───────────────────── PARSING ─────────────────────

def parse_tile(div) -> Optional[Dict]:
    try:
        a = div.select_one('a[href^="/product/"]')
        if not a:
            return None

        href = a.get("href", "").strip()
        product_url = absolute_url(href)
        slug = extract_slug(product_url)

        title = a.get("data-name") or a.get_text(strip=True)
        model = (a.get("data-sku") or "").strip()
        ean = extract_ean_from_url(href) or ""

        img = div.select_one(".product-picture-wrap img")
        main_img = ""
        if img:
            main_img = img.get("src") or img.get("data-src") or ""
            if not main_img and img.get("srcset"):
                main_img = img.get("srcset").split(",")[0].split()[0]
            main_img = absolute_url(main_img)

        short_points = []
        for pp in div.select("div.product-points p.product-point"):
            spans = pp.find_all("span")
            if spans:
                txt = spans[-1].get_text(strip=True)
                if txt:
                    short_points.append(txt)
        short_desc = ", ".join(short_points)

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
            price_now_txt = a.get("data-price") or ""
            if not price_now_txt:
                btn = div.select_one("a.js_add_to_cart")
                if btn:
                    price_now_txt = btn.get("data-price") or ""

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

def get_total_pages(html: str) -> int:
    try:
        soup = BeautifulSoup(html, "html.parser")
        pagination_text = soup.get_text()
        if "Total" in pagination_text and "Pages" in pagination_text:
            match = re.search(r'Total\s+(\d+)\s+Pages', pagination_text, re.IGNORECASE)
            if match:
                return int(match.group(1))

        page_links = soup.find_all('a', href=re.compile(r'page=\d+'))
        if page_links:
            nums = []
            for link in page_links:
                m = re.search(r'page=(\d+)', link.get('href', ''))
                if m:
                    nums.append(int(m.group(1)))
            if nums:
                return max(nums)
        return 1
    except Exception as e:
        logging.warning(f"Could not determine total pages: {e}")
        return 1

# ───────────────────────── LOGIC ─────────────────────────

def scrape_category(slug: str) -> List[Dict]:
    """Scrapes all pages for a specific category slug."""
    all_items: List[Dict] = []
    seen_urls = set()

    url = f"{BASE_URL}/collections/{slug}?page=1"
    logging.info(f"Fetching {url}")
    html = fetch(url)
    if not html:
        logging.warning(f"No HTML returned for first page of {slug}")
        return all_items

    total_pages = get_total_pages(html)
    max_pages = min(total_pages, MAX_PAGES_PER_COLLECTION)
    logging.info(f"Will scrape {max_pages} pages for category '{slug}'")

    # Page 1
    items = parse_collection(html)
    if items:
        new_items = [x for x in items if x["product_url"] not in seen_urls]
        for x in new_items:
            x["category"] = slug.replace("-", " ").title()
        all_items.extend(new_items)
        seen_urls.update(x["product_url"] for x in new_items)

    # Subsequent pages
    for page in range(2, max_pages + 1):
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

        new_items = [x for x in items if x["product_url"] not in seen_urls]
        for x in new_items:
            x["category"] = slug.replace("-", " ").title()
        all_items.extend(new_items)
        seen_urls.update(x["product_url"] for x in new_items)

        sleep_politely()
        if len(new_items) == 0:
            logging.info(f"Stopping: no new items on page {page} of {slug}")
            break

    return all_items

# ───────────────────────── BIGQUERY ─────────────────────────

def get_bq_client() -> bigquery.Client:
    return bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)

def ensure_dataset(client: bigquery.Client, dataset_id: str) -> bigquery.Dataset:
    ds_ref = bigquery.Dataset(f"{client.project}.{dataset_id}")
    ds_ref.location = BQ_LOCATION
    try:
        return client.get_dataset(ds_ref)
    except NotFound:
        logging.info(f"Creating dataset {client.project}.{dataset_id} in {BQ_LOCATION}")
        return client.create_dataset(ds_ref, exists_ok=True)

def ensure_table(client: bigquery.Client, dataset_id: str, table_id: str) -> bigquery.Table:
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    schema = [
        bigquery.SchemaField("ts", "TIMESTAMP"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("product_url", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("short_description", "STRING"),
        bigquery.SchemaField("price_now", "STRING"),
        bigquery.SchemaField("price_was", "STRING"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("main_image_url", "STRING"),
        bigquery.SchemaField("ean", "STRING"),
        bigquery.SchemaField("model", "STRING"),
        bigquery.SchemaField("stock_status", "STRING"),
        bigquery.SchemaField("slug", "STRING"),
    ]
    try:
        return client.get_table(table_ref)
    except NotFound:
        logging.info(f"Creating table {table_ref}")
        table = bigquery.Table(table_ref, schema=schema)
        return client.create_table(table, exists_ok=True)
    except Conflict:
        return client.get_table(table_ref)

def bq_append_rows(client: bigquery.Client, dataset_id: str, table_id: str, rows: List[Dict]) -> int:
    if not rows:
        return 0

    ensure_dataset(client, dataset_id)
    ensure_table(client, dataset_id, table_id)

    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    load_job = client.load_table_from_json(
        rows,
        table_ref,
        job_config=job_config,
        location=BQ_LOCATION,
    )
    result = load_job.result()
    return result.output_rows or len(rows)

# ───────────────────────── RUN ─────────────────────────
def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    t0 = time.time()
    
    # NEW: Uses the simple YYYY-MM-DD HH:MM:SS format (UTC)
    ts_str = ts_now_utc_fmt()  

    everything: List[Dict] = []

    # Clean loop using the helper function
    for slug in CATEGORY_SLUGS:
        items = scrape_category(slug)
        logging.info(f"{slug}: {len(items)} items")
        everything.extend(items)

    logging.info(f"Total products scraped: {len(everything)}")

    # Prepare rows for BigQuery
    rows: List[Dict] = []
    for it in everything:
        rows.append({
            "ts": ts_str,
            "category": it.get("category", ""),
            "product_url": it.get("product_url", ""),
            "title": it.get("title", ""),
            "short_description": it.get("short_description", ""),
            "price_now": it.get("price_now", ""),
            "price_was": it.get("price_was", ""),
            "currency": it.get("currency", CURRENCY),
            "main_image_url": it.get("main_image_url", ""),
            "ean": it.get("ean", ""),
            "model": it.get("model", ""),
            "stock_status": it.get("stock_status", ""),
            "slug": it.get("slug", ""),
        })

    # Upload to BigQuery
    if rows:
        client = get_bq_client()
        inserted = bq_append_rows(client, BQ_DATASET, BQ_TABLE, rows)
        logging.info(f"Appended {inserted} rows to {client.project}.{BQ_DATASET}.{BQ_TABLE}.")
    else:
        logging.warning("No rows collected, skipping BQ upload.")

    logging.info(f"Done in {round(time.time()-t0, 1)}s")

if __name__ == "__main__":
    run()