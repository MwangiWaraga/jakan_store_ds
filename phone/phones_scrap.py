import json
import logging
import os
import random
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict

# ───────────────────────── CONFIG ─────────────────────────
GCP_PROJECT_ID = "jakan-group"
BQ_DATASET = "core"
BQ_PRODUCTS_TABLE = "phoneplace_products_raw_bqt"
BQ_VARIANTS_TABLE = "phoneplace_variants_raw_bqt"
BQ_LOCATION = "europe-west1"

CATEGORY_URLS: Dict[str, str] = {
    "infinix": "https://www.phoneplacekenya.com/product-category/smartphones/infinix-phones-in-kenya/",
    "tecno": "https://www.phoneplacekenya.com/product-category/smartphones/tecno-phones/",
    "itel": "https://www.phoneplacekenya.com/product-category/smartphones/itel/",
}
# Add more categories here, e.g.:
# CATEGORY_URLS["samsung"] = "https://www.phoneplacekenya.com/product-category/smartphones/samsung/"

REQUEST_TIMEOUT = 25
RETRY_COUNT = 3
DELAY_RANGE = (1.0, 2.0)
MAX_PAGES_PER_CATEGORY = 200

# If you get 403s in requests, enable this fallback
USE_PLAYWRIGHT_FALLBACK = True
PLAYWRIGHT_HEADLESS = True

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-KE,en;q=0.9",
    "Connection": "keep-alive",
}

CURRENCY_DEFAULT = "KES"


# ───────────────────────── UTILS ─────────────────────────
def ts_now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def sleep_politely():
    time.sleep(random.uniform(*DELAY_RANGE))


def clean_text(s: str) -> str:
    s = s or ""
    s = re.sub(r"\s+", " ", s).strip()
    return s


def text_or_empty(el) -> str:
    if not el:
        return ""
    return clean_text(el.get_text(" ", strip=True))


def strip_query(url: str) -> str:
    try:
        p = urlparse(url)
        return p._replace(query="", fragment="").geturl()
    except Exception:
        return url


def parse_prices_any(text: str) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Returns (current_price, regular_price, minmax_range_mid=None)
    - If a single price: current_price set
    - If range: current_price None, and min/max returned via min/max parsing elsewhere
    We keep it simple: just extract numbers from the string; caller decides min/max.
    """
    if not text:
        return None, None, None
    nums = re.findall(r"(\d[\d,]*\.?\d*)", text.replace("\u2013", "-"))
    vals = []
    for n in nums:
        try:
            vals.append(float(n.replace(",", "")))
        except Exception:
            pass
    if not vals:
        return None, None, None
    if len(vals) == 1:
        return vals[0], None, None
    # multiple numbers (could be sale/regular or range). Caller will compute min/max.
    return None, None, None


def pick_attr(attrs: Dict[str, str], keywords: List[str]) -> str:
    """
    attrs keys often like: attribute_pa_storage, attribute_pa_ram
    """
    for k, v in (attrs or {}).items():
        lk = (k or "").lower()
        if any(kw in lk for kw in keywords):
            return v or ""
    return ""


def kv_from_colon_text(block: str) -> Dict[str, str]:
    """
    Attempts to extract "Key: Value" style specs from a text blob.
    Example: "RAM: 8GB Internal Memory: 128/256GB OS: Android 15"
    """
    out: Dict[str, str] = {}
    if not block:
        return out

    # Find sequences like "Label: value" until next "Label:"
    pattern = re.compile(r"([A-Za-z][A-Za-z0-9 /&\-\(\)]+):\s*([^:]+?)(?=\s+[A-Za-z][A-Za-z0-9 /&\-\(\)]+:\s*|$)")
    for m in pattern.finditer(block):
        k = clean_text(m.group(1))
        v = clean_text(m.group(2))
        if k and v and len(k) <= 50 and len(v) <= 300:
            out[k] = v
    return out


# ───────────────────────── FETCHER ─────────────────────────
class HybridFetcher:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

        self._pw = None
        self._browser = None
        self._page = None

    def _fetch_requests(self, url: str) -> Tuple[Optional[str], Optional[int]]:
        for attempt in range(1, RETRY_COUNT + 1):
            try:
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
                status = resp.status_code
                ctype = resp.headers.get("Content-Type", "")
                if status == 200 and "text/html" in ctype:
                    return resp.text, status

                # Many WAFs respond 403/503 for bots
                logging.warning(f"[requests] {status} for {url} (ctype={ctype})")
                if 400 <= status < 500 and status not in (429,):
                    # likely not recoverable by retry
                    return None, status

            except requests.RequestException as ex:
                logging.warning(f"[requests] error attempt {attempt} {url}: {ex}")

            sleep_politely()

        return None, None

    def _ensure_playwright(self):
        if self._pw is not None:
            return
        try:
            from playwright.sync_api import sync_playwright  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "Playwright not installed. Install with:\n"
                "  pip install playwright\n"
                "  playwright install chromium\n"
            ) from e

        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=PLAYWRIGHT_HEADLESS)
        self._page = self._browser.new_page(user_agent=USER_AGENT, locale="en-KE")

    def _fetch_playwright(self, url: str) -> Optional[str]:
        self._ensure_playwright()
        assert self._page is not None
        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            # a little extra settle time for Woo pages
            self._page.wait_for_timeout(800)
            return self._page.content()
        except Exception as ex:
            logging.warning(f"[playwright] failed for {url}: {ex}")
            return None

    def fetch_html(self, url: str) -> Optional[str]:
        html, status = self._fetch_requests(url)
        if html:
            return html

        if USE_PLAYWRIGHT_FALLBACK and status in (403, 429, 503, None):
            logging.info(f"Falling back to Playwright for {url}")
            html2 = self._fetch_playwright(url)
            if html2:
                return html2

        return None

    def close(self):
        try:
            if self._page:
                self._page.close()
            if self._browser:
                self._browser.close()
            if self._pw:
                self._pw.stop()
        except Exception:
            pass


# ───────────────────────── PARSING ─────────────────────────
def parse_category_page(html: str, base_url: str) -> Tuple[List[str], Optional[str]]:
    soup = BeautifulSoup(html, "html.parser")

    # Collect product links
    urls = set()
    for a in soup.select('a[href*="/product/"]'):
        href = a.get("href")
        if not href:
            continue
        abs_url = urljoin(base_url, href)
        path = urlparse(abs_url).path or ""
        if "/product/" in path:
            urls.add(strip_query(abs_url))

    # Next page (WooCommerce pagination)
    next_url = None
    nxt = soup.select_one("a.next.page-numbers, nav.woocommerce-pagination a.next")
    if nxt and nxt.get("href"):
        next_url = urljoin(base_url, nxt["href"])
    else:
        rel_next = soup.find("link", attrs={"rel": "next"})
        if rel_next and rel_next.get("href"):
            next_url = urljoin(base_url, rel_next["href"])

    return sorted(urls), next_url


def extract_specs(soup: BeautifulSoup) -> Tuple[Dict[str, str], str]:
    """
    Returns (specs_dict, specs_raw_text)
    """
    specs: Dict[str, str] = {}

    # 1) Woo "Additional information" table
    table = soup.select_one("table.woocommerce-product-attributes")
    if table:
        for row in table.select("tr"):
            k = text_or_empty(row.select_one("th"))
            v = text_or_empty(row.select_one("td"))
            if k and v:
                specs[k] = v

    # 2) Also try to parse Key: Value style specs from short description
    short_desc = text_or_empty(soup.select_one("div.woocommerce-product-details__short-description"))
    if short_desc:
        specs.update({k: v for k, v in kv_from_colon_text(short_desc).items() if k not in specs})

    # 3) And from full description tab
    desc_panel = soup.select_one(
        "div.woocommerce-Tabs-panel--description, #tab-description, div#tab-description"
    )
    full_desc_text = text_or_empty(desc_panel)
    if full_desc_text:
        specs.update({k: v for k, v in kv_from_colon_text(full_desc_text).items() if k not in specs})

    specs_raw = short_desc or full_desc_text or ""
    return specs, specs_raw


def extract_variants(soup: BeautifulSoup) -> List[Dict]:
    """
    Woo variable products typically embed all variants in:
      form.variations_form[data-product_variations="...json..."]
    """
    form = soup.select_one("form.variations_form")
    if not form:
        return []

    raw = form.get("data-product_variations")
    if not raw:
        return []

    try:
        variations = json.loads(raw)
    except Exception:
        # Sometimes it is HTML-escaped or otherwise funky; try to unescape common patterns
        raw2 = raw.replace("&quot;", '"')
        try:
            variations = json.loads(raw2)
        except Exception:
            return []

    out: List[Dict] = []
    for v in variations:
        attrs = v.get("attributes") or {}
        storage = pick_attr(attrs, ["storage", "memory", "rom", "internal"])
        ram = pick_attr(attrs, ["ram"])
        out.append(
            {
                "variation_id": v.get("variation_id"),
                "sku": v.get("sku"),
                "in_stock": bool(v.get("is_in_stock")),
                "display_price": v.get("display_price"),
                "display_regular_price": v.get("display_regular_price"),
                "attributes": attrs,
                "storage": storage,
                "ram": ram,
            }
        )
    return out


def parse_product_page(html: str, product_url: str, category: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")

    name = text_or_empty(soup.select_one("h1.product_title, h1.entry-title"))
    if not name:
        # fallback to og:title
        og = soup.select_one('meta[property="og:title"]')
        if og and og.get("content"):
            name = clean_text(og["content"])

    # Price blocks
    price_el = soup.select_one("p.price, span.price")
    price_raw = text_or_empty(price_el)

    # Capture sale/regular if present
    price_regular = None
    price_current = None
    if price_el:
        del_el = price_el.select_one("del .woocommerce-Price-amount, del bdi")
        ins_el = price_el.select_one("ins .woocommerce-Price-amount, ins bdi")
        if del_el:
            nums = re.findall(r"(\d[\d,]*\.?\d*)", del_el.get_text(" ", strip=True))
            if nums:
                try:
                    price_regular = float(nums[0].replace(",", ""))
                except Exception:
                    pass
        if ins_el:
            nums = re.findall(r"(\d[\d,]*\.?\d*)", ins_el.get_text(" ", strip=True))
            if nums:
                try:
                    price_current = float(nums[0].replace(",", ""))
                except Exception:
                    pass

    # If no explicit ins/del, parse any number in price_raw as "current"
    if price_current is None and price_raw:
        nums = re.findall(r"(\d[\d,]*\.?\d*)", price_raw.replace("\u2013", "-"))
        vals = []
        for n in nums:
            try:
                vals.append(float(n.replace(",", "")))
            except Exception:
                pass
        if len(vals) == 1:
            price_current = vals[0]
        elif len(vals) >= 2:
            # Could be range. Keep min/max.
            pass

    # Compute min/max range from any numeric tokens in price_raw
    price_min = None
    price_max = None
    if price_raw:
        nums = re.findall(r"(\d[\d,]*\.?\d*)", price_raw.replace("\u2013", "-"))
        vals = []
        for n in nums:
            try:
                vals.append(float(n.replace(",", "")))
            except Exception:
                pass
        if vals:
            price_min = min(vals)
            price_max = max(vals)

    # Stock
    stock_status = "Unknown"
    in_stock = None
    stock_el = soup.select_one("p.stock, span.stock")
    if stock_el:
        st = stock_el.get_text(" ", strip=True).lower()
        if "out of stock" in st:
            stock_status = "OutOfStock"
            in_stock = False
        elif "in stock" in st:
            stock_status = "InStock"
            in_stock = True
        else:
            stock_status = clean_text(stock_el.get_text(" ", strip=True))
    else:
        # fallback: body/product class
        body = soup.select_one("body")
        if body and body.get("class"):
            cls = " ".join(body.get("class"))
            if "outofstock" in cls:
                stock_status = "OutOfStock"
                in_stock = False

    short_desc = text_or_empty(soup.select_one("div.woocommerce-product-details__short-description"))
    desc_panel = soup.select_one(
        "div.woocommerce-Tabs-panel--description, #tab-description, div#tab-description"
    )
    description = text_or_empty(desc_panel)

    specs_dict, specs_raw = extract_specs(soup)
    variants = extract_variants(soup)

    # If product-level stock unknown, infer from variants when present
    if in_stock is None and variants:
        in_stock = any(v.get("in_stock") for v in variants)
        stock_status = "InStock" if in_stock else "OutOfStock"

    return {
        "category": category,
        "product_url": strip_query(product_url),
        "name": name,
        "currency": CURRENCY_DEFAULT,
        "price_raw": price_raw,
        "price_current": price_current,
        "price_regular": price_regular,
        "price_min": price_min,
        "price_max": price_max,
        "in_stock": in_stock,
        "stock_status": stock_status,
        "short_description": short_desc,
        "description": description,
        "specs_json": json.dumps(specs_dict, ensure_ascii=False),
        "specs_raw": specs_raw,
        "variants_json": json.dumps(variants, ensure_ascii=False),
        "variants": variants,  # keep list for variant-table flattening
    }


# ───────────────────────── SCRAPE LOGIC ─────────────────────────
def scrape_category(fetcher: HybridFetcher, category: str, start_url: str) -> List[str]:
    all_urls: List[str] = []
    seen = set()

    url = start_url
    pages = 0

    while url and pages < MAX_PAGES_PER_CATEGORY:
        pages += 1
        logging.info(f"[{category}] category page {pages}: {url}")
        html = fetcher.fetch_html(url)
        if not html:
            logging.warning(f"[{category}] no html for {url}")
            break

        urls, next_url = parse_category_page(html, start_url)
        new_urls = [u for u in urls if u not in seen]
        all_urls.extend(new_urls)
        for u in new_urls:
            seen.add(u)

        if not next_url or not new_urls:
            # If no next page OR no new products found, stop
            url = next_url
            if not next_url:
                break
        else:
            url = next_url

        sleep_politely()

    logging.info(f"[{category}] discovered {len(all_urls)} product urls")
    return all_urls


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


def ensure_table_products(client: bigquery.Client, dataset_id: str, table_id: str) -> bigquery.Table:
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    schema = [
        bigquery.SchemaField("ts", "TIMESTAMP"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("product_url", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("price_raw", "STRING"),
        bigquery.SchemaField("price_current", "FLOAT"),
        bigquery.SchemaField("price_regular", "FLOAT"),
        bigquery.SchemaField("price_min", "FLOAT"),
        bigquery.SchemaField("price_max", "FLOAT"),
        bigquery.SchemaField("in_stock", "BOOLEAN"),
        bigquery.SchemaField("stock_status", "STRING"),
        bigquery.SchemaField("short_description", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("specs_json", "STRING"),
        bigquery.SchemaField("specs_raw", "STRING"),
        bigquery.SchemaField("variants_json", "STRING"),
        bigquery.SchemaField("scrape_error", "STRING"),
    ]
    try:
        return client.get_table(table_ref)
    except NotFound:
        logging.info(f"Creating table {table_ref}")
        table = bigquery.Table(table_ref, schema=schema)
        return client.create_table(table, exists_ok=True)
    except Conflict:
        return client.get_table(table_ref)


def ensure_table_variants(client: bigquery.Client, dataset_id: str, table_id: str) -> bigquery.Table:
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    schema = [
        bigquery.SchemaField("ts", "TIMESTAMP"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("product_url", "STRING"),
        bigquery.SchemaField("product_name", "STRING"),
        bigquery.SchemaField("variation_id", "STRING"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("storage", "STRING"),
        bigquery.SchemaField("ram", "STRING"),
        bigquery.SchemaField("price", "FLOAT"),
        bigquery.SchemaField("regular_price", "FLOAT"),
        bigquery.SchemaField("in_stock", "BOOLEAN"),
        bigquery.SchemaField("attributes_json", "STRING"),
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

    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    load_job = client.load_table_from_json(rows, table_ref, job_config=job_config, location=BQ_LOCATION)
    result = load_job.result()
    return result.output_rows or len(rows)


# ───────────────────────── RUN ─────────────────────────
def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    ts = ts_now_utc()

    fetcher = HybridFetcher()

    try:
        # 1) Discover product URLs
        cat_to_urls: Dict[str, List[str]] = {}
        for cat, url in CATEGORY_URLS.items():
            cat_to_urls[cat] = scrape_category(fetcher, cat, url)

        # 2) Scrape each product page
        products_rows: List[Dict] = []
        variants_rows: List[Dict] = []

        seen_products = set()

        for cat, urls in cat_to_urls.items():
            for purl in urls:
                if purl in seen_products:
                    continue
                seen_products.add(purl)

                logging.info(f"[{cat}] product: {purl}")
                html = fetcher.fetch_html(purl)
                if not html:
                    products_rows.append({
                        "ts": ts,
                        "category": cat,
                        "product_url": purl,
                        "name": "",
                        "currency": CURRENCY_DEFAULT,
                        "price_raw": "",
                        "price_current": None,
                        "price_regular": None,
                        "price_min": None,
                        "price_max": None,
                        "in_stock": None,
                        "stock_status": "Unknown",
                        "short_description": "",
                        "description": "",
                        "specs_json": "{}",
                        "specs_raw": "",
                        "variants_json": "[]",
                        "scrape_error": "fetch_failed",
                    })
                    sleep_politely()
                    continue

                item = parse_product_page(html, purl, cat)

                products_rows.append({
                    "ts": ts,
                    "category": item["category"],
                    "product_url": item["product_url"],
                    "name": item["name"],
                    "currency": item["currency"],
                    "price_raw": item["price_raw"],
                    "price_current": item["price_current"],
                    "price_regular": item["price_regular"],
                    "price_min": item["price_min"],
                    "price_max": item["price_max"],
                    "in_stock": item["in_stock"],
                    "stock_status": item["stock_status"],
                    "short_description": item["short_description"],
                    "description": item["description"],
                    "specs_json": item["specs_json"],
                    "specs_raw": item["specs_raw"],
                    "variants_json": item["variants_json"],
                    "scrape_error": "",
                })

                # Flatten variants
                for v in item.get("variants") or []:
                    variants_rows.append({
                        "ts": ts,
                        "category": cat,
                        "product_url": item["product_url"],
                        "product_name": item["name"],
                        "variation_id": str(v.get("variation_id") or ""),
                        "sku": v.get("sku") or "",
                        "storage": v.get("storage") or "",
                        "ram": v.get("ram") or "",
                        "price": v.get("display_price"),
                        "regular_price": v.get("display_regular_price"),
                        "in_stock": v.get("in_stock"),
                        "attributes_json": json.dumps(v.get("attributes") or {}, ensure_ascii=False),
                    })

                sleep_politely()

        logging.info(f"Collected products: {len(products_rows)} rows")
        logging.info(f"Collected variants: {len(variants_rows)} rows")

        # 3) Load to BigQuery
        client = get_bq_client()
        ensure_table_products(client, BQ_DATASET, BQ_PRODUCTS_TABLE)
        ensure_table_variants(client, BQ_DATASET, BQ_VARIANTS_TABLE)

        inserted_p = bq_append_rows(client, BQ_DATASET, BQ_PRODUCTS_TABLE, products_rows)
        inserted_v = bq_append_rows(client, BQ_DATASET, BQ_VARIANTS_TABLE, variants_rows)

        logging.info(f"Appended {inserted_p} to {client.project}.{BQ_DATASET}.{BQ_PRODUCTS_TABLE}")
        logging.info(f"Appended {inserted_v} to {client.project}.{BQ_DATASET}.{BQ_VARIANTS_TABLE}")

    finally:
        fetcher.close()


if __name__ == "__main__":
    run()
