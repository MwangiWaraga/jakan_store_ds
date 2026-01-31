import json
import logging
import random
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup
from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict

# ───────────────────────── BIGQUERY CONFIG ─────────────────────────
GCP_PROJECT_ID = "jakan-group"
BQ_DATASET = "core"
BQ_TABLE = "phoneplace_products_raw_bqt"
BQ_LOCATION = "europe-west1"

# ───────────────────────── SCRAPER CONFIG ─────────────────────────
BRAND_CATEGORY_URLS: Dict[str, str] = {
    "infinix": "https://www.phoneplacekenya.com/product-category/smartphones/infinix-phones-in-kenya/",
    "tecno": "https://www.phoneplacekenya.com/product-category/smartphones/tecno-phones/",
    "itel": "https://www.phoneplacekenya.com/product-category/smartphones/itel/",
}
# Easy to add more:
# BRAND_CATEGORY_URLS["samsung"] = "https://www.phoneplacekenya.com/product-category/smartphones/samsung/"

PLAYWRIGHT_HEADLESS = True
DELAY_RANGE = (0.7, 1.4)
MAX_PAGES_PER_BRAND = 120

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

# ───────────────────────── UTILS ─────────────────────────
def ts_now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def sleep_politely():
    time.sleep(random.uniform(*DELAY_RANGE))


def clean_text(s: str) -> str:
    s = s or ""
    return re.sub(r"\s+", " ", s).strip()


def text_or_empty(el) -> str:
    return clean_text(el.get_text(" ", strip=True)) if el else ""


def strip_query(url: str) -> str:
    try:
        p = urlparse(url)
        return p._replace(query="", fragment="").geturl()
    except Exception:
        return url


# ───────────────────────── PLAYWRIGHT FETCHER ─────────────────────────
class PWFetcher:
    def __init__(self):
        self._pw = None
        self._browser = None
        self._context = None
        self._page = None

    def _ensure(self):
        if self._pw is not None:
            return

        try:
            from playwright.sync_api import sync_playwright  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "Playwright not installed.\n"
                "Install:\n"
                "  pip install playwright\n"
                "  playwright install chromium\n"
            ) from e

        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=PLAYWRIGHT_HEADLESS)

        self._context = self._browser.new_context(
            user_agent=USER_AGENT,
            locale="en-KE",
            viewport={"width": 1365, "height": 768},
            extra_http_headers={"Accept-Language": "en-KE,en;q=0.9"},
        )
        self._page = self._context.new_page()
        self._page.set_default_timeout(25_000)

        # Speed: block heavy assets (safe for HTML parsing)
        def _route(route, request):
            if request.resource_type in ("image", "media", "font", "stylesheet"):
                return route.abort()
            return route.continue_()

        self._page.route("**/*", _route)

    def fetch_html(self, url: str, wait_selector: Optional[str] = None) -> Optional[str]:
        self._ensure()
        assert self._page is not None

        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=60_000)

            if wait_selector:
                try:
                    self._page.wait_for_selector(wait_selector, timeout=20_000)
                except Exception:
                    pass

            # tiny settle
            self._page.wait_for_timeout(300)
            return self._page.content()
        except Exception as ex:
            logging.warning(f"[playwright] failed for {url}: {ex}")
            return None

    def close(self):
        try:
            if self._page:
                self._page.close()
            if self._context:
                self._context.close()
            if self._browser:
                self._browser.close()
            if self._pw:
                self._pw.stop()
        except Exception:
            pass


# ───────────────────────── CATEGORY PARSING ─────────────────────────
def parse_category_page(html: str, base_url: str) -> Tuple[List[str], Optional[str]]:
    """
    Returns (product_urls, next_page_url)
    Robust enough for theme variations.
    """
    soup = BeautifulSoup(html, "html.parser")
    main = soup.select_one("main") or soup

    # collect product links
    loose = []
    strict = []

    for a in main.select('a[href*="/product/"]'):
        href = a.get("href")
        if not href:
            continue

        abs_url = strip_query(urljoin(base_url, href))
        path = urlparse(abs_url).path or ""
        if "/product/" not in path:
            continue
        if "add-to-cart" in abs_url:
            continue
        if a.find_parent(["header", "footer", "nav"]):
            continue

        loose.append(abs_url)

        # strict heuristic: link appears inside some container with class containing 'product'
        is_tile = False
        for parent in a.parents:
            if getattr(parent, "name", None) in (None,):
                continue
            if parent.name in ("main", "body", "html"):
                break
            cls = " ".join(parent.get("class", [])).lower() if parent.has_attr("class") else ""
            if "product-category" in cls:
                is_tile = False
                break
            if "product" in cls:
                is_tile = True
                break
        if is_tile:
            strict.append(abs_url)

    urls = strict if len(set(strict)) >= 6 else loose
    urls = sorted(set(urls))

    # next page link (Woo patterns)
    next_url = None
    nxt = soup.select_one("a.next.page-numbers, nav.woocommerce-pagination a.next, a.next")
    if nxt and nxt.get("href"):
        next_url = urljoin(base_url, nxt["href"])
    else:
        rel_next = soup.find("link", attrs={"rel": "next"})
        if rel_next and rel_next.get("href"):
            next_url = urljoin(base_url, rel_next["href"])

    return urls, next_url


def scrape_brand_urls(fetcher: PWFetcher, brand: str, start_url: str) -> List[str]:
    all_urls: List[str] = []
    seen = set()

    url = start_url
    page = 0

    while url and page < MAX_PAGES_PER_BRAND:
        page += 1
        logging.info(f"[{brand}] category page {page}: {url}")

        html = fetcher.fetch_html(url, wait_selector='a[href*="/product/"]')
        if not html:
            logging.warning(f"[{brand}] no html for {url}")
            break

        urls, next_url = parse_category_page(html, start_url)
        new_urls = [u for u in urls if u not in seen]
        for u in new_urls:
            seen.add(u)
        all_urls.extend(new_urls)

        # stop if we’re not finding anything
        logging.info(f"[{brand}] page {page}: found {len(urls)} urls, new {len(new_urls)}")
        if page > 1 and len(urls) == 0:
            break

        url = next_url
        sleep_politely()

    logging.info(f"[{brand}] discovered {len(all_urls)} product urls")
    return all_urls


# ───────────────────────── PRODUCT PARSING ─────────────────────────
def parse_key_features(soup: BeautifulSoup) -> List[str]:
    features: List[str] = []
    key_node = soup.find(string=re.compile(r"\bKey Features\b", re.IGNORECASE))
    if key_node:
        tag = key_node.parent if hasattr(key_node, "parent") else None
        ul = tag.find_next("ul") if tag else None
        if ul:
            for li in ul.find_all("li"):
                t = text_or_empty(li)
                if t:
                    features.append(t)

    # fallback: best UL by ":" density
    if not features:
        uls = soup.find_all("ul")
        best = None
        best_score = 0
        for ul in uls:
            lis = [text_or_empty(li) for li in ul.find_all("li")]
            lis = [x for x in lis if x]
            if len(lis) < 3:
                continue
            score = sum(1 for x in lis if ":" in x)
            if score > best_score:
                best_score = score
                best = lis
        if best:
            features = best

    return features


def detect_in_stock(soup: BeautifulSoup) -> Optional[bool]:
    txt = soup.get_text(" ", strip=True).lower()
    if "sold out" in txt or "out of stock" in txt:
        return False
    if re.search(r"\bin stock\b", txt):
        return True
    return None


def parse_price_raw(soup: BeautifulSoup) -> str:
    price_el = soup.select_one("p.price, span.price")
    return text_or_empty(price_el)


def extract_variants_json(soup: BeautifulSoup) -> str:
    """
    Returns a JSON string with:
      { "variations": [...], "options": {...} }
    so it’s always the same shape.
    """
    variations = []
    options = {}

    form = soup.select_one("form.variations_form")

    # variations embedded (best case)
    if form and form.get("data-product_variations"):
        raw = form.get("data-product_variations")
        try:
            variations = json.loads(raw)
        except Exception:
            raw2 = (raw or "").replace("&quot;", '"')
            try:
                variations = json.loads(raw2)
            except Exception:
                variations = []

    # dropdown options (fallback)
    if form:
        for sel in form.select("select"):
            name = sel.get("name") or sel.get("id") or ""
            name = clean_text(name)
            if not name:
                continue
            vals = []
            for opt in sel.select("option"):
                t = clean_text(opt.get_text(" ", strip=True))
                if not t or "choose an option" in t.lower():
                    continue
                vals.append(t)
            if vals:
                options[name] = vals

    payload = {"variations": variations, "options": options}
    return json.dumps(payload, ensure_ascii=False)


def parse_description(soup: BeautifulSoup) -> str:
    # full description tab if exists
    desc_panel = soup.select_one("div#tab-description, #tab-description, .woocommerce-Tabs-panel--description")
    desc = text_or_empty(desc_panel)
    if desc:
        return desc
    # otherwise short description
    short_desc = soup.select_one("div.woocommerce-product-details__short-description")
    return text_or_empty(short_desc)


def extract_breadcrumbs_text(soup: BeautifulSoup) -> str:
    nav = soup.select_one("nav.woocommerce-breadcrumb, .woocommerce-breadcrumb")
    if not nav:
        return ""
    return clean_text(nav.get_text(" ", strip=True))


def brand_matches_page(soup: BeautifulSoup, expected_brand: str) -> bool:
    """
    Prevent mis-labeling (e.g., Apple adapters showing in infinix category).
    If breadcrumb contains the expected brand word, we accept.
    If no breadcrumb found, we accept (don’t over-filter).
    """
    crumb = extract_breadcrumbs_text(soup)
    if not crumb:
        return True
    return expected_brand.lower() in crumb.lower()


def parse_product(html: str, brand: str, product_url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")

    name = text_or_empty(soup.select_one("h1.product_title, h1.entry-title, h1"))
    in_stock = detect_in_stock(soup)
    price_raw = parse_price_raw(soup)
    specs = parse_key_features(soup)
    description = parse_description(soup)
    variants_json = extract_variants_json(soup)

    return {
        "brand": brand,
        "product_url": strip_query(product_url),
        "name": name,
        "price_raw": price_raw,
        "in_stock": in_stock,
        "specs_json": json.dumps(specs, ensure_ascii=False),
        "description": description,
        "variants_json": variants_json,
    }


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
        bigquery.SchemaField("brand", "STRING"),
        bigquery.SchemaField("product_url", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("price_raw", "STRING"),
        bigquery.SchemaField("in_stock", "BOOLEAN"),
        bigquery.SchemaField("specs_json", "STRING"),
        bigquery.SchemaField("description", "STRING"),
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


def bq_replace_rows(client: bigquery.Client, dataset_id: str, table_id: str, rows: List[Dict]) -> int:
    """
    WRITE_TRUNCATE (fresh snapshot).
    Safe guard: only run if rows > 0.
    """
    if not rows:
        return 0

    ensure_dataset(client, dataset_id)
    ensure_table(client, dataset_id, table_id)

    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        ignore_unknown_values=True,
    )

    job = client.load_table_from_json(rows, table_ref, job_config=job_config, location=BQ_LOCATION)
    res = job.result()
    return res.output_rows or len(rows)


# ───────────────────────── RUN ─────────────────────────
def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    ts = ts_now_utc()
    fetcher = PWFetcher()

    try:
        # 1) discover urls per brand
        brand_to_urls: Dict[str, List[str]] = {}
        for brand, url in BRAND_CATEGORY_URLS.items():
            brand_to_urls[brand] = scrape_brand_urls(fetcher, brand, url)

        # 2) scrape products
        rows: List[Dict] = []
        seen = set()

        for brand, urls in brand_to_urls.items():
            for purl in urls:
                if purl in seen:
                    continue
                seen.add(purl)

                logging.info(f"[{brand}] product: {purl}")
                html = fetcher.fetch_html(purl, wait_selector="h1")
                if not html:
                    rows.append({
                        "ts": ts,
                        "brand": brand,
                        "product_url": purl,
                        "name": "",
                        "price_raw": "",
                        "in_stock": None,
                        "specs_json": "[]",
                        "description": "",
                        "variants_json": json.dumps({"variations": [], "options": {}}, ensure_ascii=False),
                        "scrape_error": "fetch_failed",
                    })
                    sleep_politely()
                    continue

                try:
                    soup = BeautifulSoup(html, "html.parser")

                    # small accuracy+efficiency win:
                    # skip unrelated products incorrectly listed under the brand category
                    if not brand_matches_page(soup, brand):
                        logging.info(f"[{brand}] skipped (breadcrumb mismatch): {purl}")
                        sleep_politely()
                        continue

                    item = parse_product(html, brand, purl)
                    rows.append({
                        "ts": ts,
                        **item,
                        "scrape_error": "",
                    })

                except Exception as ex:
                    logging.exception(f"Parse failed for {purl}: {ex}")
                    rows.append({
                        "ts": ts,
                        "brand": brand,
                        "product_url": purl,
                        "name": "",
                        "price_raw": "",
                        "in_stock": None,
                        "specs_json": "[]",
                        "description": "",
                        "variants_json": json.dumps({"variations": [], "options": {}}, ensure_ascii=False),
                        "scrape_error": "parse_failed",
                    })

                sleep_politely()

        logging.info(f"Collected rows: {len(rows)}")

        # 3) load to BQ (truncate snapshot)
        if rows:
            client = get_bq_client()
            inserted = bq_replace_rows(client, BQ_DATASET, BQ_TABLE, rows)
            logging.info(f"WRITE_TRUNCATE loaded {inserted} rows into {client.project}.{BQ_DATASET}.{BQ_TABLE}")
        else:
            logging.warning("0 rows collected -> skipping WRITE_TRUNCATE to avoid wiping previous data.")

    finally:
        fetcher.close()


if __name__ == "__main__":
    run()
