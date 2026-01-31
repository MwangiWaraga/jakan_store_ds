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
from google.api_core.exceptions import NotFound

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
# Add more easily:
# BRAND_CATEGORY_URLS["samsung"] = "https://www.phoneplacekenya.com/product-category/smartphones/samsung/"

PLAYWRIGHT_HEADLESS = True
DELAY_RANGE = (0.7, 1.4)
MAX_PAGES_PER_BRAND = 120

# Extra safety: if category scraping ever leaks items again, this filters them out
# (cheap, because you’re fetching the product page anyway).
FILTER_BY_BREADCRUMB = True

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


def price_text_clean(price_el) -> str:
    """
    Returns clean visible price string:
    - Range: "KSh 13,500 - KSh 17,000"
    - Sale:  "KSh 40,999 -> KSh 38,000"
    - Single:"KSh 32,999"
    """
    if not price_el:
        return ""

    # remove screen-reader text to avoid "Original price was..."
    for sr in price_el.select(".screen-reader-text"):
        sr.decompose()

    del_el = price_el.select_one("del bdi, del .woocommerce-Price-amount")
    ins_el = price_el.select_one("ins bdi, ins .woocommerce-Price-amount")
    if del_el and ins_el:
        return f"{clean_text(del_el.get_text(' ', strip=True))} -> {clean_text(ins_el.get_text(' ', strip=True))}"

    bdis = [clean_text(b.get_text(" ", strip=True)) for b in price_el.select("bdi")]
    bdis = [b for b in bdis if b]
    uniq = []
    seen = set()
    for b in bdis:
        if b not in seen:
            uniq.append(b)
            seen.add(b)
    if len(uniq) >= 2:
        return " - ".join(uniq)

    return clean_text(price_el.get_text(" ", strip=True))


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

        # Speed: block heavy assets (DON'T block stylesheet; that change caused issues on some themes)
        def _route(route, request):
            if request.resource_type in ("image", "media", "font"):
                return route.abort()
            return route.continue_()

        self._page.route("**/*", _route)

    def fetch_html(self, url: str) -> Optional[str]:
        self._ensure()
        assert self._page is not None
        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=60_000)
            # small settle; no strict selector wait (prevents 20s timeouts)
            self._page.wait_for_timeout(250)
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
def remove_sidebars(root: BeautifulSoup):
    """
    Removes sidebar containers so we don’t capture the "Latest Products" widget.
    """
    for bad in root.select("aside, .sidebar, #sidebar, .widget-area, .product_list_widget"):
        bad.decompose()


def find_best_grid_container(root: BeautifulSoup):
    """
    Pick the container with the most /product/ links (after sidebars removed).
    Works across themes.
    """
    # common Woo selectors first
    for sel in ["ul.products", "div.products", "section.products"]:
        cand = root.select_one(sel)
        if cand:
            return cand

    # heuristic: container with max product links
    best = None
    best_count = 0
    for cand in root.select("ul,div,section"):
        links = cand.select('a[href*="/product/"]')
        cnt = 0
        for a in links:
            href = a.get("href") or ""
            if "/product/" in href and "add-to-cart" not in href:
                cnt += 1
        if cnt > best_count:
            best_count = cnt
            best = cand

    return best or root


def extract_product_links_and_prices(html: str, category_url: str) -> Tuple[Dict[str, str], Optional[str]]:
    """
    Returns:
      {product_url: price_raw_from_category_tile}, next_page_url
    """
    soup = BeautifulSoup(html, "html.parser")
    root = soup.select_one("main") or soup

    # remove sidebar widget(s) before extracting
    remove_sidebars(root)

    # also avoid header/footer/nav contamination
    for bad in root.select("header, footer, nav"):
        bad.decompose()

    grid = find_best_grid_container(root)

    price_map: Dict[str, str] = {}

    # collect urls
    anchors = grid.select('a[href*="/product/"]')
    logging.info(f"[debug] raw product-like anchors in grid: {len(anchors)}")

    for a in anchors:
        href = a.get("href")
        if not href:
            continue
        abs_url = strip_query(urljoin(category_url, href))
        path = urlparse(abs_url).path or ""
        if "/product/" not in path:
            continue
        if "add-to-cart" in abs_url:
            continue

        # find a "tile" around the link that has a price element
        tile = None
        for parent in a.parents:
            if getattr(parent, "name", None) in ("li", "article", "div", "section"):
                if parent.select_one("span.price, p.price, .price"):
                    tile = parent
                    break
            if getattr(parent, "name", None) in ("main", "body", "html"):
                break

        price_el = tile.select_one("span.price, p.price, .price") if tile else None
        price_raw = price_text_clean(price_el)

        if abs_url not in price_map:
            price_map[abs_url] = price_raw

    # pagination next
    next_url = None
    nxt = soup.select_one("a.next.page-numbers, nav.woocommerce-pagination a.next, a.next")
    if nxt and nxt.get("href"):
        next_url = urljoin(category_url, nxt["href"])
    else:
        rel_next = soup.find("link", attrs={"rel": "next"})
        if rel_next and rel_next.get("href"):
            next_url = urljoin(category_url, rel_next["href"])

    return price_map, next_url


def scrape_brand_urls_with_prices(fetcher: PWFetcher, brand: str, start_url: str) -> Dict[str, str]:
    url = start_url
    page = 0
    out: Dict[str, str] = {}

    while url and page < MAX_PAGES_PER_BRAND:
        page += 1
        logging.info(f"[{brand}] category page {page}: {url}")

        html = fetcher.fetch_html(url)
        if not html:
            logging.warning(f"[{brand}] no html for {url}")
            break

        price_map, next_url = extract_product_links_and_prices(html, start_url)

        new_count = 0
        for purl, pprice in price_map.items():
            if purl not in out:
                out[purl] = pprice
                new_count += 1

        logging.info(f"[{brand}] page {page}: found {len(price_map)} urls, new {new_count}")

        if page > 1 and new_count == 0:
            break

        url = next_url
        sleep_politely()

    logging.info(f"[{brand}] discovered {len(out)} product urls")
    return out


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


def parse_description(soup: BeautifulSoup) -> str:
    desc_panel = soup.select_one("div#tab-description, #tab-description, .woocommerce-Tabs-panel--description")
    desc = text_or_empty(desc_panel)
    if desc:
        return desc
    short_desc = soup.select_one("div.woocommerce-product-details__short-description")
    return text_or_empty(short_desc)


def extract_variants_json(soup: BeautifulSoup) -> str:
    """
    Always returns:
      { "variations": [...], "options": {...} }
    """
    variations = []
    options = {}

    form = soup.select_one("form.variations_form")
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

    return json.dumps({"variations": variations, "options": options}, ensure_ascii=False)


def breadcrumb_text(soup: BeautifulSoup) -> str:
    nav = soup.select_one("nav.woocommerce-breadcrumb, .woocommerce-breadcrumb")
    if not nav:
        return ""
    return clean_text(nav.get_text(" ", strip=True))


def parse_product(html: str, brand: str, product_url: str, category_price_raw: str) -> Optional[Dict]:
    soup = BeautifulSoup(html, "html.parser")

    if FILTER_BY_BREADCRUMB:
        bc = breadcrumb_text(soup)
        # If breadcrumbs exist and do NOT mention the brand, skip as cross-sell leakage.
        if bc and brand.lower() not in bc.lower():
            return None

    name = text_or_empty(soup.select_one("h1.product_title, h1.entry-title, h1"))
    in_stock = detect_in_stock(soup)
    specs = parse_key_features(soup)
    description = parse_description(soup)
    variants_json = extract_variants_json(soup)

    # price: prefer category tile (range/sale), fallback to product page visible price
    price_raw = category_price_raw or ""
    if not price_raw:
        price_el = soup.select_one("p.price, span.price, .price")
        price_raw = price_text_clean(price_el)

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


def bq_write_truncate(client: bigquery.Client, dataset_id: str, table_id: str, rows: List[Dict]) -> int:
    # Guard: don’t wipe last good snapshot if this run failed
    if not rows:
        logging.warning("0 rows collected -> skipping WRITE_TRUNCATE to avoid wiping previous data.")
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
        # 1) collect product urls + category prices
        brand_to_price_map: Dict[str, Dict[str, str]] = {}
        for brand, cat_url in BRAND_CATEGORY_URLS.items():
            brand_to_price_map[brand] = scrape_brand_urls_with_prices(fetcher, brand, cat_url)

        # 2) scrape product pages
        rows: List[Dict] = []
        seen = set()

        for brand, price_map in brand_to_price_map.items():
            for purl, cat_price in price_map.items():
                if purl in seen:
                    continue
                seen.add(purl)

                logging.info(f"[{brand}] product: {purl}")
                html = fetcher.fetch_html(purl)
                if not html:
                    rows.append({
                        "ts": ts,
                        "brand": brand,
                        "product_url": purl,
                        "name": "",
                        "price_raw": cat_price or "",
                        "in_stock": None,
                        "specs_json": "[]",
                        "description": "",
                        "variants_json": json.dumps({"variations": [], "options": {}}, ensure_ascii=False),
                        "scrape_error": "fetch_failed",
                    })
                    sleep_politely()
                    continue

                try:
                    item = parse_product(html, brand, purl, cat_price)
                    if item is None:
                        # breadcrumb mismatch -> skip leakage
                        logging.info(f"[{brand}] skipped (breadcrumb mismatch): {purl}")
                        sleep_politely()
                        continue

                    rows.append({"ts": ts, **item, "scrape_error": ""})

                except Exception as ex:
                    logging.exception(f"Parse failed for {purl}: {ex}")
                    rows.append({
                        "ts": ts,
                        "brand": brand,
                        "product_url": purl,
                        "name": "",
                        "price_raw": cat_price or "",
                        "in_stock": None,
                        "specs_json": "[]",
                        "description": "",
                        "variants_json": json.dumps({"variations": [], "options": {}}, ensure_ascii=False),
                        "scrape_error": "parse_failed",
                    })

                sleep_politely()

        logging.info(f"Collected rows: {len(rows)}")

        client = get_bq_client()
        inserted = bq_write_truncate(client, BQ_DATASET, BQ_TABLE, rows)
        if inserted:
            logging.info(f"WRITE_TRUNCATE loaded {inserted} rows into {client.project}.{BQ_DATASET}.{BQ_TABLE}")

    finally:
        fetcher.close()


if __name__ == "__main__":
    run()
