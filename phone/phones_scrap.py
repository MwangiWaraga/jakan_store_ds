import json
import logging
import random
import re
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from google.cloud import bigquery
from google.api_core.exceptions import NotFound, Conflict

# ───────────────────────── BIGQUERY CONFIG ─────────────────────────
GCP_PROJECT_ID = "jakan-group"
BQ_DATASET = "core"
BQ_PRODUCTS_TABLE = "phoneplace_products_raw_bqt"
BQ_VARIANTS_TABLE = "phoneplace_variants_raw_bqt"
BQ_LOCATION = "europe-west1"

# ───────────────────────── SCRAPER CONFIG ─────────────────────────
CATEGORY_URLS: Dict[str, str] = {
    "infinix": "https://www.phoneplacekenya.com/product-category/smartphones/infinix-phones-in-kenya/",
    "tecno": "https://www.phoneplacekenya.com/product-category/smartphones/tecno-phones/",
    "itel": "https://www.phoneplacekenya.com/product-category/smartphones/itel/",
}
# Easy to add more:
# CATEGORY_URLS["samsung"] = "https://www.phoneplacekenya.com/product-category/smartphones/samsung/"

FETCH_MODE = "playwright"  # "playwright" recommended (requests gets 403)
PLAYWRIGHT_HEADLESS = True

REQUEST_TIMEOUT = 25
RETRY_COUNT = 2
DELAY_RANGE = (0.8, 1.6)
MAX_PAGES_PER_CATEGORY = 120

CURRENCY_DEFAULT = "KES"

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


def first_match_float(s: str) -> Optional[float]:
    if not s:
        return None
    nums = re.findall(r"(\d[\d,]*\.?\d*)", s)
    if not nums:
        return None
    try:
        return float(nums[0].replace(",", ""))
    except Exception:
        return None


def all_floats(s: str) -> List[float]:
    if not s:
        return []
    nums = re.findall(r"(\d[\d,]*\.?\d*)", s.replace("\u2013", "-"))
    out = []
    for n in nums:
        try:
            out.append(float(n.replace(",", "")))
        except Exception:
            pass
    return out


def pick_attr(attrs: Dict[str, str], keywords: List[str]) -> str:
    for k, v in (attrs or {}).items():
        lk = (k or "").lower()
        if any(kw in lk for kw in keywords):
            return v or ""
    return ""


# ───────────────────────── FETCHER ─────────────────────────
class HybridFetcher:
    """
    Playwright-first fetcher.
    Blocks images/fonts/media for speed.
    """

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)

        self._pw = None
        self._browser = None
        self._context = None
        self._page = None

    def _ensure_playwright(self):
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

        # Speed: block heavy assets
        def _route_handler(route, request):
            if request.resource_type in ("image", "media", "font"):
                return route.abort()
            return route.continue_()

        self._page.route("**/*", _route_handler)

    def fetch_html_playwright(self, url: str, wait_selector: Optional[str] = None) -> Optional[str]:
        self._ensure_playwright()
        assert self._page is not None

        try:
            self._page.goto(url, wait_until="domcontentloaded", timeout=60_000)

            # Give JS time; try a networkidle settle (safe-guarded)
            try:
                self._page.wait_for_load_state("networkidle", timeout=10_000)
            except Exception:
                pass

            if wait_selector:
                try:
                    self._page.wait_for_selector(wait_selector, timeout=20_000)
                except Exception:
                    pass

            self._page.wait_for_timeout(500)
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


# ───────────────────────── CATEGORY PARSING (ROBUST) ─────────────────────────
def extract_product_links_from(container, base_url: str) -> List[str]:
    """
    Extract /product/ links from a container.
    Prefer links that appear inside a product tile (ancestor class contains 'product'
    but not 'product-category').
    """
    urls = set()

    for a in container.select('a[href*="/product/"]'):
        href = a.get("href")
        if not href:
            continue

        abs_url = strip_query(urljoin(base_url, href))
        path = urlparse(abs_url).path or ""
        if "/product/" not in path:
            continue
        if "add-to-cart" in abs_url:
            continue

        # Exclude header/footer/nav areas if they leak into container
        if a.find_parent(["header", "footer", "nav"]):
            continue

        # Must look like it's in a product tile
        is_tile = False
        for parent in a.parents:
            if getattr(parent, "name", None) in (None,):
                continue
            if parent.name in ("main", "body", "html"):
                break
            cls = " ".join(parent.get("class", [])).lower() if parent.has_attr("class") else ""
            if "product-category" in cls:
                # categories list, not products
                is_tile = False
                break
            if "product" in cls:
                is_tile = True
                break

        if is_tile:
            urls.add(abs_url)

    # If we got nothing with strict means, loosen within the same container
    if not urls:
        for a in container.select('a[href*="/product/"]'):
            href = a.get("href")
            if not href:
                continue
            abs_url = strip_query(urljoin(base_url, href))
            path = urlparse(abs_url).path or ""
            if "/product/" in path and "add-to-cart" not in abs_url:
                if not a.find_parent(["header", "footer", "nav"]):
                    urls.add(abs_url)

    return sorted(urls)


def parse_category_product_urls(html: str, base_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")

    # Prefer main content to avoid menu/footer contamination
    main = (
        soup.select_one("main")
        or soup.select_one("#primary")
        or soup.select_one("div.site-main")
        or soup.select_one("div.content-area")
        or soup
    )

    # Try common grid wrappers first (but not required)
    for sel in ["ul.products", "div.products", "section.products", "div.woocommerce"]:
        grid = main.select_one(sel)
        if grid:
            urls = extract_product_links_from(grid, base_url)
            if urls:
                return urls

    # Fallback: extract from main
    return extract_product_links_from(main, base_url)


def discover_pagination_urls(html: str, category_url: str) -> List[str]:
    """
    Generates page URLs if it finds Woo-like pagination.
    If it cannot confidently detect pagination, returns [category_url].
    """
    soup = BeautifulSoup(html, "html.parser")
    base = strip_query(category_url)

    nav = soup.select_one("nav.woocommerce-pagination, .woocommerce-pagination, nav.pagination, .pagination")
    if not nav:
        return [base]

    links = [urljoin(base, a.get("href")) for a in nav.select("a[href]")]
    page_nums = []
    sample = None

    for h in links:
        h = strip_query(h)
        m1 = re.search(r"/page/(\d+)/?", h)
        if m1:
            n = int(m1.group(1))
            page_nums.append(n)
            if n == 2:
                sample = h
            continue

        m2 = re.search(r"[?&](paged|product-page|page)=(\d+)", h)
        if m2:
            n = int(m2.group(2))
            page_nums.append(n)
            if n == 2:
                sample = h

    if not page_nums:
        return [base]

    max_page = max(page_nums)
    urls = [base]

    if sample:
        if "/page/" in sample:
            tmpl = re.sub(r"/page/\d+/?", r"/page/{page}/", sample)
            for p in range(2, max_page + 1):
                urls.append(tmpl.format(page=p))
        else:
            tmpl = re.sub(r"([?&](?:paged|product-page|page)=)\d+", r"\g<1>{page}", sample)
            for p in range(2, max_page + 1):
                urls.append(tmpl.format(page=p))

    # de-dupe, cap
    out, seen = [], set()
    for u in urls:
        u = strip_query(u)
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out[:MAX_PAGES_PER_CATEGORY]


def scrape_category(fetcher: HybridFetcher, category: str, start_url: str) -> List[str]:
    logging.info(f"[{category}] category page 1: {start_url}")
    html = fetcher.fetch_html_playwright(start_url, wait_selector='main a[href*="/product/"]')
    if not html:
        logging.warning(f"[{category}] no html for {start_url}")
        return []

    page_urls = discover_pagination_urls(html, start_url)

    all_urls: List[str] = []
    seen = set()

    for i, page_url in enumerate(page_urls, start=1):
        if i > 1:
            logging.info(f"[{category}] category page {i}: {page_url}")
            html = fetcher.fetch_html_playwright(page_url, wait_selector='main a[href*="/product/"]')
            if not html:
                logging.warning(f"[{category}] no html for {page_url}")
                break

        urls = parse_category_product_urls(html, start_url)
        logging.info(f"[{category}] page {i}: found {len(urls)} product links")

        new_urls = [u for u in urls if u not in seen]
        for u in new_urls:
            seen.add(u)
        all_urls.extend(new_urls)

        sleep_politely()

        # If a later page returns zero, stop early
        if i > 1 and not urls:
            break

    logging.info(f"[{category}] discovered {len(all_urls)} product urls")
    return all_urls


# ───────────────────────── PRODUCT PARSING ─────────────────────────
def parse_key_features(soup: BeautifulSoup) -> Tuple[List[str], Dict[str, str]]:
    features_list: List[str] = []
    key_node = soup.find(string=re.compile(r"\bKey Features\b", re.IGNORECASE))
    if key_node:
        tag = key_node.parent if hasattr(key_node, "parent") else None
        ul = tag.find_next("ul") if tag else None
        if ul:
            for li in ul.find_all("li"):
                t = text_or_empty(li)
                if t:
                    features_list.append(t)

    # fallback: best UL by ":" density
    if not features_list:
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
            features_list = best

    features_kv: Dict[str, str] = {}
    for item in features_list:
        if ":" in item:
            k, v = item.split(":", 1)
            k = clean_text(k)
            v = clean_text(v)
            if k and v:
                features_kv[k] = v

    return features_list, features_kv


def detect_stock(soup: BeautifulSoup) -> Tuple[Optional[bool], str]:
    full = soup.get_text(" ", strip=True).lower()

    if "sold out" in full or "out of stock" in full:
        return False, "OutOfStock"
    if re.search(r"\bin stock\b", full):
        return True, "InStock"

    stock_el = soup.select_one("p.stock, span.stock")
    if stock_el:
        st = stock_el.get_text(" ", strip=True).lower()
        if "out of stock" in st:
            return False, "OutOfStock"
        if "in stock" in st:
            return True, "InStock"

    body = soup.select_one("body")
    if body and body.get("class"):
        cls = " ".join(body.get("class"))
        if "outofstock" in cls:
            return False, "OutOfStock"

    return None, "Unknown"


def parse_price(soup: BeautifulSoup) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], str]:
    price_el = soup.select_one("p.price, span.price")
    price_raw = text_or_empty(price_el)

    price_regular = None
    price_current = None

    if price_el:
        del_el = price_el.select_one("del bdi, del .woocommerce-Price-amount")
        ins_el = price_el.select_one("ins bdi, ins .woocommerce-Price-amount")
        if del_el:
            price_regular = first_match_float(del_el.get_text(" ", strip=True))
        if ins_el:
            price_current = first_match_float(ins_el.get_text(" ", strip=True))

    vals = all_floats(price_raw)
    price_min = min(vals) if vals else None
    price_max = max(vals) if vals else None
    if price_current is None and len(vals) == 1:
        price_current = vals[0]

    return price_current, price_regular, price_min, price_max, price_raw


def extract_images(soup: BeautifulSoup, base_url: str) -> List[str]:
    urls = []
    for img in soup.select(".woocommerce-product-gallery img, figure.woocommerce-product-gallery__wrapper img"):
        src = img.get("src") or img.get("data-src") or ""
        if src:
            urls.append(urljoin(base_url, src))
    out, seen = [], set()
    for u in urls:
        if u not in seen:
            out.append(u)
            shows = True
            seen.add(u)
    return out


def extract_breadcrumbs(soup: BeautifulSoup) -> List[str]:
    crumbs = []
    nav = soup.select_one("nav.woocommerce-breadcrumb, .woocommerce-breadcrumb")
    if nav:
        txt = nav.get_text(">", strip=True)
        parts = [clean_text(x) for x in txt.split(">") if clean_text(x)]
        crumbs.extend(parts)
    return crumbs


def extract_meta(soup: BeautifulSoup) -> Tuple[str, str, str]:
    sku = text_or_empty(soup.select_one("span.sku"))
    brand = ""
    meta = soup.select_one("div.product_meta")
    if meta:
        for span in meta.select("span"):
            t = span.get_text(" ", strip=True)
            if re.search(r"\bBrands?\b", t, re.IGNORECASE):
                links = [text_or_empty(a) for a in span.select("a")]
                links = [x for x in links if x]
                if links:
                    brand = ", ".join(links)
                else:
                    brand = clean_text(re.sub(r"(?i)\bBrands?\b\s*:\s*", "", t))
                break

    product_id = ""
    pid = soup.select_one('form.cart input[name="add-to-cart"]')
    if pid and pid.get("value"):
        product_id = str(pid["value"])
    else:
        body = soup.select_one("body")
        if body and body.get("class"):
            for c in body.get("class"):
                m = re.match(r"postid-(\d+)", c)
                if m:
                    product_id = m.group(1)
                    break

    return sku, brand, product_id


def extract_variant_options(soup: BeautifulSoup) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    form = soup.select_one("form.variations_form")
    if not form:
        return out

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
            out[name] = vals
    return out


def extract_variants(soup: BeautifulSoup) -> List[Dict]:
    form = soup.select_one("form.variations_form")
    if not form:
        return []

    raw = form.get("data-product_variations")
    if not raw:
        return []

    try:
        variations = json.loads(raw)
    except Exception:
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
        color = pick_attr(attrs, ["color", "colour"])
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
                "color": color,
            }
        )
    return out


def parse_product_page(html: str, product_url: str, category: str, source_category_url: str) -> Dict:
    soup = BeautifulSoup(html, "html.parser")

    name = text_or_empty(soup.select_one("h1.product_title, h1.entry-title, h1"))
    sku, brand, product_id = extract_meta(soup)

    in_stock, stock_status = detect_stock(soup)
    price_current, price_regular, price_min, price_max, price_raw = parse_price(soup)

    key_features_list, key_features_kv = parse_key_features(soup)

    short_desc = text_or_empty(soup.select_one("div.woocommerce-product-details__short-description"))
    desc_panel = soup.select_one("div#tab-description, #tab-description, .woocommerce-Tabs-panel--description")
    description = text_or_empty(desc_panel)

    specs = dict(key_features_kv)
    table = soup.select_one("table.woocommerce-product-attributes")
    if table:
        for row in table.select("tr"):
            k = text_or_empty(row.select_one("th"))
            v = text_or_empty(row.select_one("td"))
            if k and v and k not in specs:
                specs[k] = v

    images = extract_images(soup, product_url)
    breadcrumbs = extract_breadcrumbs(soup)

    variants = extract_variants(soup)
    variant_options = extract_variant_options(soup)

    if in_stock is None and variants:
        in_stock = any(v.get("in_stock") for v in variants)
        stock_status = "InStock" if in_stock else "OutOfStock"

    return {
        "category": category,
        "source_category_url": source_category_url,
        "product_url": strip_query(product_url),
        "product_id": product_id,
        "name": name,
        "brand": brand,
        "sku": sku,
        "currency": CURRENCY_DEFAULT,
        "price_raw": price_raw,
        "price_current": price_current,
        "price_regular": price_regular,
        "price_min": price_min,
        "price_max": price_max,
        "in_stock": in_stock,
        "stock_status": stock_status,
        "key_features_json": json.dumps(key_features_list, ensure_ascii=False),
        "specs_json": json.dumps(specs, ensure_ascii=False),
        "short_description": short_desc,
        "description": description,
        "images_json": json.dumps(images, ensure_ascii=False),
        "breadcrumbs_json": json.dumps(breadcrumbs, ensure_ascii=False),
        "variant_options_json": json.dumps(variant_options, ensure_ascii=False),
        "variants_json": json.dumps(variants, ensure_ascii=False),
        "variants": variants,
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


def ensure_products_table(client: bigquery.Client, dataset_id: str, table_id: str) -> bigquery.Table:
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    schema = [
        bigquery.SchemaField("ts", "TIMESTAMP"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("source_category_url", "STRING"),
        bigquery.SchemaField("product_url", "STRING"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("brand", "STRING"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("price_raw", "STRING"),
        bigquery.SchemaField("price_current", "FLOAT"),
        bigquery.SchemaField("price_regular", "FLOAT"),
        bigquery.SchemaField("price_min", "FLOAT"),
        bigquery.SchemaField("price_max", "FLOAT"),
        bigquery.SchemaField("in_stock", "BOOLEAN"),
        bigquery.SchemaField("stock_status", "STRING"),
        bigquery.SchemaField("key_features_json", "STRING"),
        bigquery.SchemaField("specs_json", "STRING"),
        bigquery.SchemaField("short_description", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("images_json", "STRING"),
        bigquery.SchemaField("breadcrumbs_json", "STRING"),
        bigquery.SchemaField("variant_options_json", "STRING"),
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


def ensure_variants_table(client: bigquery.Client, dataset_id: str, table_id: str) -> bigquery.Table:
    table_ref = f"{client.project}.{dataset_id}.{table_id}"
    schema = [
        bigquery.SchemaField("ts", "TIMESTAMP"),
        bigquery.SchemaField("category", "STRING"),
        bigquery.SchemaField("source_category_url", "STRING"),
        bigquery.SchemaField("product_url", "STRING"),
        bigquery.SchemaField("product_id", "STRING"),
        bigquery.SchemaField("product_name", "STRING"),
        bigquery.SchemaField("variation_id", "STRING"),
        bigquery.SchemaField("sku", "STRING"),
        bigquery.SchemaField("storage", "STRING"),
        bigquery.SchemaField("ram", "STRING"),
        bigquery.SchemaField("color", "STRING"),
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
        ignore_unknown_values=True,  # safer if schema drifts slightly
    )
    job = client.load_table_from_json(rows, table_ref, job_config=job_config, location=BQ_LOCATION)
    res = job.result()
    return res.output_rows or len(rows)


# ───────────────────────── RUN ─────────────────────────
def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    ts = ts_now_utc()

    fetcher = HybridFetcher()

    try:
        # 1) Discover product URLs per category
        cat_to_urls: Dict[str, List[str]] = {}
        for cat, url in CATEGORY_URLS.items():
            cat_to_urls[cat] = scrape_category(fetcher, cat, url)

        # 2) Scrape product pages
        products_rows: List[Dict] = []
        variants_rows: List[Dict] = []
        seen_products = set()

        for cat, urls in cat_to_urls.items():
            source_category_url = CATEGORY_URLS[cat]
            for purl in urls:
                if purl in seen_products:
                    continue
                seen_products.add(purl)

                logging.info(f"[{cat}] product: {purl}")
                html = fetcher.fetch_html_playwright(purl, wait_selector="h1, h1.product_title")
                if not html:
                    products_rows.append({
                        "ts": ts,
                        "category": cat,
                        "source_category_url": source_category_url,
                        "product_url": purl,
                        "product_id": "",
                        "name": "",
                        "brand": "",
                        "sku": "",
                        "currency": CURRENCY_DEFAULT,
                        "price_raw": "",
                        "price_current": None,
                        "price_regular": None,
                        "price_min": None,
                        "price_max": None,
                        "in_stock": None,
                        "stock_status": "Unknown",
                        "key_features_json": "[]",
                        "specs_json": "{}",
                        "short_description": "",
                        "description": "",
                        "images_json": "[]",
                        "breadcrumbs_json": "[]",
                        "variant_options_json": "{}",
                        "variants_json": "[]",
                        "scrape_error": "fetch_failed",
                    })
                    sleep_politely()
                    continue

                try:
                    item = parse_product_page(html, purl, cat, source_category_url)

                    products_rows.append({
                        "ts": ts,
                        "category": item["category"],
                        "source_category_url": item["source_category_url"],
                        "product_url": item["product_url"],
                        "product_id": item["product_id"],
                        "name": item["name"],
                        "brand": item["brand"],
                        "sku": item["sku"],
                        "currency": item["currency"],
                        "price_raw": item["price_raw"],
                        "price_current": item["price_current"],
                        "price_regular": item["price_regular"],
                        "price_min": item["price_min"],
                        "price_max": item["price_max"],
                        "in_stock": item["in_stock"],
                        "stock_status": item["stock_status"],
                        "key_features_json": item["key_features_json"],
                        "specs_json": item["specs_json"],
                        "short_description": item["short_description"],
                        "description": item["description"],
                        "images_json": item["images_json"],
                        "breadcrumbs_json": item["breadcrumbs_json"],
                        "variant_options_json": item["variant_options_json"],
                        "variants_json": item["variants_json"],
                        "scrape_error": "",
                    })

                    for v in item.get("variants") or []:
                        variants_rows.append({
                            "ts": ts,
                            "category": cat,
                            "source_category_url": source_category_url,
                            "product_url": item["product_url"],
                            "product_id": item["product_id"],
                            "product_name": item["name"],
                            "variation_id": str(v.get("variation_id") or ""),
                            "sku": v.get("sku") or "",
                            "storage": v.get("storage") or "",
                            "ram": v.get("ram") or "",
                            "color": v.get("color") or "",
                            "price": v.get("display_price"),
                            "regular_price": v.get("display_regular_price"),
                            "in_stock": v.get("in_stock"),
                            "attributes_json": json.dumps(v.get("attributes") or {}, ensure_ascii=False),
                        })

                except Exception as ex:
                    logging.exception(f"Parse failed for {purl}: {ex}")
                    products_rows.append({
                        "ts": ts,
                        "category": cat,
                        "source_category_url": source_category_url,
                        "product_url": purl,
                        "product_id": "",
                        "name": "",
                        "brand": "",
                        "sku": "",
                        "currency": CURRENCY_DEFAULT,
                        "price_raw": "",
                        "price_current": None,
                        "price_regular": None,
                        "price_min": None,
                        "price_max": None,
                        "in_stock": None,
                        "stock_status": "Unknown",
                        "key_features_json": "[]",
                        "specs_json": "{}",
                        "short_description": "",
                        "description": "",
                        "images_json": "[]",
                        "breadcrumbs_json": "[]",
                        "variant_options_json": "{}",
                        "variants_json": "[]",
                        "scrape_error": "parse_failed",
                    })

                sleep_politely()

        logging.info(f"Collected products: {len(products_rows)} rows")
        logging.info(f"Collected variants: {len(variants_rows)} rows")

        # 3) BigQuery load
        client = get_bq_client()
        ensure_dataset(client, BQ_DATASET)
        ensure_products_table(client, BQ_DATASET, BQ_PRODUCTS_TABLE)
        ensure_variants_table(client, BQ_DATASET, BQ_VARIANTS_TABLE)

        inserted_p = bq_append_rows(client, BQ_DATASET, BQ_PRODUCTS_TABLE, products_rows)
        inserted_v = bq_append_rows(client, BQ_DATASET, BQ_VARIANTS_TABLE, variants_rows)

        logging.info(f"Appended {inserted_p} to {client.project}.{BQ_DATASET}.{BQ_PRODUCTS_TABLE}")
        logging.info(f"Appended {inserted_v} to {client.project}.{BQ_DATASET}.{BQ_VARIANTS_TABLE}")

    finally:
        fetcher.close()


if __name__ == "__main__":
    run()
