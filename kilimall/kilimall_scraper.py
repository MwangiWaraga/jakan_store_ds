# kilimall_scraper.py
# Scrape specific Kilimall stores (using store IDs) and append product snapshots to Google Sheets.
# Columns written: updated_at, store_name, product_title, product_url, listing_id, price
#
# Auth: expects GOOGLE_APPLICATION_CREDENTIALS env var pointing to a service account JSON.

import os
import re
import time
import json
import random
import logging
from typing import Dict, List, Optional, Iterable
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

# ───────────────────────── CONFIG ─────────────────────────

BASE_URL = "https://www.kilimall.co.ke"

# Provide name + id pairs here (IDs are numeric; e.g., 8958 for Jakan)
STORES: List[Dict] = [
    {"name": "Jakan Phone Store", "id": 8958},
    # {"name": "Another Store", "id": 1234},
]

SHEET_ID = "18QRcbrEq2T-iaNQICu535J2u_cPFzQxCY-GRcDMt49o"
SHEET_TAB = "kilimall"  # your existing tab name

REQUEST_TIMEOUT = 20
RETRY_COUNT = 3
REQUEST_DELAY_RANGE = (0.8, 1.6)
MAX_PAGES_PER_STORE = 400   # safety cap
MAX_EMPTY_PAGES = 3         # stop after N consecutive empty pages

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-KE,en;q=0.8",
    "X-Requested-With": "XMLHttpRequest",
    "Connection": "close",
}

# Nairobi timestamp
try:
    from zoneinfo import ZoneInfo  # Py3.9+
    KE_TZ = ZoneInfo("Africa/Nairobi")
except Exception:
    from datetime import timezone, timedelta
    KE_TZ = timezone(timedelta(hours=3))

# New header with listing_id
HEADER_ROW = ["updated_at", "store_name", "product_title", "product_url", "listing_id", "price"]
OLD_HEADER_ROW = ["updated_at", "store_name", "product_title", "product_url", "price"]


# ───────────────────────── UTIL ─────────────────────────

def ts_now_ke() -> str:
    from datetime import datetime
    return datetime.now(KE_TZ).strftime("%Y-%m-%d %H:%M:%S")

def sleep_politely():
    time.sleep(random.uniform(*REQUEST_DELAY_RANGE))

def fetch(url: str, headers: Optional[dict] = None) -> Optional[str]:
    H = dict(HEADERS)
    if headers:
        H.update(headers)
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            r = requests.get(url, headers=H, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r.text
            if 400 <= r.status_code < 500:
                logging.warning(f"[{r.status_code}] for {url}")
                return None
            logging.warning(f"[{r.status_code}] transient for {url}")
        except requests.RequestException as ex:
            logging.warning(f"Request error (attempt {attempt}) {url}: {ex}")
        sleep_politely()
    return None


# ───────────────────────── SHEETS ─────────────────────────

import gspread
from google.oauth2.service_account import Credentials

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
        ws = sh.add_worksheet(title=SHEET_TAB, rows=1000, cols=len(HEADER_ROW) + 2)
        ws.append_row(HEADER_ROW, value_input_option="RAW")
        return ws

    first = ws.row_values(1)
    if not first:
        ws.append_row(HEADER_ROW, value_input_option="RAW")
    elif first == OLD_HEADER_ROW:
        # Auto-upgrade header to include listing_id (keeps old data intact)
        ws.update(range_name="A1", values=[HEADER_ROW], value_input_option="RAW")
        logging.info(f"Upgraded header in '{SHEET_TAB}' to include listing_id.")
    elif first != HEADER_ROW:
        logging.warning("Sheet header differs from expected; appending under existing header.")
    return ws

def append_rows(ws, rows: List[List]):
    if not rows:
        return
    try:
        ws.append_rows(rows, value_input_option="RAW")
    except Exception:
        for r in rows:
            ws.append_row(r, value_input_option="RAW")


# ───────────────────── PARSING & CLEANUP ─────────────────────

PRICE_RE = re.compile(r"KSh\s*[\d,]+", re.I)
RATING_TAIL_RE = re.compile(r"\s*\(\d+\)\s*$")  # trailing "(0)", "(12)", …
MULTISPACE_RE = re.compile(r"\s+")

# Broad product anchor patterns seen across Kilimall templates
A_SELECTORS = [
    'a[href^="/listing/"]',
    'a[href*="/listing/"]',
    'a[href^="/item/"]',
    'a[href*="/item/"]',
    'a[href*="/product/"]',
    'a[href*="/detail/"]',
    'a[href*="/goods/"]',
]

# Nearby elements that often carry the item name
TITLE_CANDIDATE_SELECTORS = [
    '[class*="title"]',
    '[class*="name"]',
    '[class*="goods"]',
    '[class*="pro"]',
    'h1', 'h2', 'h3', 'p', 'span'
]

def decode_html_from_json(s: str) -> str:
    """Sub-page sometimes returns JSON { html: '...'}; return the inner HTML if present."""
    s = s or ""
    t = s.lstrip()
    if t.startswith("{"):
        try:
            obj = json.loads(t)
            for k in ("html", "data", "content", "body"):
                v = obj.get(k)
                if isinstance(v, str) and v.strip():
                    return v
        except Exception:
            pass
    return s

def canonical_product_url(url: str) -> str:
    """Dedupe by scheme + host + path only; drop query + fragment."""
    p = urlparse(url)
    netloc = "www.kilimall.co.ke"
    return urlunparse((p.scheme or "https", netloc, p.path.rstrip("/"), "", "", ""))

def extract_listing_id(url: str) -> str:
    """Grab the numeric id after /listing/, e.g. /listing/1001433571-... -> 1001433571."""
    path = urlparse(url).path or ""
    m = re.search(r"/listing/(\d+)", path)
    return m.group(1) if m else ""

def clean_title(title: str) -> str:
    if not title:
        return ""
    title = PRICE_RE.sub("", title)          # drop embedded prices from anchor text
    title = RATING_TAIL_RE.sub("", title)    # drop trailing "(0)" ratings
    title = MULTISPACE_RE.sub(" ", title).strip(" -–•|")
    return title

def best_title_for_anchor(a) -> str:
    """
    Try multiple places for a human-readable title:
    - a@title / a@aria-label
    - text inside <a>
    - <img alt|title> inside <a>
    - nearby elements with 'title/name/goods/pro' classes or headings
    - a@data-* attributes that look like a name
    """
    def ok(txt: str) -> str:
        txt = clean_title((txt or "").strip())
        # reject price-only or too-short strings
        if not txt or PRICE_RE.search(txt) or len([c for c in txt if c.isalpha()]) < 3:
            return ""
        return txt

    # 1) direct attributes
    for k in ("title", "aria-label"):
        t = ok(a.get(k))
        if t:
            return t

    # 2) inner text
    t = ok(a.get_text(" ", strip=True))
    if t:
        return t

    # 3) image alt/title inside anchor
    for img in a.find_all("img"):
        t = ok(img.get("alt") or img.get("title"))
        if t:
            return t

    # 4) nearby candidates (anchor → parent → grandparent)
    node = a
    for _ in range(3):
        if not node:
            break
        cands = []
        for sel in TITLE_CANDIDATE_SELECTORS:
            for el in node.select(sel):
                txt = ok(el.get_text(" ", strip=True))
                if txt:
                    cands.append(txt)
        if cands:
            cands.sort(key=lambda s: (-len(s), s))  # prefer longest
            return cands[0]
        node = node.parent

    # 5) helpful data-* attributes
    for k, v in (a.attrs or {}).items():
        if not isinstance(v, str):
            continue
        kl = k.lower()
        if any(kw in kl for kw in ("title", "name", "goods", "label", "alt")):
            t = ok(v)
            if t:
                return t

    return ""

def parse_product_tiles(html: str) -> List[Dict]:
    """Parse product anchors and closest 'KSh …' price, plus listing_id."""
    soup = BeautifulSoup(html or "", "html.parser")

    # be generous: collect likely anchors once
    anchors = []
    for sel in A_SELECTORS:
        anchors.extend(soup.select(sel))
    # dedupe element objects while preserving order
    seen_elem_ids = set()
    uniq_anchors = []
    for el in anchors:
        _id = id(el)
        if _id not in seen_elem_ids:
            seen_elem_ids.add(_id)
            uniq_anchors.append(el)

    out, seen_hrefs = [], set()
    for a in uniq_anchors:
        # support href, data-href, data-url
        href = (a.get("href") or a.get("data-href") or a.get("data-url") or "").strip()
        if not href or href.startswith("javascript:") or href == "#":
            continue
        if href in seen_hrefs:
            continue
        seen_hrefs.add(href)

        full_url = urljoin(BASE_URL, href)

        # robust title extraction
        title = best_title_for_anchor(a)

        # Climb up to find nearest price text
        price = ""
        node = a
        for _ in range(8):
            if not node:
                break
            text = " ".join(node.stripped_strings)
            m = PRICE_RE.search(text)
            if m:
                price = m.group(0).strip()
                break
            node = node.parent

        out.append({
            "product_title": title,
            "product_url": full_url,
            "listing_id": extract_listing_id(full_url),
            "price": price,
        })
    return out


# ───────────────────── PAGINATION (INFINITE SCROLL) ─────────────────────

def subpage_candidates(store_id: str, page: int) -> Iterable[str]:
    """
    Try both zero-based and one-based indexes for EVERY page.
    page==1 → try 0, then 1
    page>=2 → try page-1 (zero-based), then page (one-based)
    Generate multiple param name variants per index.
    """
    base = f"{BASE_URL}/new/store/sub-page/{store_id}"

    if page == 1:
        idxs = [0, 1]
    else:
        idxs = [page - 1, page]

    tried = set()
    for idx in idxs:
        if idx < 0:
            continue
        variants = [
            f"{base}?typeName=All+Products&pageNum={idx}",
            f"{base}?typeName=All+Products&page={idx}",
            f"{base}?typeName=All+Products&pageNo={idx}",
            f"{base}?pageNum={idx}&typeName=All+Products",
            f"{base}?typeName=All+Products&pageNum={idx}&pageSize=36",
            f"{base}?typeName=All+Products&pageNum={idx}&pageSize=32",
            f"{base}?typeName=All+Products&pageNum={idx}&pageSize=48",
        ]
        for url in variants:
            if url not in tried:
                tried.add(url)
                yield url

def scrape_by_store_id(store_name: str, store_id: int) -> List[Dict]:
    """
    Use the AJAX sub-page endpoint for page 1..N until no new tiles appear.
    Dedupe across pages by canonical URL; keep listing_id.
    """
    logging.info(f"Store: {store_name} (id={store_id})")
    all_items: List[Dict] = []
    seen_paths = set()

    page = 1
    empty_streak = 0
    while page <= MAX_PAGES_PER_STORE and empty_streak < MAX_EMPTY_PAGES:
        got_new = False

        for url in subpage_candidates(str(store_id), page):
            body = fetch(url)
            if not body:
                continue
            html = decode_html_from_json(body)
            batch = parse_product_tiles(html)

            new = []
            for it in batch:
                canon = canonical_product_url(it["product_url"])
                if canon in seen_paths:
                    continue
                seen_paths.add(canon)
                # normalize url to canonical form and keep listing_id
                it["product_url"] = canon
                new.append(it)

            if new:
                all_items.extend(new)
                logging.info(f"page {page}: +{len(new)} (total {len(all_items)}) via {url}")
                got_new = True
                break  # next page

        if not got_new:
            empty_streak += 1
            logging.info(f"page {page}: no new items (empty_streak={empty_streak})")
        else:
            empty_streak = 0

        page += 1
        sleep_politely()

    for it in all_items:
        it["store_name"] = store_name

    return all_items


# ───────────────────────── RUN ─────────────────────────

def run():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
    ts = ts_now_ke()

    everything: List[Dict] = []
    for entry in STORES:
        name = entry["name"]
        sid = entry.get("id")
        if not sid:
            logging.warning(f"{name}: missing 'id' – please add a numeric store id")
            continue
        items = scrape_by_store_id(name, int(sid))
        logging.info(f"{name}: {len(items)} unique products scraped")
        everything.extend(items)

    logging.info(f"Total rows: {len(everything)}")

    # Optional sanity: warn if some titles are blank
    blank_titles = sum(1 for it in everything if not it["product_title"])
    if blank_titles:
        logging.warning(f"{blank_titles} items had empty product_title (will still be written).")

    # Prepare rows for Sheets (now includes listing_id)
    rows = [
        [ts, it["store_name"], it["product_title"], it["product_url"], it.get("listing_id", ""), it["price"]]
        for it in everything
    ]

    # Write to Google Sheets
    gc = get_sheets_client()
    sh = gc.open_by_key(SHEET_ID)
    ws = ensure_worksheet(sh)
    append_rows(ws, rows)

    logging.info(f"Appended {len(rows)} rows to '{SHEET_TAB}'. Done.")

if __name__ == "__main__":
    run()


# Suddently this stopped getting the product titles
# # kilimall_scraper.py
# # Scrape specific Kilimall stores (using store IDs) and append product snapshots to Google Sheets.
# # Columns written: updated_at, store_name, product_title, product_url, listing_id, price
# #
# # Auth: expects GOOGLE_APPLICATION_CREDENTIALS env var pointing to a service account JSON.

# import os
# import re
# import time
# import json
# import random
# import logging
# from typing import Dict, List, Optional, Iterable
# from urllib.parse import urljoin, urlparse, urlunparse

# import requests
# from bs4 import BeautifulSoup

# # ───────────────────────── CONFIG ─────────────────────────

# BASE_URL = "https://www.kilimall.co.ke"

# # Provide name + id pairs here (IDs are numeric; e.g., 8958 for Jakan)
# STORES: List[Dict] = [
#     {"name": "Jakan Phone Store", "id": 8958},
#     # {"name": "Another Store", "id": 1234},
# ]

# SHEET_ID = "18QRcbrEq2T-iaNQICu535J2u_cPFzQxCY-GRcDMt49o"
# SHEET_TAB = "kilimall"  # your existing tab name

# REQUEST_TIMEOUT = 20
# RETRY_COUNT = 3
# REQUEST_DELAY_RANGE = (0.8, 1.6)
# MAX_PAGES_PER_STORE = 400   # safety cap
# MAX_EMPTY_PAGES = 3         # stop after N consecutive empty pages

# HEADERS = {
#     "User-Agent": (
#         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
#         "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
#     ),
#     "Accept": "text/html,application/xhtml+xml,application/json;q=0.9,*/*;q=0.8",
#     "Accept-Language": "en-KE,en;q=0.8",
#     "X-Requested-With": "XMLHttpRequest",
#     "Connection": "close",
# }

# # Nairobi timestamp
# try:
#     from zoneinfo import ZoneInfo  # Py3.9+
#     KE_TZ = ZoneInfo("Africa/Nairobi")
# except Exception:
#     from datetime import timezone, timedelta
#     KE_TZ = timezone(timedelta(hours=3))

# # New header with listing_id
# HEADER_ROW = ["updated_at", "store_name", "product_title", "product_url", "listing_id", "price"]
# OLD_HEADER_ROW = ["updated_at", "store_name", "product_title", "product_url", "price"]


# # ───────────────────────── UTIL ─────────────────────────

# def ts_now_ke() -> str:
#     from datetime import datetime
#     return datetime.now(KE_TZ).strftime("%Y-%m-%d %H:%M:%S")

# def sleep_politely():
#     time.sleep(random.uniform(*REQUEST_DELAY_RANGE))

# def fetch(url: str, headers: Optional[dict] = None) -> Optional[str]:
#     H = dict(HEADERS)
#     if headers:
#         H.update(headers)
#     for attempt in range(1, RETRY_COUNT + 1):
#         try:
#             r = requests.get(url, headers=H, timeout=REQUEST_TIMEOUT)
#             if r.status_code == 200:
#                 return r.text
#             if 400 <= r.status_code < 500:
#                 logging.warning(f"[{r.status_code}] for {url}")
#                 return None
#             logging.warning(f"[{r.status_code}] transient for {url}")
#         except requests.RequestException as ex:
#             logging.warning(f"Request error (attempt {attempt}) {url}: {ex}")
#         sleep_politely()
#     return None


# # ───────────────────────── SHEETS ─────────────────────────

# import gspread
# from google.oauth2.service_account import Credentials

# def get_sheets_client():
#     scopes = [
#         "https://www.googleapis.com/auth/spreadsheets",
#         "https://www.googleapis.com/auth/drive",
#     ]
#     creds = Credentials.from_service_account_file(
#         os.environ["GOOGLE_APPLICATION_CREDENTIALS"], scopes=scopes
#     )
#     return gspread.authorize(creds)

# def ensure_worksheet(sh) -> gspread.Worksheet:
#     try:
#         ws = sh.worksheet(SHEET_TAB)
#     except gspread.WorksheetNotFound:
#         ws = sh.add_worksheet(title=SHEET_TAB, rows=1000, cols=len(HEADER_ROW) + 2)
#         ws.append_row(HEADER_ROW, value_input_option="RAW")
#         return ws

#     first = ws.row_values(1)
#     if not first:
#         ws.append_row(HEADER_ROW, value_input_option="RAW")
#     elif first == OLD_HEADER_ROW:
#         # Auto-upgrade header to include listing_id (keeps old data intact)
#         # ws.update("A1", [HEADER_ROW])
#         ws.update(range_name="A1", values=[HEADER_ROW], value_input_option="RAW")
#         logging.info(f"Upgraded header in '{SHEET_TAB}' to include listing_id.")
#     elif first != HEADER_ROW:
#         logging.warning("Sheet header differs from expected; appending under existing header.")
#     return ws

# def append_rows(ws, rows: List[List]):
#     if not rows:
#         return
#     try:
#         ws.append_rows(rows, value_input_option="RAW")
#     except Exception:
#         for r in rows:
#             ws.append_row(r, value_input_option="RAW")


# # ───────────────────── PARSING & CLEANUP ─────────────────────

# PRICE_RE = re.compile(r"KSh\s*[\d,]+", re.I)
# RATING_TAIL_RE = re.compile(r"\s*\(\d+\)\s*$")  # trailing "(0)", "(12)", …
# MULTISPACE_RE = re.compile(r"\s+")

# # Broad product anchor patterns seen across Kilimall templates
# A_SELECTORS = [
#     'a[href^="/listing/"]',
#     'a[href*="/listing/"]',
#     'a[href^="/item/"]',
#     'a[href*="/item/"]',
#     'a[href*="/product/"]',
#     'a[href*="/detail/"]',
#     'a[href*="/goods/"]',
# ]

# def decode_html_from_json(s: str) -> str:
#     """Sub-page sometimes returns JSON { html: '...'}; return the inner HTML if present."""
#     s = s or ""
#     t = s.lstrip()
#     if t.startswith("{"):
#         try:
#             obj = json.loads(t)
#             for k in ("html", "data", "content", "body"):
#                 v = obj.get(k)
#                 if isinstance(v, str) and v.strip():
#                     return v
#         except Exception:
#             pass
#     return s

# def canonical_product_url(url: str) -> str:
#     """Dedupe by scheme + host + path only; drop query + fragment."""
#     p = urlparse(url)
#     netloc = "www.kilimall.co.ke"
#     return urlunparse((p.scheme or "https", netloc, p.path.rstrip("/"), "", "", ""))

# def extract_listing_id(url: str) -> str:
#     """Grab the numeric id after /listing/, e.g. /listing/1001433571-... -> 1001433571."""
#     path = urlparse(url).path or ""
#     m = re.search(r"/listing/(\d+)", path)
#     return m.group(1) if m else ""

# def clean_title(title: str) -> str:
#     if not title:
#         return ""
#     title = PRICE_RE.sub("", title)          # drop embedded prices from anchor text
#     title = RATING_TAIL_RE.sub("", title)    # drop trailing "(0)" ratings
#     title = MULTISPACE_RE.sub(" ", title).strip(" -–•|")
#     return title

# def parse_product_tiles(html: str) -> List[Dict]:
#     """Parse product anchors and closest 'KSh …' price, plus listing_id."""
#     soup = BeautifulSoup(html or "", "html.parser")
#     anchors = []
#     for sel in A_SELECTORS:
#         anchors.extend(soup.select(sel))

#     out, seen_hrefs = [], set()
#     for a in anchors:
#         href = (a.get("href") or "").strip()
#         if not href or href == "#" or "javascript:" in href:
#             continue
#         if href in seen_hrefs:
#             continue
#         seen_hrefs.add(href)

#         full_url = urljoin(BASE_URL, href)
#         raw_title = (a.get("title") or a.get_text(" ", strip=True) or "").strip()
#         title = clean_title(raw_title)

#         # Climb up to find nearest price text
#         price = ""
#         node = a
#         for _ in range(8):
#             if not node:
#                 break
#             text = " ".join(node.stripped_strings)
#             m = PRICE_RE.search(text)
#             if m:
#                 price = m.group(0)
#                 break
#             node = node.parent

#         out.append({
#             "product_title": title,
#             "product_url": full_url,
#             "listing_id": extract_listing_id(full_url),
#             "price": price,
#         })
#     return out


# # ───────────────────── PAGINATION (INFINITE SCROLL) ─────────────────────

# def subpage_candidates(store_id: str, page: int) -> Iterable[str]:
#     """
#     Try both zero-based and one-based indexes for EVERY page.
#     page==1 → try 0, then 1
#     page>=2 → try page-1 (zero-based), then page (one-based)
#     Generate multiple param name variants per index.
#     """
#     base = f"{BASE_URL}/new/store/sub-page/{store_id}"

#     if page == 1:
#         idxs = [0, 1]
#     else:
#         idxs = [page - 1, page]

#     tried = set()
#     for idx in idxs:
#         if idx < 0:
#             continue
#         variants = [
#             f"{base}?typeName=All+Products&pageNum={idx}",
#             f"{base}?typeName=All+Products&page={idx}",
#             f"{base}?typeName=All+Products&pageNo={idx}",
#             f"{base}?pageNum={idx}&typeName=All+Products",
#             f"{base}?typeName=All+Products&pageNum={idx}&pageSize=36",
#             f"{base}?typeName=All+Products&pageNum={idx}&pageSize=32",
#             f"{base}?typeName=All+Products&pageNum={idx}&pageSize=48",
#         ]
#         for url in variants:
#             if url not in tried:
#                 tried.add(url)
#                 yield url

# def scrape_by_store_id(store_name: str, store_id: int) -> List[Dict]:
#     """
#     Use the AJAX sub-page endpoint for page 1..N until no new tiles appear.
#     Dedupe across pages by canonical URL; keep listing_id.
#     """
#     logging.info(f"Store: {store_name} (id={store_id})")
#     all_items: List[Dict] = []
#     seen_paths = set()

#     page = 1
#     empty_streak = 0
#     while page <= MAX_PAGES_PER_STORE and empty_streak < MAX_EMPTY_PAGES:
#         got_new = False

#         for url in subpage_candidates(str(store_id), page):
#             body = fetch(url)
#             if not body:
#                 continue
#             html = decode_html_from_json(body)
#             batch = parse_product_tiles(html)

#             new = []
#             for it in batch:
#                 canon = canonical_product_url(it["product_url"])
#                 if canon in seen_paths:
#                     continue
#                 seen_paths.add(canon)
#                 # normalize url to canonical form and keep listing_id
#                 it["product_url"] = canon
#                 new.append(it)

#             if new:
#                 all_items.extend(new)
#                 logging.info(f"page {page}: +{len(new)} (total {len(all_items)}) via {url}")
#                 got_new = True
#                 break  # next page

#         if not got_new:
#             empty_streak += 1
#             logging.info(f"page {page}: no new items (empty_streak={empty_streak})")
#         else:
#             empty_streak = 0

#         page += 1
#         sleep_politely()

#     for it in all_items:
#         it["store_name"] = store_name

#     return all_items


# # ───────────────────────── RUN ─────────────────────────

# def run():
#     logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
#     ts = ts_now_ke()

#     everything: List[Dict] = []
#     for entry in STORES:
#         name = entry["name"]
#         sid = entry.get("id")
#         if not sid:
#             logging.warning(f"{name}: missing 'id' – please add a numeric store id")
#             continue
#         items = scrape_by_store_id(name, int(sid))
#         logging.info(f"{name}: {len(items)} unique products scraped")
#         everything.extend(items)

#     logging.info(f"Total rows: {len(everything)}")

#     # Prepare rows for Sheets (now includes listing_id)
#     rows = [
#         [ts, it["store_name"], it["product_title"], it["product_url"], it.get("listing_id", ""), it["price"]]
#         for it in everything
#     ]

#     # Write to Google Sheets
#     gc = get_sheets_client()
#     sh = gc.open_by_key(SHEET_ID)
#     ws = ensure_worksheet(sh)
#     append_rows(ws, rows)

#     logging.info(f"Appended {len(rows)} rows to '{SHEET_TAB}'. Done.")

# if __name__ == "__main__":
#     run()
