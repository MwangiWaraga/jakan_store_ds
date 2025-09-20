#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kilimall -> best Oraimo matcher (Python + BigQuery).
- For each Kilimall title, pick exactly one best Oraimo title.
- Compare by trimming the Kilimall title to each Oraimo title's length, normalize, then score via token-Jaccard.
- Writes to BigQuery with WRITE_TRUNCATE.

Requires: google-cloud-bigquery, pandas
  pip install google-cloud-bigquery pandas
Auth: set GOOGLE_APPLICATION_CREDENTIALS to your service account JSON.
"""

import os
from typing import List, Dict, Optional, Tuple
import pandas as pd
from google.cloud import bigquery

# =====================
# config (edit these)
# =====================
PROJECT_ID: str = "YOUR_GCP_PROJECT_ID"

# source datasets / tables
SOURCE_DATASET: str = "YOUR_SOURCE_DATASET"         # e.g. "dbt_jackson"
TABLE_ORAIMO: str   = "oraimo_products"
TABLE_KILIMALL: str = "kilimall_products"

# destination table
DEST_DATASET: str = "YOUR_DEST_DATASET"             # can be same as SOURCE_DATASET
DEST_TABLE: str   = "kilmall_to_oraimo_best"

# kilimall filters
BRAND_FILTER: str = "Oraimo"
ONLY_ACTIVE: bool = True

# write mode
WRITE_DISPOSITION: str = "WRITE_TRUNCATE"           # WRITE_TRUNCATE | WRITE_APPEND | WRITE_EMPTY

# matching controls
USE_BLOCKING: bool = True
BLOCK_PREFIX_LEN_PRIMARY: int  = 8                  # first N alnum chars
BLOCK_PREFIX_LEN_FALLBACK: int = 6                  # fallback width
VERBOSE_LOGGING: bool = True

# =====================
# helpers
# =====================

def normalize_text(s: Optional[str]) -> str:
    if s is None:
        return ""
    s = s.lower()
    out = []
    prev_space = False
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            prev_space = False
        else:
            if not prev_space:
                out.append(" ")
                prev_space = True
    return "".join(out).strip()

def alnum_prefix(s: str, n: int) -> str:
    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
            if len(out) >= n:
                break
    return "".join(out)

def tokenize(s: str) -> List[str]:
    return [t for t in s.split(" ") if t]

def jaccard_tokens(a_tokens: List[str], b_tokens: List[str]) -> float:
    sa, sb = set(a_tokens), set(b_tokens)
    if not sa and not sb:
        return 0.0
    union = sa | sb
    if not union:
        return 0.0
    return len(sa & sb) / len(union)

def trim_to_len(s: str, n: int) -> str:
    return s[:max(0, n)]

# =====================
# bigquery I/O
# =====================

def read_tables(client: bigquery.Client) -> Tuple[pd.DataFrame, pd.DataFrame]:
    sql_oraimo = f"""
    select
      product_id,
      product_title,
      product_model,
      product_url,
      current_price,
      in_stock
    from `{PROJECT_ID}.{SOURCE_DATASET}.{TABLE_ORAIMO}`
    """
    sql_kilmall = f"""
    select
      listing_id,
      sku_id,
      product_title,
      product_url,
      selling_price
    from `{PROJECT_ID}.{SOURCE_DATASET}.{TABLE_KILIMALL}`
    where brand = '{BRAND_FILTER}'{" and is_active = 1" if ONLY_ACTIVE else ""}
    """
    if VERBOSE_LOGGING:
        print("running query (oraimo):", sql_oraimo)
        print("running query (kilmall):", sql_kilmall)
    oraimo = client.query(sql_oraimo).result().to_dataframe()
    kilmall = client.query(sql_kilmall).result().to_dataframe()
    return oraimo, kilmall

def write_table(client: bigquery.Client, df: pd.DataFrame) -> None:
    table_id = f"{PROJECT_ID}.{DEST_DATASET}.{DEST_TABLE}"
    job_config = bigquery.LoadJobConfig(write_disposition=WRITE_DISPOSITION)
    if VERBOSE_LOGGING:
        print(f"writing {len(df)} rows to {table_id} with {WRITE_DISPOSITION}...")
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    if VERBOSE_LOGGING:
        print("write complete.")

# =====================
# matching logic
# =====================

def build_block_key(title_norm: str, n: int) -> str:
    return alnum_prefix(title_norm, n)

def prepare_data(oraimo: pd.DataFrame, kilmall: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    # oraimo normalization
    oraimo = oraimo.copy()
    oraimo["title_norm"] = oraimo["product_title"].astype(str).apply(normalize_text)
    oraimo["len_norm"]   = oraimo["title_norm"].str.len().astype(int)
    oraimo["tokens"]     = oraimo["title_norm"].apply(tokenize)
    if USE_BLOCKING:
        oraimo["block"]          = oraimo["title_norm"].apply(lambda s: build_block_key(s, BLOCK_PREFIX_LEN_PRIMARY))
        oraimo["block_fallback"] = oraimo["title_norm"].apply(lambda s: build_block_key(s, BLOCK_PREFIX_LEN_FALLBACK))

    # kilmall normalization
    kilmall = kilmall.copy()
    kilmall["title_norm"] = kilmall["product_title"].astype(str).apply(normalize_text)
    if USE_BLOCKING:
        kilmall["block"]          = kilmall["title_norm"].apply(lambda s: build_block_key(s, BLOCK_PREFIX_LEN_PRIMARY))
        kilmall["block_fallback"] = kilmall["title_norm"].apply(lambda s: build_block_key(s, BLOCK_PREFIX_LEN_FALLBACK))

    return oraimo, kilmall

def match_kilmall_to_best_oraimo(oraimo: pd.DataFrame, kilmall: pd.DataFrame) -> pd.DataFrame:
    # candidate indices for blocking
    if USE_BLOCKING:
        idx_primary: Dict[str, List[int]] = {}
        idx_fallback: Dict[str, List[int]] = {}
        for i, row in oraimo.iterrows():
            idx_primary.setdefault(row["block"], []).append(i)
            idx_fallback.setdefault(row["block_fallback"], []).append(i)
    else:
        idx_all = list(oraimo.index)

    results = []
    for _, k in kilmall.iterrows():
        # choose candidates
        if USE_BLOCKING:
            cands_idx = idx_primary.get(k.get("block", ""), [])
            if not cands_idx:
                cands_idx = idx_fallback.get(k.get("block_fallback", ""), [])
            if not cands_idx:
                cands_idx = list(oraimo.index)  # last resort: all
        else:
            cands_idx = idx_all

        best_sim = -1.0
        best_idx = None
        k_norm = k["title_norm"]

        for i in cands_idx:
            o = oraimo.loc[i]
            k_trim = trim_to_len(k_norm, int(o["len_norm"]))
            sim = jaccard_tokens(tokenize(k_trim), o["tokens"])
            if sim > best_sim:
                best_sim = sim
                best_idx = i

        o = oraimo.loc[best_idx] if best_idx is not None else None
        results.append({
            "listing_id":     k.get("listing_id"),
            "sku_id":         k.get("sku_id"),
            "kilmall_title":  k.get("product_title"),
            "kilmall_url":    k.get("product_url"),
            "kilmall_price":  k.get("selling_price"),
            "product_id":     None if o is None else o.get("product_id"),
            "oraimo_title":   None if o is None else o.get("product_title"),
            "product_model":  None if o is None else o.get("product_model"),
            "oraimo_url":     None if o is None else o.get("product_url"),
            "oraimo_price":   None if o is None else o.get("current_price"),
            "oraimo_in_stock":None if o is None else o.get("in_stock"),
            "similarity":     best_sim
        })

    return pd.DataFrame(results)

def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    oraimo_df, kilmall_df = read_tables(client)
    if VERBOSE_LOGGING:
        print(f"loaded {len(oraimo_df)} oraimo rows, {len(kilmall_df)} kilmall rows")
    oraimo_df, kilmall_df = prepare_data(oraimo_df, kilmall_df)
    result_df = match_kilmall_to_best_oraimo(oraimo_df, kilmall_df)

    # final columns (includes oraimo product_id as requested)
    cols = [
        "listing_id", "sku_id", "kilmall_title", "kilmall_url", "kilmall_price",
        "product_id", "oraimo_title", "product_model", "oraimo_url", "oraimo_price", "oraimo_in_stock",
        "similarity"
    ]
    result_df = result_df[cols]
    write_table(client, result_df)
    if VERBOSE_LOGGING:
        print("done.")

if __name__ == "__main__":
    main()
