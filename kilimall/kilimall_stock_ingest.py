#!/usr/bin/env python3
"""
Simplified BigQuery loader:
- Reads Excel
- Snake-cases headers only (no business cleaning)
- Keeps listing_id, sku_id (and vendor_product_id) as STRING
- Explicit BigQuery schema
- Single load step with WRITE_TRUNCATE (create-or-replace semantics)

Prereqs:
  pip install pandas google-cloud-bigquery pyarrow openpyxl

Auth:
  Uses GOOGLE_APPLICATION_CREDENTIALS (service account JSON)
"""

# -----------------------------
# CONFIG — edit these defaults
# -----------------------------
PROJECT_ID  = "jakan-group"
DATASET_ID  = "jakan_phone_store"
TABLE_ID    = "kilimall_products_raw"
EXCEL_PATH  = r"D:\kilimall\stock.xlsx"
# -----------------------------

import argparse
import re
import pandas as pd
from google.cloud import bigquery

SOURCE_COLS = [
    "ListingId",
    "SkuId",
    "Vendor Product Id",
    "Title",
    "Currency",
    "Market Reference Price",
    "Selling Price",
    "FBK Inventory",
    "Non-FBK Inventory",
    "Status",
]

SNAKE_MAP = {
    "ListingId": "listing_id",
    "SkuId": "sku_id",
    "Vendor Product Id": "vendor_product_id",
    "Title": "title",
    "Currency": "currency",
    "Market Reference Price": "market_reference_price",
    "Selling Price": "selling_price",
    "FBK Inventory": "fbk_inventory",
    "Non-FBK Inventory": "non_fbk_inventory",
    "Status": "status",
}

# BigQuery schema (single source of truth)
BQ_SCHEMA = [
    bigquery.SchemaField("listing_id", "STRING"),
    bigquery.SchemaField("sku_id", "STRING"),
    bigquery.SchemaField("vendor_product_id", "STRING"),
    bigquery.SchemaField("title", "STRING"),
    bigquery.SchemaField("currency", "STRING"),
    bigquery.SchemaField("market_reference_price", "FLOAT64"),
    bigquery.SchemaField("selling_price", "FLOAT64"),
    bigquery.SchemaField("fbk_inventory", "INT64"),
    bigquery.SchemaField("non_fbk_inventory", "INT64"),
    bigquery.SchemaField("status", "STRING"),
]

NUMERIC_COLS = ["market_reference_price", "selling_price", "fbk_inventory", "non_fbk_inventory"]

def to_snake_cols(df: pd.DataFrame) -> pd.DataFrame:
    def snake(s: str) -> str:
        s = re.sub(r"[^\w\s]+", " ", s).strip()
        s = re.sub(r"\s+", "_", s)
        return s.lower()
    return df.rename(columns={c: SNAKE_MAP.get(c, snake(c)) for c in df.columns})

def main():
    # CLI overrides (optional)
    ap = argparse.ArgumentParser(description="Load Excel into BigQuery with snake_case headers.")
    ap.add_argument("--excel_path", default=EXCEL_PATH)
    ap.add_argument("--project_id", default=PROJECT_ID)
    ap.add_argument("--dataset_id", default=DATASET_ID)
    ap.add_argument("--table_id", default=TABLE_ID)
    args = ap.parse_args()

    # Read as strings to protect ID columns from precision loss
    df = pd.read_excel(args.excel_path, dtype=str)

    # Validate expected columns and order
    missing = [c for c in SOURCE_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing expected columns in Excel: {missing}")
    df = df[SOURCE_COLS].copy()

    # Snake-case only (no data changes)
    df = to_snake_cols(df)

    # IDs remain strings; type numerics for clean load
    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    fq_table = f"{args.project_id}.{args.dataset_id}.{args.table_id}"
    client = bigquery.Client(project=args.project_id)

    # Ensure dataset exists (best-effort)
    try:
        client.get_dataset(args.dataset_id)
    except Exception:
        client.create_dataset(bigquery.Dataset(f"{args.project_id}.{args.dataset_id}"))

    # Create-or-replace semantics in one step
    job_config = bigquery.LoadJobConfig(
        schema=BQ_SCHEMA,
        write_disposition="WRITE_TRUNCATE",
    )

    load_job = client.load_table_from_dataframe(df, fq_table, job_config=job_config)
    load_job.result()

    table = client.get_table(fq_table)
    print(f"Loaded {table.num_rows} rows into {table.full_table_id}.")

if __name__ == "__main__":
    main()
