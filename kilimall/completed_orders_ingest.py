import os
import sys
import pandas as pd
import numpy as np # Needed for safe replacement
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# --- CONFIGURATION ---
PROJECT_ID = 'jakan-group'
DATASET_ID = 'core'
TABLE_NAME = 'kilimall_completed_orders_raw_bqt' 
PATH = 'data/completed_orders.xlsx'

# 1. Define Schema
SCHEMA_DEFINITION = [
    bigquery.SchemaField("order_number", "STRING"),
    bigquery.SchemaField("order_id", "STRING"),
    bigquery.SchemaField("sku_id", "STRING"),
    bigquery.SchemaField("sku_title", "STRING"),
    bigquery.SchemaField("sold_qty", "INTEGER"),
    bigquery.SchemaField("deal_price", "FLOAT"), 
    bigquery.SchemaField("promotion_type", "STRING"),
    bigquery.SchemaField("discount", "FLOAT"),
    bigquery.SchemaField("order_time", "TIMESTAMP"),
    bigquery.SchemaField("payment_time", "TIMESTAMP"),
    bigquery.SchemaField("complete_time", "TIMESTAMP"),
    bigquery.SchemaField("status", "STRING"),
    bigquery.SchemaField("shop_name", "STRING"),
]

# 2. Target Excel Headers
EXCEL_COLUMNS_MAP = [
    'order_number', 'order_id', 'sku_id', 'sku_title', 'sold_qty', 
    'deal_price', 'promotion_type', 'discount', 'order_time', 
    'payment_time', 'complete_time', 'status'
]

def run_pipeline():
    # --- AUTH CHECK ---
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("❌ Error: GOOGLE_APPLICATION_CREDENTIALS environment variable not set.")
        sys.exit(1)

    print(f"🚀 Starting Import for: {PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}")
    
    try:
        client = bigquery.Client(project=PROJECT_ID)

        # --- READ EXCEL ---
        print(f"📖 Reading {PATH}...")
        try:
            df = pd.read_excel(PATH, header=0)
        except FileNotFoundError:
            print(f"❌ Error: File not found at {PATH}")
            return

        # --- CLEANUP & FORMATTING ---
        if len(df.columns) != len(EXCEL_COLUMNS_MAP):
            df = df.iloc[:, :len(EXCEL_COLUMNS_MAP)]

        df.columns = EXCEL_COLUMNS_MAP
        df['shop_name'] = 'Jakan Phone Store' 

        # --- FIX FOR "nan" TEXT ---
        # Instead of astype(str), we use this safe conversion for string columns
        string_cols = ['order_number', 'order_id', 'sku_id', 'sku_title', 
                       'promotion_type', 'status', 'shop_name']
        
        for col in string_cols:
            # Convert to string, then replace the literal text "nan" with None (NULL)
            df[col] = df[col].astype(str).replace({'nan': None, 'NaT': None})

        # Numeric cleanup (Keep 0 fill for math, or remove .fillna to have NULLs there too)
        df['sold_qty'] = df['sold_qty'].fillna(0).astype(int)
        df['deal_price'] = df['deal_price'].fillna(0.0).astype(float)
        df['discount'] = df['discount'].fillna(0.0).astype(float)

        # Date cleanup
        for col in ['order_time', 'payment_time', 'complete_time']:
            df[col] = pd.to_datetime(df[col], errors='coerce')

        # --- UPLOAD TO BIGQUERY ---
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"
        
        # Reset Table (Delete to ensure no partition rules remain)
        print(f"REQ: Resetting table {table_ref}...")
        client.delete_table(table_ref, not_found_ok=True) 

        print(f"REQ: Uploading {len(df)} rows...")

        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE", 
            schema=SCHEMA_DEFINITION
            # NO PARTITIONING CONFIG HERE
        )

        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result() 
        
        print(f"✅ Success! Table {TABLE_NAME} replaced (Clean NULLs, No Partition).")

    except GoogleAPIError as e:
        print(f"❌ Cloud Error: {e}")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run_pipeline()