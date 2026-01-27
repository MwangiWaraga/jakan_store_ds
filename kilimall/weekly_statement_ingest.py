import os
import glob
import pandas as pd
import numpy as np
from google.cloud import bigquery
from datetime import datetime
import pandas.api.types as ptypes

# --- CONFIGURATION ---
# Use absolute path to ensure files are found
FOLDER_PATH = r'C:/Users/Jack Admin/Desktop/Data Science/Projects/oraimo_scrap/data/weekly_statements/'
PROJECT_ID = 'jakan-group'
DATASET_ID = 'core'

# 1. TAB MAPPING (Excel Sheet Name -> BigQuery Table Name)
TABS_TO_PROCESS = {
    'bill': 'kilimall_finance_bill_raw_bqt',
    'bill details': 'kilimall_finance_bill_details_raw_bqt',
    'operation fee': 'kilimall_finance_operation_fees_raw_bqt',
    'storage fee': 'kilimall_finance_storage_fees_raw_bqt',
    'ds processing fee': 'kilimall_finance_ds_fees_raw_bqt',
    'fine': 'kilimall_finance_fines_raw_bqt',
    'Other Deductions': 'kilimall_finance_deductions_raw_bqt',
    'customer service discount': 'kilimall_finance_cs_discount_raw_bqt',
    'Billing Appeal': 'kilimall_finance_appeals_raw_bqt',
    'compensation': 'kilimall_finance_compensation_raw_bqt'
}

# 2. SCHEMA DEFINITION
COMMON_COLS = [
    bigquery.SchemaField("source_filename", "STRING"),
    bigquery.SchemaField("statement_start_date", "DATE"),
    bigquery.SchemaField("statement_end_date", "DATE"),
]

# Define critical columns and types here.
# Missing columns in Excel will be auto-filled with NULL.
SCHEMAS = {
    'bill': COMMON_COLS + [
        bigquery.SchemaField("store_id", "STRING"),
        bigquery.SchemaField("store_name", "STRING"),
        bigquery.SchemaField("total_valume", "FLOAT"),
        bigquery.SchemaField("goods_number", "INTEGER"),
        bigquery.SchemaField("total_commission", "FLOAT"),
        bigquery.SchemaField("ds_quality_inspection_fee", "FLOAT"),
        bigquery.SchemaField("settlement_payable_1", "FLOAT"),
        bigquery.SchemaField("fine", "FLOAT"),
        bigquery.SchemaField("warehouse_operation_fee", "FLOAT"),
        bigquery.SchemaField("warehouse_storage_fee", "FLOAT"),
        bigquery.SchemaField("ds_processing_fee", "FLOAT"),
        bigquery.SchemaField("lite_shipping_fee", "FLOAT"),
        bigquery.SchemaField("customer_service_discount_fee", "FLOAT"),
        bigquery.SchemaField("other_deductions", "FLOAT"),
        bigquery.SchemaField("billing_appeal", "FLOAT"),
        bigquery.SchemaField("compensations", "FLOAT"),
        bigquery.SchemaField("settlement_payable_2", "FLOAT"),
        bigquery.SchemaField("previous_balance", "FLOAT"),
        bigquery.SchemaField("final_settlement_payable", "FLOAT"),
        bigquery.SchemaField("remark", "STRING"),
    ],
    'bill details': COMMON_COLS + [
        bigquery.SchemaField("store_id", "STRING"),
        bigquery.SchemaField("store_name", "STRING"),
        bigquery.SchemaField("order_sn", "STRING"),
        bigquery.SchemaField("payment_time", "TIMESTAMP"),
        bigquery.SchemaField("finnshed_time", "TIMESTAMP"),
        bigquery.SchemaField("goods_id", "STRING"),
        bigquery.SchemaField("goods_name", "STRING"),
        bigquery.SchemaField("goods_price", "FLOAT"),
        bigquery.SchemaField("goods_num", "INTEGER"),
        bigquery.SchemaField("rate", "FLOAT"),
        bigquery.SchemaField("complete_amount", "FLOAT"),
        bigquery.SchemaField("commission", "FLOAT"),
        bigquery.SchemaField("ds_quality_inspection_fee", "FLOAT"),
        bigquery.SchemaField("settlement", "FLOAT"),
        bigquery.SchemaField("fine", "FLOAT"),
        bigquery.SchemaField("warehouse_operation_fee", "FLOAT"),
        bigquery.SchemaField("warehouse_storage_fee", "FLOAT"),
        bigquery.SchemaField("ds_processing_fee", "FLOAT"),
        bigquery.SchemaField("lite_shipping_fee", "FLOAT"),
        bigquery.SchemaField("customer_service_discount_fee", "FLOAT"),
        bigquery.SchemaField("other_deductions", "FLOAT"),
        bigquery.SchemaField("billing_appeal", "FLOAT"),
        bigquery.SchemaField("compensations", "FLOAT"),
        bigquery.SchemaField("settlement_payable", "FLOAT"),
        bigquery.SchemaField("previous_balance", "FLOAT"),
        bigquery.SchemaField("final_settlement_payable", "FLOAT"),
    ],
    # Add other schemas here as needed
}

# Ensure all tabs have at least COMMON_COLS for schema
for tab in TABS_TO_PROCESS.keys():
    if tab not in SCHEMAS:
        SCHEMAS[tab] = COMMON_COLS

def clean_column_name(col_name):
    """Standardizes column headers (lowercase, underscores, no special chars)."""
    return (str(col_name).strip()
            .replace(' ', '_')
            .replace('.', '')
            .replace('(', '_')
            .replace(')', '')
            .replace('（', '_')
            .replace('）', '')
            .lower())

def clean_dataframe(df):
    """ Cleans data and handles duplicate columns. """
    # 1. Clean Headers
    df.columns = [clean_column_name(c) for c in df.columns]
    
    # 2. DEDUPLICATE: Keep first occurrence of any duplicate column name
    # .copy() prevents 'SettingWithCopyWarning'
    df = df.loc[:, ~df.columns.duplicated()].copy()
    
    # 3. Data Cleanup
    for col in df.columns:
        if df[col].dtype == 'object':
             # Strip whitespace, handle 'nan' text, and clean up Order IDs
             df[col] = df[col].astype(str).str.strip().replace({'nan': None, 'NaT': None})
             if 'order' in col or 'sn' in col:
                 df[col] = df[col].str.lstrip(',')
    return df

def align_dataframe_to_schema(df, schema):
    """
    Ensures DataFrame matches BigQuery Schema strictly.
    1. Adds missing columns as NULL (using appropriate NA types).
    2. Forces correct data types (prevents int64 vs string errors).
    """
    if not schema:
        return df
    
    for field in schema:
        col = field.name
        
        # A. Missing Column Handling
        if col not in df.columns:
            if field.field_type in ['INTEGER', 'INT64']:
                df[col] = pd.Series(pd.NA, dtype='Int64', index=df.index)
            elif field.field_type in ['FLOAT', 'FLOAT64']:
                df[col] = pd.Series(pd.NA, dtype='Float64', index=df.index)
            elif field.field_type == 'BOOLEAN':
                df[col] = pd.Series(pd.NA, dtype='boolean', index=df.index)
            elif field.field_type in ['DATE', 'DATETIME', 'TIMESTAMP']:
                df[col] = pd.Series(pd.NaT, dtype='object', index=df.index)
            else:  # STRING, etc.
                df[col] = pd.Series(None, dtype='object', index=df.index)
        
        # B. Type Enforcement
        if field.field_type in ['FLOAT', 'FLOAT64']:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Float64')
        elif field.field_type in ['INTEGER', 'INT64']:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
        elif field.field_type == 'STRING':
            df[col] = df[col].apply(lambda x: str(x).strip() if pd.notnull(x) else None)
        elif field.field_type == 'DATE':
            dates = pd.to_datetime(df[col], errors='coerce')
            df[col] = dates.apply(lambda x: x.date() if pd.notnull(x) else None).astype('object')
        elif field.field_type == 'TIMESTAMP':
            df[col] = pd.to_datetime(df[col], errors='coerce').astype('object')
        elif field.field_type == 'BOOLEAN':
            df[col] = df[col].astype('boolean')
    
    return df

def infer_bq_type(dtype):
    if ptypes.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    elif ptypes.is_integer_dtype(dtype):
        return "INTEGER"
    elif ptypes.is_float_dtype(dtype):
        return "FLOAT"
    elif ptypes.is_bool_dtype(dtype):
        return "BOOLEAN"
    else:
        return "STRING"

def parse_filename_dates(filename):
    """ Parses dates from '20240401_20240427_...' filename format. """
    try:
        basename = os.path.basename(filename)
        parts = basename.split('_')
        if len(parts) >= 2 and len(parts[0]) == 8 and len(parts[1]) == 8:
            s_date = datetime.strptime(parts[0], "%Y%m%d").date()
            e_date = datetime.strptime(parts[1], "%Y%m%d").date()
            return s_date, e_date
    except:
        pass
    return None, None

def run_pipeline():
    if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
        print("❌ Error: GOOGLE_APPLICATION_CREDENTIALS not set.")
        return
    
    client = bigquery.Client(project=PROJECT_ID)
    
    search_pattern = os.path.join(FOLDER_PATH, "*.xlsx")
    all_files = glob.glob(search_pattern)
    
    if not all_files:
        print(f"❌ No Excel files found in: {FOLDER_PATH}")
        return
    
    print(f"Found {len(all_files)} files. Processing...")
    
    aggregated_data = {tab: [] for tab in TABS_TO_PROCESS.keys()}
    
    # --- 1. READ FILES ---
    for filename in all_files:
        print(f" Processing: {os.path.basename(filename)}...")
        start_date, end_date = parse_filename_dates(filename)
        
        try:
            xls = pd.ExcelFile(filename)
            for target_tab in TABS_TO_PROCESS.keys():
                # Fuzzy match tab names
                sheet_match = next((s for s in xls.sheet_names if s.strip().lower() == target_tab.lower().strip()), None)
                if sheet_match:
                    df = pd.read_excel(xls, sheet_name=sheet_match)
                    if df.empty: continue
                    df['source_filename'] = os.path.basename(filename)
                    df['statement_start_date'] = start_date
                    df['statement_end_date'] = end_date
                    
                    aggregated_data[target_tab].append(df)
        except Exception as e:
            print(f" ❌ Error reading {filename}: {e}")
    
    # --- 2. UPLOAD ---
    print("\n--- Starting Uploads ---")
    
    for tab_name, df_list in aggregated_data.items():
        if not df_list: continue

        # Filter out empty or all-NA DataFrames to avoid concatenation warnings
        non_empty_dfs = [df for df in df_list if not df.dropna(how='all').empty]
        if not non_empty_dfs:
            continue
            
        master_df = pd.concat(non_empty_dfs, ignore_index=True, sort=False)
        master_df = clean_dataframe(master_df)
        
        current_schema = SCHEMAS.get(tab_name)
        if current_schema:
            master_df = align_dataframe_to_schema(master_df, current_schema)
            
            gold_names = {field.name for field in current_schema}
            extra_cols = [col for col in master_df.columns if col not in gold_names]
            extra_fields = [bigquery.SchemaField(col, infer_bq_type(master_df[col].dtype)) for col in extra_cols]
            full_schema = current_schema + extra_fields
            autodetect = False
        else:
            full_schema = None
            autodetect = True
        
        bq_table_name = TABS_TO_PROCESS[tab_name]
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{bq_table_name}"
        
        print(f"REQ: Uploading {len(master_df)} rows to {bq_table_name}...")
        
        # --- STRATEGY: DELETE AND CREATE (Bypasses Schema Update Conflict) ---
        try:
            client.delete_table(table_ref, not_found_ok=True)
            
            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_EMPTY",
                schema=full_schema,
                autodetect=autodetect
            )
            
            job = client.load_table_from_dataframe(master_df, table_ref, job_config=job_config)
            job.result()
            print(f"✅ Success.")
        except Exception as e:
            print(f"❌ Failed: {e}")

if __name__ == "__main__":
    run_pipeline()