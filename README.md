# Oraimo Scrap

A data pipeline project for scraping product data from [Oraimo Kenya](https://ke.oraimo.com) and ingesting sales/inventory data from Kilimall into Google BigQuery.

## Overview

This project consists of two main components:

1. **Oraimo Web Scraper** - Scrapes product information (prices, stock status, descriptions) from the Oraimo Kenya e-commerce website
2. **Kilimall Data Ingest** - Loads Excel exports from Kilimall seller portal into BigQuery for analysis

## Project Structure

```
oraimo_scrap/
├── oraimo/
│   ├── oraimo_scraper.py        # Scrapes Oraimo products → Google Sheets
│   └── oraimo_scrapper_bq.py    # Scrapes Oraimo products → BigQuery
├── kilimall/
│   ├── kilimall_stock_ingest.py      # Loads product inventory to BigQuery
│   ├── completed_orders_ingest.py    # Loads completed orders to BigQuery
│   └── weekly_statement_ingest.py    # Loads weekly financial statements to BigQuery
├── data/
│   ├── stock.xlsx                    # Kilimall stock export
│   ├── completed_orders.xlsx         # Kilimall orders export
│   └── weekly_statements/            # Folder for weekly statement files
├── requirements.txt
├── run_scraper.bat.template          # Windows batch script template
└── README.md
```

## Features

### Oraimo Scraper

- Scrapes product data from multiple categories (audio, power, smart-office, personal-care, home-appliances)
- Extracts: title, price, stock status, images, EAN, model, descriptions
- Handles pagination automatically
- Polite crawling with random delays
- Outputs to **Google Sheets** or **BigQuery**

### Kilimall Data Ingest

- **Stock Ingest**: Loads product inventory with pricing and stock levels
- **Orders Ingest**: Loads completed order history with timestamps
- **Weekly Statements**: Processes multiple financial tabs including:
  - Bills & bill details
  - Operation fees, storage fees, DS processing fees
  - Fines, deductions, compensations
  - Customer service discounts, billing appeals

## Prerequisites

- Python 3.9+
- Google Cloud Platform account with BigQuery enabled
- Service account JSON key with appropriate permissions

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd oraimo_scrap
   ```

2. **Create virtual environment**
   ```bash
   python -m venv .venv
   .venv\Scripts\activate  # Windows
   # source .venv/bin/activate  # Linux/Mac
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Additional packages** (if not in requirements.txt)
   ```bash
   pip install pandas google-cloud-bigquery pyarrow openpyxl gspread beautifulsoup4 requests
   ```

## Configuration

### Google Cloud Authentication

Set the environment variable to your service account JSON:

```bash
# Windows
set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\your\credentials.json

# Linux/Mac
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/credentials.json
```

### BigQuery Settings

Update the configuration in each script as needed:

```python
PROJECT_ID = "your_project"
DATASET_ID = "dataset"
TABLE_ID   = "your_table_name"
```

### Google Sheets (for oraimo_scraper.py)

Update the `SHEET_ID` in `oraimo/oraimo_scraper.py`:

```python
SHEET_ID = "your-google-sheet-id"
SHEET_TAB = "sheet_name"
```

## Usage

### Run Oraimo Scraper (BigQuery)

```bash
cd oraimo
python oraimo_scrapper_bq.py
```

### Run Oraimo Scraper (Google Sheets)

```bash
cd oraimo
python oraimo_scraper.py
```

### Run Kilimall Stock Ingest

```bash
python kilimall/kilimall_stock_ingest.py --excel_path data/stock.xlsx
```

### Run Kilimall Orders Ingest

```bash
python kilimall/completed_orders_ingest.py
```

### Run Kilimall Weekly Statements Ingest

```bash
python kilimall/weekly_statement_ingest.py
```

### Using Batch Script (Windows)

1. Copy `run_scraper.bat.template` to `run_scraper.bat`
2. Edit paths in the batch file
3. Run: `run_scraper.bat`

## BigQuery Tables

| Table | Description |
|-------|-------------|
| `oraimo_products_raw_bqt` | Scraped Oraimo product catalog |
| `kilimall_products_raw_bqt` | Kilimall product inventory |
| `kilimall_completed_orders_raw_bqt` | Completed order history |
| `kilimall_finance_bill_raw_bqt` | Weekly billing summaries |
| `kilimall_finance_bill_details_raw_bqt` | Detailed bill line items |
| `kilimall_finance_operation_fees_raw_bqt` | Operation fee records |
| `kilimall_finance_storage_fees_raw_bqt` | Storage fee records |
| `kilimall_finance_ds_fees_raw_bqt` | DS processing fee records |
| `kilimall_finance_fines_raw_bqt` | Fine records |
| `kilimall_finance_deductions_raw_bqt` | Other deductions |
| `kilimall_finance_cs_discount_raw_bqt` | Customer service discounts |
| `kilimall_finance_appeals_raw_bqt` | Billing appeals |
| `kilimall_finance_compensation_raw_bqt` | Compensation records |

## Data Schema Examples

### Oraimo Products

| Column | Type | Description |
|--------|------|-------------|
| ts | TIMESTAMP | Scrape timestamp |
| category | STRING | Product category |
| product_url | STRING | Product page URL |
| title | STRING | Product name |
| price_now | STRING | Current price |
| price_was | STRING | Original price (if discounted) |
| stock_status | STRING | InStock/OutOfStock/Unknown |
| ean | STRING | Product EAN code |
| model | STRING | SKU/Model number |

### Kilimall Stock

| Column | Type | Description |
|--------|------|-------------|
| listing_id | STRING | Kilimall listing ID |
| sku_id | STRING | SKU identifier |
| title | STRING | Product title |
| selling_price | FLOAT64 | Current selling price |
| fbk_inventory | INT64 | FBK warehouse stock |
| non_fbk_inventory | INT64 | Non-FBK stock |
| status | STRING | Listing status |
| updated_at_ts | TIMESTAMP | Load timestamp |

## Scheduling

For automated runs, you can:

1. **Windows Task Scheduler** - Use the batch script template
2. **Linux/Mac Cron** - Create a shell script and add to crontab
3. **Cloud Scheduler** - Deploy to Cloud Functions/Cloud Run

## License

MIT License

## Author

Waraga Mwangi
