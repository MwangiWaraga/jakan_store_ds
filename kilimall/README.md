# Kilimall Store Scraper

**Optimized web scraper for Kilimall.co.ke stores with Google Sheets integration**

## 🎯 Performance
- **Coverage**: 80%+ product coverage (69/86 products for JAKAN store)  
- **Efficiency**: Only ~18 HTTP requests per store
- **Speed**: ~52 seconds execution time
- **Reliability**: Multi-strategy pagination with intelligent deduplication

## 🚀 Quick Start

```bash
# Run the scraper
python kilimall_scraper.py
```

## ⚙️ Configuration

Edit `kilimall_scraper.py` to customize:

```python
# Add more stores to scrape
STORES = [
    {"name": "JAKAN PHONE STORE", "path": "/store/JAKAN-PHONE-STORE"},
    {"name": "Your Store Name", "path": "/store/YOUR-STORE-SLUG"},
]

# Update Google Sheets settings  
SHEET_ID = "your-google-sheet-id"
SHEET_TAB = "kilimall_products"
```

## 🔧 How It Works

Uses **5 proven pagination strategies**:
1. `pageNo` - Primary strategy (gets most products)
2. `price_desc` - Price-sorted pages (catches missed items)
3. `sales` - Sales-sorted pages (different ordering)
4. `pageNum` - Alternative numbering (edge cases)
5. `offset` - Offset-based pagination (final cleanup)

## 📊 Output

Data is automatically uploaded to Google Sheets with columns:
- `timestamp` - When the product was scraped
- `store_name` - Name of the store
- `product_url` - Direct link to product
- `title` - Product title
- `price` - Product price

## 📝 Logs

Execution logs are saved to `kilimall_scraper.log` with performance metrics.

---

*Last updated: 2025-08-31*
*Optimized for maximum coverage with minimal requests*
