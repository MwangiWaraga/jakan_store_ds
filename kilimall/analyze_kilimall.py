import requests
from bs4 import BeautifulSoup
import json
import re

def deep_analyze_kilimall():
    url = 'https://www.kilimall.co.ke/store/JAKAN-PHONE-STORE'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    print("=== DEEP ANALYSIS OF KILIMALL STORE PAGE ===")
    resp = requests.get(url, headers=headers, timeout=30)
    print(f"Status: {resp.status_code}")
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    # 1. Find ALL product links
    product_links = soup.select('a[href*="/listing/"]')
    print(f"\n1. PRODUCT LINKS: {len(product_links)} found")
    
    # 2. Look for JavaScript/AJAX data
    print(f"\n2. JAVASCRIPT DATA ANALYSIS:")
    scripts = soup.find_all('script')
    for i, script in enumerate(scripts):
        if script.string and len(script.string) > 100:
            content = script.string
            # Look for JSON data
            if 'products' in content.lower() or 'goods' in content.lower():
                print(f"Script {i+1}: Contains product data ({len(content)} chars)")
                if '"products"' in content or '"goods"' in content:
                    print("  -> Contains JSON product array")
                if 'page' in content.lower() and 'total' in content.lower():
                    print("  -> Contains pagination info")
            if 'ajax' in content.lower() or 'fetch' in content.lower():
                print(f"Script {i+1}: Contains AJAX calls")

    # 3. Look for hidden form data or API endpoints
    print(f"\n3. FORMS AND HIDDEN DATA:")
    forms = soup.find_all('form')
    print(f"Forms found: {len(forms)}")
    for i, form in enumerate(forms):
        action = form.get('action', 'N/A')
        method = form.get('method', 'GET')
        print(f"  Form {i+1}: {method} -> {action}")
        
    # 4. Look for data attributes that might contain product info
    print(f"\n4. ELEMENTS WITH DATA ATTRIBUTES:")
    data_elements = soup.find_all(attrs={'data-total': True}) + soup.find_all(attrs={'data-count': True}) + soup.find_all(attrs={'data-products': True})
    for elem in data_elements:
        print(f"  {elem.name}: {dict(elem.attrs)}")

    # 5. Look for the "Products: 86" text and surrounding context
    print(f"\n5. PRODUCTS COUNT ANALYSIS:")
    page_text = soup.get_text()
    product_count_matches = re.findall(r'Products?\s*:?\s*(\d+)', page_text, re.IGNORECASE)
    print(f"Product count mentions: {product_count_matches}")
    
    # Find the exact element containing the count
    for elem in soup.find_all(text=re.compile(r'Products?\s*:?\s*\d+', re.IGNORECASE)):
        parent = elem.parent
        print(f"  Found in: {parent.name} - '{elem.strip()}'")
        print(f"    Parent classes: {parent.get('class', [])}")
        print(f"    Parent attributes: {dict(parent.attrs)}")

    # 6. Check for infinite scroll or load more buttons
    print(f"\n6. LOAD MORE / INFINITE SCROLL:")
    load_more = soup.find_all(text=re.compile(r'load\s*more|show\s*more|view\s*more', re.IGNORECASE))
    print(f"Load more buttons: {len(load_more)}")
    
    # 7. Look for pagination in different forms
    print(f"\n7. PAGINATION PATTERNS:")
    # Check for page info in text
    page_info = re.findall(r'(\d+)\s*of\s*(\d+)|page\s*(\d+)|showing\s*(\d+)', page_text.lower())
    print(f"Page indicators in text: {page_info}")
    
    # 8. Network requests analysis (look for XHR endpoints in JS)
    print(f"\n8. POTENTIAL API ENDPOINTS:")
    all_text = soup.get_text() + resp.text
    api_patterns = [
        r'/api/[^"\s]+',
        r'/ajax/[^"\s]+', 
        r'\.json[^"\s]*',
        r'/store/[^"\s]+\?[^"\s]*page[^"\s]*',
    ]
    
    for pattern in api_patterns:
        matches = re.findall(pattern, all_text)
        if matches:
            print(f"  Pattern {pattern}: {len(matches)} matches")
            for match in matches[:3]:  # Show first 3
                print(f"    {match}")

    # 9. Check for any containers that might hold all products
    print(f"\n9. PRODUCT CONTAINERS:")
    containers = soup.select('[class*="product"], [class*="goods"], [class*="item"], [id*="product"]')
    container_info = {}
    for container in containers:
        key = f"{container.name}.{'.'.join(container.get('class', []))}"
        if key not in container_info:
            container_info[key] = 0
        container_info[key] += 1
    
    for key, count in sorted(container_info.items(), key=lambda x: x[1], reverse=True):
        if count > 1:
            print(f"  {key}: {count} instances")

if __name__ == "__main__":
    deep_analyze_kilimall()
