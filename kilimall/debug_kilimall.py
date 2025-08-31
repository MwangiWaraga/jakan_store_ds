import requests
from bs4 import BeautifulSoup
import re

def debug_kilimall_pagination():
    url = 'https://www.kilimall.co.ke/store/JAKAN-PHONE-STORE'
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; KilimallStoreScraper/2.1; +learning-project) PythonRequests',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-KE,en;q=0.8',
        'Connection': 'close'
    }

    print(f"Fetching: {url}")
    resp = requests.get(url, headers=headers, timeout=20)
    print(f"Status: {resp.status_code}")
    print(f"Content-Type: {resp.headers.get('Content-Type', 'N/A')}")
    
    soup = BeautifulSoup(resp.text, 'html.parser')
    
    # Find product links
    listing_links = soup.select('a[href^="/listing/"]')
    print(f"\n=== PRODUCTS FOUND ===")
    print(f"Total /listing/ links: {len(listing_links)}")
    
    # Look for pagination elements
    print(f"\n=== PAGINATION ANALYSIS ===")
    
    # Check for any links with numbers
    numeric_links = []
    for a in soup.find_all('a'):
        text = a.get_text(strip=True)
        if text.isdigit():
            numeric_links.append((text, a.get('href', '')))
    
    print(f"Numeric page links found: {len(numeric_links)}")
    for text, href in numeric_links[:10]:
        print(f"  '{text}' -> {href}")
    
    # Check for "next" type links
    next_links = []
    for a in soup.find_all('a'):
        text = a.get_text(strip=True).lower()
        if any(word in text for word in ['next', 'more', '›', '»', '下一页']):
            next_links.append((text, a.get('href', '')))
    
    print(f"Next/More type links found: {len(next_links)}")
    for text, href in next_links:
        print(f"  '{text}' -> {href}")
    
    # Look for pagination containers
    pagination_containers = soup.select('nav, .pagination, .page-nav, [class*=page], [class*=Page]')
    print(f"Pagination containers found: {len(pagination_containers)}")
    for i, container in enumerate(pagination_containers[:3]):
        print(f"  Container {i+1}: {container.get_text(' ', strip=True)[:100]}")
        if container.name == 'nav':
            links = container.find_all('a')
            print(f"    Links in nav: {len(links)}")
    
    # Check if there might be AJAX pagination
    scripts = soup.find_all('script')
    ajax_indicators = 0
    for script in scripts:
        if script.string and any(word in script.string.lower() for word in ['ajax', 'page', 'load', 'next']):
            ajax_indicators += 1
    
    print(f"Scripts with potential AJAX pagination: {ajax_indicators}")
    
    # Try to find the total number of products mentioned on the page
    page_text = soup.get_text()
    numbers = re.findall(r'\b\d+\b', page_text)
    large_numbers = [n for n in numbers if int(n) > 50 and int(n) < 1000]
    print(f"Numbers found that could be total products: {large_numbers[:10]}")

if __name__ == "__main__":
    debug_kilimall_pagination()
