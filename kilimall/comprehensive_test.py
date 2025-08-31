import requests
from bs4 import BeautifulSoup
import re
import time

def test_all_pagination_approaches():
    """Test every conceivable pagination approach for Kilimall"""
    
    base_url = 'https://www.kilimall.co.ke/store/JAKAN-PHONE-STORE'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
    }
    
    all_unique_products = set()
    
    # Test different URL patterns
    test_patterns = [
        # Basic pagination
        (base_url + "?page={}", range(1, 6)),
        (base_url + "?p={}", range(1, 6)),
        (base_url + "?pageNo={}", range(1, 6)),
        (base_url + "?pageNum={}", range(1, 6)),
        
        # Offset-based
        (base_url + "?offset={}", [0, 32, 64, 96, 128]),
        (base_url + "?start={}", [0, 32, 64, 96, 128]),
        
        # With sorting
        (base_url + "?sort=price_asc&page={}", range(1, 4)),
        (base_url + "?sort=price_desc&page={}", range(1, 4)),
        (base_url + "?sort=sales&page={}", range(1, 4)),
        (base_url + "?orderBy=price&page={}", range(1, 4)),
        
        # Different views
        (base_url + "?view=list&page={}", range(1, 4)),
        (base_url + "?view=grid&page={}", range(1, 4)),
        
        # Size limits
        (base_url + "?size=100", [None]),
        (base_url + "?pageSize=100", [None]),
        (base_url + "?limit=100", [None]),
    ]
    
    print("=== COMPREHENSIVE PAGINATION TEST ===")
    
    for url_pattern, values in test_patterns:
        pattern_name = url_pattern.split('?')[1] if '?' in url_pattern else 'base'
        print(f"\n--- Testing {pattern_name} ---")
        
        pattern_products = set()
        
        for value in values:
            if value is None:
                test_url = url_pattern
            else:
                test_url = url_pattern.format(value)
                
            try:
                resp = requests.get(test_url, headers=headers, timeout=15)
                if resp.status_code == 200:
                    soup = BeautifulSoup(resp.text, 'html.parser')
                    links = soup.select('a[href^="/listing/"]')
                    
                    new_links = []
                    for link in links:
                        href = link.get('href', '')
                        if href and href not in pattern_products:
                            new_links.append(href)
                            pattern_products.add(href)
                            all_unique_products.add(href)
                    
                    print(f"  {test_url.replace(base_url, '...')} -> {len(links)} total, {len(new_links)} new")
                    
                    if len(new_links) == 0 and value != values[0]:
                        print(f"    No new products, stopping this pattern")
                        break
                else:
                    print(f"  {test_url.replace(base_url, '...')} -> HTTP {resp.status_code}")
                    
            except Exception as e:
                print(f"  {test_url.replace(base_url, '...')} -> Error: {str(e)[:50]}")
            
            time.sleep(0.5)  # Be polite
        
        print(f"  Pattern total: {len(pattern_products)} unique products")
    
    print(f"\n=== FINAL RESULTS ===")
    print(f"Total unique products found: {len(all_unique_products)}")
    print(f"Target: 86 products")
    
    coverage = len(all_unique_products) / 86 * 100
    print(f"Coverage: {coverage:.1f}%")
    
    if coverage >= 95:
        print("🎉 Excellent! Found almost all products")
    elif coverage >= 80:
        print("✅ Good! Found most products")
    elif coverage >= 60:
        print("⚠️ Moderate coverage, missing some products")
    else:
        print("❌ Low coverage, need different approach")
    
    return sorted(list(all_unique_products))

def find_best_strategy():
    """Find the most efficient strategy"""
    products = test_all_pagination_approaches()
    
    print(f"\n=== OPTIMIZATION SUGGESTIONS ===")
    print(f"Based on {len(products)} products found:")
    
    # Test if we can get all products with fewer requests
    if len(products) >= 75:  # If we found most products
        print("✅ Current multi-strategy approach is working well")
        print("💡 Suggested optimization: Use top 3-4 most effective sorting methods")
    else:
        print("❌ Need to explore other approaches:")
        print("  - Check for AJAX endpoints")
        print("  - Look for mobile/API versions")
        print("  - Consider scraping category pages")

if __name__ == "__main__":
    find_best_strategy()
