import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import random

# Bright Data proxy URL with your credentials
PROXY = "http://brd-customer-hl_a1966b18-zone-watcharbitrageproxy:qn8vxsi6tjtz@brd.superproxy.io:22225"
def get_proxy():
    return {'http': PROXY, 'https': PROXY}

# Rotating User-Agents to mimic real browsers
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1 Mobile/15E148 Safari/604.1',
]

# Headers to emulate a real browser
HEADERS = {
    'User-Agent': random.choice(USER_AGENTS),
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Referer': 'https://www.google.com/',
    'Connection': 'keep-alive',
}

def save_to_file(filename, data):
    data.to_csv(filename, index=False)

def load_from_file(filename):
    try:
        return pd.read_csv(filename)
    except:
        return pd.DataFrame(columns=['Timestamp', 'Platform', 'Listings Found', 'Listings Saved', 'Errors'])

def update_logs(log_entry):
    try:
        logs = load_from_file("scraper_log.csv")
    except:
        logs = pd.DataFrame(columns=['Timestamp', 'Platform', 'Listings Found', 'Listings Saved', 'Errors'])
    logs = pd.concat([logs, pd.DataFrame([log_entry])], ignore_index=True)
    save_to_file("scraper_log.csv", logs)

def parse_title(title):
    brands = ['Seiko', 'Omega', 'Rolex', 'Tag Heuer', 'Breitling', 'Hamilton', 'Patek Philippe', 'Audemars Piguet']
    for brand in brands:
        if brand.lower() in title.lower():
            model = title.replace(brand, '').strip()
            return brand, model
    return 'Unknown', title

def get_usd_price(price, currency):
    if currency == 'USD':
        return price
    try:
        rates = requests.get("https://api.exchangerate-api.com/v4/latest/USD", headers=HEADERS, proxies=get_proxy()).json()['rates']
        return price / rates[currency] if currency in rates else price
    except:
        return price

def scrape_ebay(max_price):
    url = "https://www.ebay.com/sch/i.html?_nkw=used+watches&_sacat=0&rt=nc&LH_ItemCondition=3000&_pgn=1"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.s-item')[:10]
        print(f"eBay response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.s-item__price').text.replace('$', '').replace(',', '')
                price = float(price_str.split(' to ')[0] if ' to ' in price_str else price_str)
                if price > max_price:
                    continue
                title = item.select_one('.s-item__title').text.strip()
                url = item.select_one('.s-item__link')['href']
                date_listed = item.select_one('.s-item__time').text.strip() if item.select_one('.s-item__time') else datetime.now().strftime('%Y-%m-%d')
                image_url = item.select_one('.s-item__image-img')['src'] if item.select_one('.s-item__image-img') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': date_listed,
                    'Platform': 'eBay',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"eBay item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'eBay', 'Listings Found': len(soup.select('.s-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'eBay', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"eBay request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_chrono24(max_price):
    url = "https://www.chrono24.com/search/index.htm?condition=used&priceMax=2000&sortorder=1"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.article-item')[:10]
        print(f"Chrono24 response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.price').text.strip().replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.article-title').text.strip()
                url = 'https://www.chrono24.com' + item.select_one('a')['href']
                image_url = item.select_one('.article-image')['src'] if item.select_one('.article-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Chrono24',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Chrono24 item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Chrono24', 'Listings Found': len(soup.select('.article-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Chrono24', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Chrono24 request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_watchbox(max_price):
    url = "https://www.watchbox.com/shop/pre-owned-watches/"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.product-tile')[:10]
        print(f"WatchBox response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.price-sales').text.strip().replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.product-name').text.strip()
                url = 'https://www.watchbox.com' + item.select_one('a')['href']
                image_url = item.select_one('.product-image')['src'] if item.select_one('.product-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'WatchBox',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"WatchBox item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'WatchBox', 'Listings Found': len(soup.select('.product-tile')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'WatchBox', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"WatchBox request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_yahoo_japan(max_price):
    url = "https://auctions.yahoo.co.jp/category/list/2084046857/?p=%E6%99%82%E8%A8%88&price_type=currentprice&max=2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.Product')[:10]
        print(f"Yahoo Japan response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.Product__priceValue').text.replace('¥', '').replace(',', '')
                price = get_usd_price(float(price_str), 'JPY')
                if price > max_price:
                    continue
                title = item.select_one('.Product__titleLink').text.strip()
                url = item.select_one('.Product__titleLink')['href']
                image_url = item.select_one('.Product__image')['src'] if item.select_one('.Product__image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Yahoo Japan',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Yahoo Japan item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Yahoo Japan', 'Listings Found': len(soup.select('.Product')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Yahoo Japan', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Yahoo Japan request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_jomashop(max_price):
    url = "https://www.jomashop.com/preowned-watches.html?price=0-2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.product-item')[:10]
        print(f"Jomashop response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.price').text.replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.product-name').text.strip()
                url = item.select_one('a')['href']
                image_url = item.select_one('.product-image')['src'] if item.select_one('.product-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Jomashop',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Jomashop item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Jomashop', 'Listings Found': len(soup.select('.product-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Jomashop', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Jomashop request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_crown_caliber(max_price):
    url = "https://www.crownandcaliber.com/collections/pre-owned-watches?sort_by=price-ascending&filter.v.price.gte=0&filter.v.price.lte=2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.product-grid-item')[:10]
        print(f"Crown & Caliber response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.price__current').text.replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.product-title').text.strip()
                url = 'https://www.crownandcaliber.com' + item.select_one('a')['href']
                image_url = item.select_one('.product-image')['src'] if item.select_one('.product-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Crown & Caliber',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Crown & Caliber item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Crown & Caliber', 'Listings Found': len(soup.select('.product-grid-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Crown & Caliber', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Crown & Caliber request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_sothebys(max_price):
    url = "https://www.sothebys.com/en/buy/watches?sort=price-asc&price_range=0-2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.lot-item')[:10]
        print(f"Sotheby’s response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.price').text.replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.lot-title').text.strip()
                url = 'https://www.sothebys.com' + item.select_one('a')['href']
                image_url = item.select_one('.lot-image')['src'] if item.select_one('.lot-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Sotheby’s',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Sotheby’s item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Sotheby’s', 'Listings Found': len(soup.select('.lot-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Sotheby’s', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Sotheby’s request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_christies(max_price):
    url = "https://www.christies.com/en/auctions/watches?sortby=PriceLowToHigh&priceRange=0-2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.lot')[:10]
        print(f"Christie’s response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.lot-price').text.replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.lot-title').text.strip()
                url = item.select_one('a')['href']
                image_url = item.select_one('.lot-image')['src'] if item.select_one('.lot-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Christie’s',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Christie’s item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Christie’s', 'Listings Found': len(soup.select('.lot')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Christie’s', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Christie’s request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_phillips(max_price):
    url = "https://www.phillips.com/auctions/department/watches?price_range=0-2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.lot-item')[:10]
        print(f"Phillips response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.price').text.replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.lot-title').text.strip()
                url = 'https://www.phillips.com' + item.select_one('a')['href']
                image_url = item.select_one('.lot-image')['src'] if item.select_one('.lot-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Phillips',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Phillips item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Phillips', 'Listings Found': len(soup.select('.lot-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Phillips', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Phillips request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_bonhams(max_price):
    url = "https://www.bonhams.com/departments/WAT/?price_range=0-2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.lot')[:10]
        print(f"Bonhams response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.lot-price').text.replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.lot-title').text.strip()
                url = 'https://www.bonhams.com' + item.select_one('a')['href']
                image_url = item.select_one('.lot-image')['src'] if item.select_one('.lot-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Bonhams',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Bonhams item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Bonhams', 'Listings Found': len(soup.select('.lot')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Bonhams', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Bonhams request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_antiquorum(max_price):
    url = "https://www.antiquorum.swiss/?price_max=2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.lot-item')[:10]
        print(f"Antiquorum response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.price').text.replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.lot-title').text.strip()
                url = 'https://www.antiquorum.swiss' + item.select_one('a')['href']
                image_url = item.select_one('.lot-image')['src'] if item.select_one('.lot-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Antiquorum',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Antiquorum item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Antiquorum', 'Listings Found': len(soup.select('.lot-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Antiquorum', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Antiquorum request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_watchuseek(max_price):
    url = "https://www.watchuseek.com/forums/watch-sales-forum.16/"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.thread')[:10]
        print(f"Watchuseek response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.price').text.replace('$', '').replace(',', '') if item.select_one('.price') else '0'
                price = float(price_str)
                if price > max_price or price == 0:
                    continue
                title = item.select_one('.threadtitle').text.strip()
                url = 'https://www.watchuseek.com' + item.select_one('a')['href']
                image_url = item.select_one('.thread-image')['src'] if item.select_one('.thread-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Watchuseek',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Watchuseek item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Watchuseek', 'Listings Found': len(soup.select('.thread')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Watchuseek', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Watchuseek request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_reddit(max_price):
    url = "https://www.reddit.com/r/WatchExchange/new/"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.Post')[:10]
        print(f"Reddit response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.price').text.replace('$', '').replace(',', '') if item.select_one('.price') else '0'
                price = float(price_str)
                if price > max_price or price == 0:
                    continue
                title = item.select_one('.title').text.strip()
                url = 'https://www.reddit.com' + item.select_one('a')['href']
                image_url = item.select_one('.thumbnail')['src'] if item.select_one('.thumbnail') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Reddit WatchExchange',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Reddit item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Reddit WatchExchange', 'Listings Found': len(soup.select('.Post')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Reddit WatchExchange', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Reddit request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_catawiki(max_price):
    url = "https://www.catawiki.com/en/c/7-watches?price_max=2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.lot')[:10]
        print(f"Catawiki response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.lot-price').text.replace('€', '').replace(',', '').strip()
                price = get_usd_price(float(price_str), 'EUR')
                if price > max_price:
                    continue
                title = item.select_one('.lot-title').text.strip()
                url = 'https://www.catawiki.com' + item.select_one('a')['href']
                image_url = item.select_one('.lot-image')['src'] if item.select_one('.lot-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Catawiki',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Catawiki item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Catawiki', 'Listings Found': len(soup.select('.lot')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Catawiki', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Catawiki request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_timepeaks(max_price):
    url = "https://timepeaks.com/?price_max=2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.item')[:10]
        print(f"Timepeaks response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.price').text.replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.item-title').text.strip()
                url = 'https://timepeaks.com' + item.select_one('a')['href']
                image_url = item.select_one('.item-image')['src'] if item.select_one('.item-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Timepeaks',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Timepeaks item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Timepeaks', 'Listings Found': len(soup.select('.item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Timepeaks', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Timepeaks request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_bobs_watches(max_price):
    url = "https://www.bobswatches.com/auctions?price_max=2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.auction-item')[:10]
        print(f"Bob’s Watches response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.price').text.replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.auction-title').text.strip()
                url = 'https://www.bobswatches.com' + item.select_one('a')['href']
                image_url = item.select_one('.auction-image')['src'] if item.select_one('.auction-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Bob’s Watches',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Bob’s Watches item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Bob’s Watches', 'Listings Found': len(soup.select('.auction-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Bob’s Watches', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Bob’s Watches request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_1stdibs(max_price):
    url = "https://www.1stdibs.com/jewelry/watches/?price_max=2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.item')[:10]
        print(f"1stDibs response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.price').text.replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.item-title').text.strip()
                url = 'https://www.1stdibs.com' + item.select_one('a')['href']
                image_url = item.select_one('.item-image')['src'] if item.select_one('.item-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': '1stDibs',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"1stDibs item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': '1stDibs', 'Listings Found': len(soup.select('.item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': '1stDibs', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"1stDibs request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_watchcollecting(max_price):
    url = "https://watchcollecting.com/?price_max=2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.lot')[:10]
        print(f"WatchCollecting response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.lot-price').text.replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.lot-title').text.strip()
                url = 'https://watchcollecting.com' + item.select_one('a')['href']
                image_url = item.select_one('.lot-image')['src'] if item.select_one('.lot-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'WatchCollecting',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"WatchCollecting item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'WatchCollecting', 'Listings Found': len(soup.select('.lot')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'WatchCollecting', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"WatchCollecting request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_invaluable(max_price):
    url = "https://www.invaluable.com/watches/sc-7L7J8J8J8J/?price_max=2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.lot')[:10]
        print(f"Invaluable response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.lot-price').text.replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.lot-title').text.strip()
                url = 'https://www.invaluable.com' + item.select_one('a')['href']
                image_url = item.select_one('.lot-image')['src'] if item.select_one('.lot-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'Invaluable',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"Invaluable item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Invaluable', 'Listings Found': len(soup.select('.lot')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Invaluable', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"Invaluable request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)

def scrape_liveauctioneers(max_price):
    url = "https://www.liveauctioneers.com/c/watches/7/?price_max=2000"
    listings = []
    try:
        response = requests.get(url, headers={**HEADERS, 'User-Agent': random.choice(USER_AGENTS)}, proxies=get_proxy(), timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.select('.lot')[:10]
        print(f"LiveAuctioneers response status: {response.status_code}, items found: {len(items)}")
        for item in items:
            try:
                price_str = item.select_one('.lot-price').text.replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.lot-title').text.strip()
                url = 'https://www.liveauctioneers.com' + item.select_one('a')['href']
                image_url = item.select_one('.lot-image')['src'] if item.select_one('.lot-image') else ''
                brand, model = parse_title(title)
                listings.append({
                    'Date Listed': datetime.now().strftime('%Y-%m-%d'),
                    'Platform': 'LiveAuctioneers',
                    'Brand': brand,
                    'Model': model,
                    'Price': price,
                    'Listing URL': url,
                    'Image URL': image_url,
                    'Description': title
                })
            except Exception as e:
                print(f"LiveAuctioneers item parse error: {str(e)}")
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'LiveAuctioneers', 'Listings Found': len(soup.select('.lot')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'LiveAuctioneers', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
        print(f"LiveAuctioneers request failed: {str(e)}")
    update_logs(log_entry)
    time.sleep(random.uniform(2, 5))
    return pd.DataFrame(listings)