import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import random

PROXY = "http://USERNAME:PASSWORD@zproxy.lum-superproxy.io:22225"  # Placeholder, update later
def get_proxy():
    return {'http': PROXY, 'https': PROXY}

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
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
        rates = requests.get("https://api.exchangerate-api.com/v4/latest/USD").json()['rates']
        return price / rates[currency] if currency in rates else price
    except:
        return price

def scrape_ebay(max_price):
    url = "https://www.ebay.com/sch/i.html?_nkw=used+watches&_sacat=0&rt=nc&LH_ItemCondition=3000&_pgn=1"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.s-item'):
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'eBay', 'Listings Found': len(soup.select('.s-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'eBay', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_chrono24(max_price):
    url = "https://www.chrono24.com/search/index.htm?condition=used&priceMax=2000&sortorder=1"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.article-item'):
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Chrono24', 'Listings Found': len(soup.select('.article-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Chrono24', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_watchbox(max_price):
    url = "https://www.watchbox.com/shop/pre-owned-watches/"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.product-tile'):
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'WatchBox', 'Listings Found': len(soup.select('.product-tile')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'WatchBox', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_yahoo_japan(max_price):
    url = "https://auctions.yahoo.co.jp/category/list/2084046857/?p=%E6%99%82%E8%A8%88&price_type=currentprice&max=2000"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.Product'):
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Yahoo Japan', 'Listings Found': len(soup.select('.Product')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Yahoo Japan', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_jomashop(max_price):
    url = "https://www.jomashop.com/preowned-watches.html?price=0-2000"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.product-item'):
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Jomashop', 'Listings Found': len(soup.select('.product-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Jomashop', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_crown_caliber(max_price):
    url = "https://www.crownandcaliber.com/collections/pre-owned-watches?sort_by=price-ascending&filter.v.price.gte=0&filter.v.price.lte=2000"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.product-grid-item'):
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Crown & Caliber', 'Listings Found': len(soup.select('.product-grid-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Crown & Caliber', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_sothebys(max_price):
    url = "https://www.sothebys.com/en/buy/watches?sort=price-asc&price_range=0-2000"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.lot-item'):
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Sotheby’s', 'Listings Found': len(soup.select('.lot-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Sotheby’s', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_christies(max_price):
    url = "https://www.christies.com/en/auctions/watches?sortby=PriceLowToHigh&priceRange=0-2000"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.lot'):
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Christie’s', 'Listings Found': len(soup.select('.lot')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Christie’s', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_phillips(max_price):
    url = "https://www.phillips.com/auctions/department/watches?price_range=0-2000"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.lot-item'):
            try:
                price_str = item.select_one('.price').text.replace('$', '').replace(',', '')
                price = float(price_str)
                if price > max_price:
                    continue
                title = item.select_one('.lot-title').text.strip()
                url = 'https://www.p