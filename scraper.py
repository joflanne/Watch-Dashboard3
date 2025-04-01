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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Phillips', 'Listings Found': len(soup.select('.lot-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Phillips', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_bonhams(max_price):
    url = "https://www.bonhams.com/departments/WAT/?price_range=0-2000"
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Bonhams', 'Listings Found': len(soup.select('.lot')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Bonhams', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_antiquorum(max_price):
    url = "https://www.antiquorum.swiss/?price_max=2000"
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Antiquorum', 'Listings Found': len(soup.select('.lot-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Antiquorum', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_watchuseek(max_price):
    url = "https://www.watchuseek.com/forums/watch-sales-forum.16/"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.thread'):
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Watchuseek', 'Listings Found': len(soup.select('.thread')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Watchuseek', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_reddit(max_price):
    url = "https://www.reddit.com/r/WatchExchange/new/"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.Post'):
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Reddit WatchExchange', 'Listings Found': len(soup.select('.Post')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Reddit WatchExchange', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_catawiki(max_price):
    url = "https://www.catawiki.com/en/c/7-watches?price_max=2000"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.lot'):
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Catawiki', 'Listings Found': len(soup.select('.lot')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Catawiki', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_timepeaks(max_price):
    url = "https://timepeaks.com/?price_max=2000"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.item'):
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Timepeaks', 'Listings Found': len(soup.select('.item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Timepeaks', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_bobs_watches(max_price):
    url = "https://www.bobswatches.com/auctions?price_max=2000"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.auction-item'):
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Bob’s Watches', 'Listings Found': len(soup.select('.auction-item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Bob’s Watches', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_1stdibs(max_price):
    url = "https://www.1stdibs.com/jewelry/watches/?price_max=2000"
    listings = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.item'):
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': '1stDibs', 'Listings Found': len(soup.select('.item')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': '1stDibs', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_watchcollecting(max_price):
    url = "https://watchcollecting.com/?price_max=2000"
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'WatchCollecting', 'Listings Found': len(soup.select('.lot')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'WatchCollecting', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_invaluable(max_price):
    url = "https://www.invaluable.com/watches/sc-7L7J8J8J8J/?price_max=2000"
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Invaluable', 'Listings Found': len(soup.select('.lot')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'Invaluable', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def scrape_liveauctioneers(max_price):
    url = "https://www.liveauctioneers.com/c/watches/7/?price_max=2000"
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
            except:
                continue
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'LiveAuctioneers', 'Listings Found': len(soup.select('.lot')), 'Listings Saved': len(listings), 'Errors': 'None'}
    except Exception as e:
        log_entry = {'Timestamp': datetime.now().strftime('%Y-%m-%d %H:%M'), 'Platform': 'LiveAuctioneers', 'Listings Found': 0, 'Listings Saved': 0, 'Errors': str(e)}
    update_logs(log_entry)
    time.sleep(random.uniform(1, 3))
    return pd.DataFrame(listings)

def detect_fake(row):
    if 'no serial' in row['Description'].lower() or 'replica' in row['Description'].lower() or row['Price'] < 100:
        return 'Likely Fake'
    market_prices = {'Rolex Submariner': 8000, 'Seiko Prospex': 400, 'Omega Speedmaster': 3000}
    key = f"{row['Brand']} {row['Model']}"
    if key in market_prices and row['Price'] < market_prices[key] * 0.3:
        return 'Likely Fake'
    return 'Likely Genuine'

def estimate_liquidity(brand, model):
    sold_data = scrape_ebay_sold(brand, model)
    avg_days = sum([d['Days Ago'] for d in sold_data]) / len(sold_data) if sold_data else 30
    return 'High' if avg_days < 14 else 'Low'

def get_resale_price(brand, model):
    sold_data = scrape_ebay_sold(brand, model)
    return sum([d['Price'] for d in sold_data]) / len(sold_data) if sold_data else None

def scrape_ebay_sold(brand, model):
    url = f"https://www.ebay.com/sch/i.html?_nkw={brand}+{model}&LH_Sold=1&LH_Complete=1"
    sold_data = []
    try:
        response = requests.get(url, headers=HEADERS, proxies=get_proxy(), timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')
        for item in soup.select('.s-item'):
            try:
                price_str = item.select_one('.s-item__price').text.replace('$', '').replace(',', '')
                price = float(price_str.split(' to ')[0] if ' to ' in price_str else price_str)
                date_sold = item.select_one('.s-item__ended-date').text.strip()
                days_ago = (datetime.now() - datetime.strptime(date_sold, '%b-%d %H:%M')).days
                sold_data.append({'Price': price, 'Days Ago': days_ago})
            except:
                continue
    except:
        pass
    return sold_data