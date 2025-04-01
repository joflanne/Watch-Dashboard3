import streamlit as st
import pandas as pd
from scraper import (
    scrape_ebay, scrape_chrono24, scrape_watchbox, scrape_yahoo_japan, scrape_jomashop,
    scrape_crown_caliber, scrape_sothebys, scrape_christies, scrape_phillips, scrape_bonhams,
    scrape_antiquorum, scrape_watchuseek, scrape_reddit, scrape_catawiki, scrape_timepeaks,
    scrape_bobs_watches, scrape_1stdibs, scrape_watchcollecting, scrape_invaluable, scrape_liveauctioneers,
    detect_fake, estimate_liquidity, get_resale_price
)
from datetime import datetime
import os

DATA_FILE = "watches.csv"
LOGS_FILE = "scraper_log.csv"
HISTORICAL_FILE = "historical_deals.csv"

def save_to_file(filename, data):
    data.to_csv(filename, index=False)

def load_from_file(filename, default_columns=None):
    try:
        return pd.read_csv(filename)
    except FileNotFoundError:
        return pd.DataFrame(columns=default_columns) if default_columns else pd.DataFrame()

@st.cache_data(ttl=3600)
def load_data():
    data_sources = [
        scrape_ebay(2000), scrape_chrono24(2000), scrape_watchbox(2000), scrape_yahoo_japan(2000), scrape_jomashop(2000),
        scrape_crown_caliber(2000), scrape_sothebys(2000), scrape_christies(2000), scrape_phillips(2000), scrape_bonhams(2000),
        scrape_antiquorum(2000), scrape_watchuseek(2000), scrape_reddit(2000), scrape_catawiki(2000), scrape_timepeaks(2000),
        scrape_bobs_watches(2000), scrape_1stdibs(2000), scrape_watchcollecting(2000), scrape_invaluable(2000), scrape_liveauctioneers(2000)
    ]
    data = pd.concat([d for d in data_sources if not d.empty], ignore_index=True)
    
    data['Predicted Resale'] = data.apply(lambda row: get_resale_price(row['Brand'], row['Model']) or row['Price'] * 1.7, axis=1)
    data['Expected Profit'] = data['Predicted Resale'] - data['Price']
    data['ROI (%)'] = (data['Expected Profit'] / data['Price']) * 100
    
    data['Authenticity'] = data.apply(detect_fake, axis=1)
    data['Liquidity'] = data.apply(lambda row: estimate_liquidity(row['Brand'], row['Model']), axis=1)
    data['Age (Days)'] = data['Date Listed'].apply(lambda x: (datetime.now() - datetime.strptime(x, '%Y-%m-%d')).days)
    
    save_to_file(DATA_FILE, data)
    return data

@st.cache_data(ttl=3600)
def load_logs():
    return load_from_file(LOGS_FILE, ['Timestamp', 'Platform', 'Listings Found', 'Listings Saved', 'Errors'])

@st.cache_data(ttl=3600)
def load_historical():
    return load_from_file(HISTORICAL_FILE)

st.set_page_config(page_title='Watch Arbitrage Dashboard', layout='wide')
data = load_data()
logs = load_logs()
historical = load_historical()

st.title("⌚ Watch Arbitrage Dashboard")
tab1, tab2, tab3 = st.tabs(["📈 Deals", "🛠 Scraper Console", "📜 Historical Deals"])

with tab1:
    st.subheader("Top Deals (ROI ≥ 100%)")
    filtered_data = data[
        (data['ROI (%)'] >= 40) & 
        (data['Expected Profit'] >= 500) & 
        (data['Price'] <= 2000) & 
        (data['Authenticity'] == 'Likely Genuine') & 
        (data['Liquidity'] == 'High') & 
        (data['Age (Days)'] <= 30)
    ]
    filtered_data['In Historical'] = filtered_data['Listing URL'].isin(historical['Listing URL'])
    filtered_data = filtered_data[~filtered_data['In Historical']]
    save_to_file(HISTORICAL_FILE, pd.concat([historical, filtered_data], ignore_index=True))
    
    top_deals = filtered_data[filtered_data['ROI (%)'] >= 100]
    st.dataframe(top_deals.style.format({
        'Price': '${:,.2f}', 'Predicted Resale': '${:,.2f}', 'Expected Profit': '${:,.2f}', 'ROI (%)': '{:.2f}%', 'Age (Days)': '{:.0f}'
    }))
    
    st.subheader("Filtered Listings (ROI ≥ 40%, Profit ≥ $500, Price < $2K)")
    st.dataframe(filtered_data.style.format({
        'Price': '${:,.2f}', 'Predicted Resale': '${:,.2f}', 'Expected Profit': '${:,.2f}', 'ROI (%)': '{:.2f}%', 'Age (Days)': '{:.0f}'
    }))

with tab2:
    st.subheader("Scraper Logs")
    platform_filter = st.selectbox("Filter by Platform", ["All"] + sorted(logs["Platform"].unique()))
    if platform_filter != "All":
        logs = logs[logs["Platform"] == platform_filter]
    st.dataframe(logs)
    st.download_button("Download Logs", logs.to_csv(index=False), "scraper_log.csv")
    st.download_button("Download Listings", filtered_data.to_csv(index=False), "watches.csv")
    st.success("Scraper runs hourly via Heroku Scheduler")

with tab3:
    st.subheader("Historical Deals")
    st.dataframe(historical.style.format({
        'Price': '${:,.2f}', 'Predicted Resale': '${:,.2f}', 'Expected Profit': '${:,.2f}', 'ROI (%)': '{:.2f}%', 'Age (Days)': '{:.0f}'
    }))