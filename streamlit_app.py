import streamlit as st
import pandas as pd
from scraper import (
    scrape_ebay, scrape_chrono24, scrape_watchbox, scrape_yahoo_japan, scrape_jomashop,
    scrape_crown_caliber, scrape_sothebys, scrape_christies, scrape_phillips, scrape_bonhams,
    scrape_antiquorum, scrape_watchuseek, scrape_reddit, scrape_catawiki, scrape_timepeaks,
    scrape_bobs_watches, scrape_1stdibs, scrape_watchcollecting, scrape_invaluable, scrape_liveauctioneers
)
from datetime import datetime
import os

DATA_FILE = "watches.csv"
LOGS_FILE = "scraper_log.csv"

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
    valid_data = [d for d in data_sources if not d.empty]
    if valid_data:
        data = pd.concat(valid_data, ignore_index=True)
    else:
        data = pd.DataFrame(columns=['Date Listed', 'Platform', 'Brand', 'Model', 'Price', 'Listing URL', 'Image URL', 'Description'])
    
    save_to_file(DATA_FILE, data)
    return data

@st.cache_data(ttl=3600)
def load_logs():
    return load_from_file(LOGS_FILE, ['Timestamp', 'Platform', 'Listings Found', 'Listings Saved', 'Errors'])

st.set_page_config(page_title='Watch Inventory Dashboard', layout='wide')
data = load_data()
logs = load_logs()

st.title("⌚ Watch Inventory Dashboard")
tab1, tab2 = st.tabs(["📋 Inventory", "🛠 Scraper Logs"])

with tab1:
    st.subheader("Watch Listings")
    st.dataframe(data.style.format({
        'Price': '${:,.2f}'
    }))

with tab2:
    st.subheader("Scraper Logs")
    platform_filter = st.selectbox("Filter by Platform", ["All"] + sorted(logs["Platform"].unique()))
    if platform_filter != "All":
        logs = logs[logs["Platform"] == platform_filter]
    st.dataframe(logs)
    st.download_button("Download Logs", logs.to_csv(index=False), "scraper_log.csv")
    st.download_button("Download Inventory", data.to_csv(index=False), "watches.csv")
    st.success("Scraper runs hourly via Heroku Scheduler")