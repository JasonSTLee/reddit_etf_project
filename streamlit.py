import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import re
import yfinance as yf
from pyetfdb_scraper.etf import ETF, load_etfs
from collections import Counter
import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2
import psycopg2.extras
import praw
from sqlalchemy import create_engine
import os
import sys
from utils import *


# Tab name on browser
st.set_page_config(page_title = 'reddit_etf_project', page_icon = ':bar_chart:')

# Title of dashboard
st.title('ETFs Subreddit Project')


### METRIC CONTAINER ###

# Metric title
st.write('### Post Metrics')

# Initializing metric variables for post info
metric_1, metric_2, metric_3, metric_4 = st.columns(4)

# Metrics

# Number of posts
post_list = get_column(pg_column = 'url', pg_table = 'title_url', limit = None)
post_num = len(post_list)
metric_1.metric('Num of Posts', post_num) 

# Number of top comments
top_list = get_column(pg_column = 'id', pg_table = 'top_comment', limit = None)
top_num = len(top_list)
metric_2.metric('Num of Top Comments', top_num) 

# Number of second comments
second_list = get_column(pg_column = 'id', pg_table = 'second_comment', limit = None)
second_num = len(second_list)
metric_3.metric('Num of Second Comments', second_num) 

# Number of unique etfs
etf_list = get_column(pg_column = 'etfs', pg_table = 'etf_cnt_year', limit = None)
etf_num = len(etf_list)
metric_4.metric('Num of Unique ETFs', etf_num)


### ETF HORIZOINTAL BAR GRAPH ###
st.write('### Top 10 ETFs by Count')

# Graph style
plt.style.use('ggplot')  

# Retrieving PG table and cleaning values
etf_df = get_table(pg_column = '*', pg_table = 'etf_cnt_year')
etf_df['count'] = etf_df['count'].astype(int) # count column is varchar
etf_sort = etf_df.sort_values(by = 'count', ascending = False)
etf_head = etf_sort.head(10)
etf_head_sort = etf_head.sort_values(by = 'count') # sorting again to show the highest number first on graph
etf_cnt = etf_head_sort['count'] # x values
etf_names = etf_head_sort['etfs'] # y values

# Plotting graph
etf_fig, ax = plt.subplots()
ax.barh(etf_names, etf_cnt)

# Labels and title
ax.set_xlabel('Count of Appearances')
ax.set_ylabel('ETF Ticker')

st.pyplot(etf_fig)