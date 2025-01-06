from utils import *

try:
    conn = psycopg2.connect(
                host = config.postgres_host, 
                dbname = config.dbname, 
                user = config.postgres_user, 
                password = config.postgres_pw, 
                port = config.postgres_port
            )
    cur = conn.cursor()

    # Checking if Reddit posts tables exist
    cur.execute(' select * from title_url ')

    if not bool(cur.rowcount):
        # If table does not exist, create a new one resulting in 3 dataframes/tables
        reddit_pg_tables(limit = 50, subreddit = 'ETFs', method = 'replace')
    else:
        # If table does exist, append new posts and comments to PG tables
        new_posts(new_limit = 50, old_limit = 25, subreddit = 'ETFs', method = 'append')
        new_comments(append = True)

    # Checking if ETF count per year table exists 
    cur.execute(' select * from etf_cnt_year ')

    if not bool(cur.rowcount):
        # Create table if one does not exist
        extract_etfs(period = 'year', push_to_pg = True)
    
    # Checking if ETF count per day table exists 
    cur.execute(' select * from etf_cnt_day ')

    if not bool(cur.rowcount):
        # Create table if one does not exist
        extract_etfs(period = 'day', push_to_pg = True)
    
    conn.commit()
except psycopg2.Error as e:
    print(f'Error: {e}')
finally:
    if cur:
        cur.close()
    if conn:
        conn.close()