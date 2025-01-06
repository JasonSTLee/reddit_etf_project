import re
from pyetfdb_scraper.etf import ETF, load_etfs
from collections import Counter
import pandas as pd
import numpy as np
from datetime import datetime
import psycopg2
import psycopg2.extras
import os
import sys
import praw
from sqlalchemy import create_engine
import config


timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')


def delete_create_table(df, table_name, column_list, column_schema_list):
    
    """
    
    Creating a PostgreSQL table from Pandas DataFrame
    
    Parameters:
        df (pd DataFrame): 
            Pandas DataFrame (clean Dataframe beforehand so the input is the intended dtype)
        table_name (str): 
            Name shown in Postgres
        column_list (list): 
            List of column names in DataFrame
        column_schema_list (list): 
            List of dtypes from DataFrame that must match the order of column_list
        
    
    """
    
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host = config.postgres_host, 
            dbname = config.dbname, 
            user = config.postgres_user, 
            password = config.postgres_pw, 
            port = config.postgres_port
        )
        cur = conn.cursor()

        # Drop table if it exists
        cur.execute(f""" 
                    DROP TABLE IF EXISTS {table_name}
                    """)

        # Construct CREATE TABLE dynamically
        columns_with_types = ', '.join([f'{col} {dtype}' for col, dtype in zip(column_list, column_schema_list)])
        create_table_query = f"""
                    CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types})
                    """
    

        # Execute CREATE TABLE 
        cur.execute(create_table_query)

        # Construct INSERT query dynamically
        placeholders = ", ".join(["%s"] * len(column_list))
        insert_query = f"""
                    INSERT INTO {table_name} ({', '.join(column_list)}) VALUES ({placeholders})
                    """

        # Insert rows from dataframe
        for _, row in df.iterrows():
            cur.execute(insert_query, tuple(row[col] for col in column_list))


        conn.commit()
        cur.close()
        conn.close()
    except Exception as error:
        print(error)
        
    # After the Try...Except...close the cursor and connect to prevent database leaks
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()



def append_table(df, pg_table):
    
    """
    
    Appends a Pandas DataFrame to an existing PostgreSQL table.
    
    Returns: 
        Nothing
    
    Parameters:
        df (pd DataFrame): 
            Pandas DataFrame (clean Dataframe beforehand so the input is the intended dtype)
        pg_table (str): 
            name of the table in PG
        
    
    """
    
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host = config.postgres_host, 
            dbname = config.dbname, 
            user = config.postgres_user, 
            password = config.postgres_pw, 
            port = config.postgres_port
        )
        cur = conn.cursor()

        # Dynamically create a list of column names
        column_list = df.columns.tolist()  
        # Create placeholder values
        placeholders = ", ".join(["%s"] * len(column_list))
        
        insert_query = f"""
            INSERT INTO {pg_table} ({', '.join(column_list)}) VALUES ({placeholders})
        """

        # Execute query to insert batch data
        cur.executemany(insert_query, df.to_records(index=False))

        
        
        # Commit changes
        conn.commit()
        cur.close()
        conn.close()
    except Exception as error:
        print(error)
        
    # After the Try...Except...close the cursor and connect to prevent database leaks
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            
    
    
reddit = praw.Reddit(
                user_name = config.user_name,
                password = config.password,
                client_id = config.client_id,
                client_secret = config.secret_id,
                user_agent = config.user_agent
)



def get_column(pg_table, pg_column, limit = 25, where = None):
    
    """
    
    Input a table and column and returns the column in the form of a list
    
    Returns:
        List of values from pg_column
        
    Parameters:
        pg_table (str): name of the table in PG
        pg_column (str): name of the column in PG
        limit (int): limit clause in PG, default is 25
        where (str): where clause in PG, default is none
        
    
    """
    
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host = config.postgres_host, 
            dbname = config.dbname, 
            user = config.postgres_user, 
            password = config.postgres_pw, 
            port = config.postgres_port
        )
        cur = conn.cursor()

        query = f""" 
                    SELECT {pg_column} FROM {pg_table}
                """
                
        if limit is not None and where is None:
            query += f""" ORDER BY date_time::timestamp DESC LIMIT {limit}"""
        elif limit is not None and where is not None:
            query += f""" WHERE {where} ORDER BY date_time::timestamp DESC LIMIT {limit}"""
        elif limit is None and where is not None:
            query += f""" WHERE {where} """
                    
        # Execute query
        cur.execute(query)
        
        tmp = cur.fetchall()
        
        # Tmp results in tuple, turn list of tuples into a regular list
        regular_list = [item for sublist in tmp for item in sublist]


        conn.commit()
        cur.close()
        conn.close()
        
        return regular_list
    
    except Exception as error:
        print(error)
        
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()





def get_table(pg_table = 'title_url', pg_column = '*', clause = None):
    
    """
    
    Similar to get_column but with an entire table
    
    Returns:
        List of values from pg_column
        
    Parameters:
        pg_table (str): name of the table in PG, default is title_url
        pg_column (str): name of the column in PG, default is all columns
        clause (str): any other clauses in PG for example - joins, limit, where etc, default is none
        
    
    
    """
    
    try:
        # Connect to the database
        conn = psycopg2.connect(
            host = config.postgres_host, 
            dbname = config.dbname, 
            user = config.postgres_user, 
            password = config.postgres_pw, 
            port = config.postgres_port
        )
        cur = conn.cursor()

        df = pd.read_sql(f'''
                         select {pg_column} from {pg_table} 
                         ''', con = conn)
        
        if clause is not None:
            df = pd.read_sql(f'''
                         select {pg_column} from {pg_table} {clause}
                         ''', con = conn)


        conn.commit()
        cur.close()
        conn.close()
        
        return df
    
    except Exception as error:
        print(error)
        
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
            
            
            
def url_dataframes(url, method = None):
    
    """
    
    Inputs a url and returns dataframes
    
    Returns:
        List of 3 dataframes
            1. title_url_df
            2. top_comment_df
            3. second_comment_df
        
    Parameters:
        url (str): url name that can't be jpeg or png
        method (str, optionbal): appends the 3 created dataframes onto the existing table
        
    
    """
    
    
    # JPEG and PNG URLs can't be extracted so they will excluded
    no_url = ['jpeg', 'png', 'pdf']
    
    if any(keyword in url for keyword in no_url):
        print(f'{url} has jpeg, pdf or png')
        
        return None, None, None
        
    else:
        try: 
            # Initialize variable to extract post info
            submission = reddit.submission(url = url)
            
            # Initialize lists and append data to create dataframe
            title_list = []
            url_list = []
            upvote_list = []
            date_time_list = []
            
            url_list.append(submission.url)
            title_list.append(submission.title)
            upvote_list.append(str(submission.upvote_ratio))
            date_time_list.append(str(timestamp))
            
            top_comment_id_list = []
            top_comment_body_list = []
            top_comment_votes_list = []
            top_comment_url_list = []

            second_comment_id_list = []
            second_comment_body_list = []
            second_comment_votes_list = []
            second_comment_parentID_list = []

            # Limit 2 means second layer in the comment forest
            submission.comments.replace_more(limit = 2)
            
            for top_level_comment in submission.comments:
                comment = str(top_level_comment)
                top_comment_id_list.append(comment)
                top_comment_body_list.append(top_level_comment.body)
                top_comment_votes_list.append(str(top_level_comment.score))
                top_comment_url_list.append(url)
                
                for second_level_comment in top_level_comment.replies:
                    comment = str(second_level_comment)
                    second_comment_id_list.append(comment)
                    second_comment_body_list.append(second_level_comment.body)
                    second_comment_votes_list.append(str(second_level_comment.score))
                    second_comment_parentID_list.append(second_level_comment.parent_id)

            title_url_df = pd.DataFrame({
                'title': title_list,
                'url': url_list,
                'upvote_ratio': upvote_list,
                'date_time': date_time_list
            })
            
            top_comment_df = pd.DataFrame({
                'id': top_comment_id_list,
                'post_url': top_comment_url_list,
                'votes': top_comment_votes_list,
                'body': top_comment_body_list
            })

            second_comment_df = pd.DataFrame({
                'id': second_comment_id_list,
                'parentID': second_comment_parentID_list,
                'votes': second_comment_votes_list,
                'body': second_comment_body_list
            })
            
            # Optional parameter to append the URL info to existing PG table
            if method == 'append':
                append_table(title_url_df, 'title_url')
                append_table(top_comment_df, 'top_comment')
                append_table(second_comment_df, 'second_comment'),
                
                
            return [title_url_df, top_comment_df, second_comment_df]
        
        except Exception as error:
            print(f'Error in url_dataframes: {error}')
            
            return None, None, None
        
        
    
def reddit_posts(limit, subreddit = None):
    
    """
    
    Returns:
        Dataframe of most recent 25 hottest posts from any subreddit with title, url, upvotes as columns
    
    Parameters:
        limit (int): number of posts to retrieve
        subreddit (str): name of the subreddit to extract, default is Funny
        
    
    """
    
    
    if subreddit:
        try:
            subred = reddit.subreddit(f'{subreddit}')

            hot = subred.hot(limit = limit)

            title_list = []
            url_list = []
            upvote_list = []
            date_time_list = []

            for i in hot:
                if 'jpeg' in i.url or 'png' in i.url:
                    continue
                
                url_list.append(i.url)
                title_list.append(i.title)
                upvote_list.append(i.upvote_ratio)
                date_time_list.append(timestamp)


            title_url_df = pd.DataFrame({
                'title': title_list,
                'url': url_list,
                'upvote_ratio': upvote_list,
                'date_time': date_time_list
            })
        
            return title_url_df
        
        except Exception as error:
            print(f'Error in reddit_posts: {error}')
    else:
        print('Enter a subreddit')
            

            
def reddit_pg_tables(limit, subreddit = 'Funny', method = 'replace'):
    
    """
    
    Extracts Reddit information from any subreddit and returns a list of 3 tables that connects to Postgre
    
    Returns a list of dataframes:    
        1. Top 25 posts with title, URL, and upvotes
        2. Top layer comments with comment_id, post_url, upvotes, body
        3. Second layer comments with comment_id, parent_comment_id, upvotes, body
        
    Parameters:
        subreddit (str): name of subreddit, default is Funny
        method (str): 
            'replace' to delete and create a new table 
            'append' to append additional dataframes to exisitng table
    
                
    """
    
    try:
        title_url_df = reddit_posts(limit = limit, subreddit = f'{subreddit}')
        
        url_list = title_url_df['url'].tolist()

        # Initialize each dataframes with their column names
        all_title_url_df = []
        all_top_comment_df = []
        all_second_comment_df = []

        # Loop each URL through url_dataframes
        for link in url_list:
            result = url_dataframes(url = link, method = method)
            if result:  
                # Returning 3 variables for the 3 dataframes from url_dataframes
                title_url_df, top_comment_df, second_comment_df = result
                
                all_title_url_df.append(title_url_df)
                all_top_comment_df.append(top_comment_df)
                all_second_comment_df.append(second_comment_df)
            
            
        title_url_df = pd.concat(all_title_url_df)
        top_comment_df = pd.concat(all_top_comment_df)
        second_comment_df = pd.concat(all_second_comment_df)
        
        # Deletes and creates new table in PG
        if method == 'replace':
            # Creates list of columns as names, and list of VARCHAR columns 
            delete_create_table(title_url_df, 'title_url', title_url_df.columns.tolist(), ['VARCHAR'] * len(title_url_df.columns))
            delete_create_table(top_comment_df, 'top_comment', top_comment_df.columns.tolist(), ['VARCHAR'] * len(top_comment_df.columns))
            delete_create_table(second_comment_df, 'second_comment', second_comment_df.columns.tolist(), ['VARCHAR'] * len(second_comment_df.columns))
        # Appends 3 dataframes to existing PG tables
        elif method == 'append':
            append_table(title_url_df, 'title_url')
            append_table(top_comment_df, 'top_comment')
            append_table(second_comment_df, 'second_comment')
        else:
            print('Use "replace" or "append" method')
            
            
        return [title_url_df, top_comment_df, second_comment_df]
    
    except Exception as error:
        print(f'Error in reddit_pg_tables: {error}')
        
        return []
    


def new_posts(new_limit, old_limit, subreddit, method = None):

    """
    
    Retrieves all posts that are not currently in the PG database
    
    Returns a list of dataframes:    
        1. Title, URL, and upvotes
        2. Top layer comments with comment_id, post_url, upvotes, body
        3. Second layer comments with comment_id, parent_comment_id, upvotes, body
        
    Parameters:
        new_limit (int): number of posts to extract from subreddit
        old_limit (int): number of posts to extract from PG table to compare to new posts
        subreddit (str): name of subreddit, default is Funny
        method (str): 
            'append' to append additional dataframes to exisitng table
    
                
    """
    try:
        old_list = get_column(pg_column = 'url', pg_table = 'title_url', limit = old_limit)

        new_list = reddit_posts(limit = new_limit, subreddit = f'{subreddit}')['url'].to_list()


        new_urls = list(set(new_list) - set(old_list))

        if new_urls:
            title_url_list = []
            top_comment_list = []
            second_comment_list = []
            
            for url in new_urls:
                results = url_dataframes(url = url)
                
                title_url_df, top_comment_df, second_comment_df = results
                
                title_url_list.append(title_url_df)
                top_comment_list.append(top_comment_df)
                second_comment_list.append(second_comment_df)
            
            final_title_url_df = pd.concat(title_url_list)
            final_top_comment_df = pd.concat(top_comment_list)
            final_second_comment_df = pd.concat(second_comment_list)
        elif not new_urls:
            print('No new URLs')
            
        if method == 'append':
            for url in new_urls:
                url_dataframes(url = url, method = 'append')
        
        
        return [final_title_url_df, final_top_comment_df, final_second_comment_df]
    
    except Exception as error:
        print(f'Error in new_posts: {error}')



def new_comments(append = False):
    """
    
    Looks up URLs in PG table to retrieve new top comments
    
    Returns one dataframes of top_comment
        
    Parameters:
        append (boolean): append the dataframe to PG table, if True then append. Default is False
    
                
    """
    existing_urls = get_column(pg_column = 'url', pg_table = 'title_url', limit = None)

    old_top = get_table(
        pg_table = 'title_url', 
        pg_column = '*', 
        clause = 'join top_comment on title_url.url = top_comment.post_url')

    old_top_df = old_top['id']

    top_df_list = []

    for urls in existing_urls:
        new_top = url_dataframes(url = urls)[1]
        
        top_merge = new_top.merge(old_top_df, on = 'id', how = 'left', indicator = True)
        top_final = top_merge.query(' _merge == "left_only" ')[['id', 'post_url', 'votes', 'body']]
        top_df_list.append(top_final)
        
    top_df = pd.concat(top_df_list)

    if append == True:
        append_table(top_df, 'top_comment')
        
    return top_df



def extract_etfs(period = None, push_to_pg = False):
    
    """
    
    Scans the title and body for mentions of any ETF
    
    Returns a dataframes with columns:
        1. Period: breakdown of the time period to examine the dataframe
        2. ETFs: name of the ETFs
        3. Count: number of times the ETF was mentioned in that time period
        
    Parameters:
        period (str): 
            'day': level view as day
            'week': level view as week
            'month': level view as month
            'year': level view as year
        push_to_pg (boolean): False is default, pushes the dataframe to PG
            False: returns a dataframe
            True: pushes table to PG

    
                
    """
    
    
    etfs = load_etfs()

    df = get_table(
            pg_table = 'title_url', 
            pg_column = 'date_time::timestamp::date::varchar, title as words', 
            clause = "union select date_time::timestamp::date::varchar, top_comment.body from title_url full join top_comment ON top_comment.post_url = title_url.url WHERE date_time is not null union select date_time::timestamp::date::varchar, second_comment.body from title_url full join top_comment on title_url.url = top_comment.post_url full join second_comment ON CONCAT('t_1', top_comment.id) = second_comment.parentid WHERE date_time is not null")

    etfs = load_etfs()
    all_df = []

    for row in df.itertuples():
        word_str = str(row.words)
        words_list = [word.upper() for word in word_str.split() if 2 <= len(word) < 5 and word.isalpha() and word in etfs]
        
        cnt = Counter(words_list)
        common_cnt = dict(cnt.most_common())
        
        etf_cnt_df = pd.DataFrame(common_cnt.items(), columns = ['etfs', 'count'])
        etf_cnt_df['date'] = row.date_time
        all_df.append(etf_cnt_df)
        
    final_df = pd.concat(all_df)
    final_df['date'] = pd.to_datetime(final_df['date'])
    final_df['year'] = final_df['date'].dt.year
        
    if period == 'day':
        final_df[f'{period}'] = final_df['date']
        group_df = final_df.groupby(by = [f'{period}', 'etfs'])['count'].sum().reset_index()
    elif period == 'week':
        final_df[f'{period}'] = final_df['date'].dt.isocalendar().week
        group_df = final_df.groupby(by = [f'{period}', 'year', 'etfs'])['count'].sum().reset_index()
    elif period == 'month':
        final_df[f'{period}'] = final_df['date'].dt.month
        group_df = final_df.groupby(by = [f'{period}', 'year', 'etfs'])['count'].sum().reset_index()
    elif period == 'year':
        group_df = final_df.groupby(by = [f'{period}', 'etfs'])['count'].sum().reset_index()


    if push_to_pg is True:
        group_df_col = group_df.columns.tolist()
        group_df_schema = ['VARCHAR'] * len(group_df.columns)
        
        delete_create_table(df = group_df, table_name = f'etf_cnt_{period}', column_list = group_df_col, column_schema_list = group_df_schema)

    return group_df