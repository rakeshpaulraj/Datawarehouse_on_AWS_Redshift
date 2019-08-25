import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, truncate_table_queries

def truncate_tables(cur, conn):
    '''
    Function to truncate tables.
    
    Connects to Redshift database and truncate the tables whose names are set in truncate_table_queries variable
    
    Parameters: Redshift cursor instance, Redshift connection instance 
    Returns  : None
    '''
    for query in truncate_table_queries:
        cur.execute(query)
        conn.commit()
        
def load_staging_tables(cur, conn):
    '''
    Function to load the staging tables.
    
    Connects to Redshift database using the supplied cursor and connection instances and loads the data from S3 buckets into staging tables using COPY command. The names of staging tables are set in variable copy_table_queries which is imported from sql_queries.py
    
    Parameters: Redshift cursor instance, Redshift connection instance
    Returns  : None
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Function to insert data into Dimension and Fact tables.
    
    Connects to Redshift database using the supplied cursor and connection instances and loads the data from staging tables into Dimension and Fact Tables. The INSERT queries are set in variable insert_table_queries which is imported from sql_queries.py
    
    Parameters: Redshift cursor instance, Redshift connection instance
    Returns  : None
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    main function to process song and log datasets
    
    Connects to Redshift database and the cursor is opened for data processing.
    Invokes functions to load data from staging tables into 4 dimension tables (songs, artists, users, time) 
    and 1 fact table (songplays).
    
    The connection parameters are set in dwh.cfg file.
    
    Parameters: None  
    Returns  : None
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #truncate_tables(cur, conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()