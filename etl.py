import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    load data from S3 bucket to the staging tables
    """
    for query in copy_table_queries:
        cur.execute(query)
        print('Processing query: {}'.format(query))
        conn.commit()
        print('{} processed OK.'.format(query))

    print('All files COPIED OK.')

def insert_tables(cur, conn):
    """
    select and transform data from staging tables into dimensional tables
    """
    print("Start inserting data from staging tables into analysis tables...")
    for query in insert_table_queries:
        cur.execute(query)
        print('Processing query: {}'.format(query))
        conn.commit()
        print('{} processed OK.'.format(query))

    print('All files COPIED OK.')

def main():
    """
    Run each processes - Load data from S3 to stage and insert them to the dimensional tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("AWS Redshift connection established OK.")
    print("load staging tables...")
    
    load_staging_tables(cur, conn)
    print("load staging tables...")
    insert_tables(cur, conn)
    print("insert tables...")

    conn.close()
    print("Jobs done")


if __name__ == "__main__":
    main()