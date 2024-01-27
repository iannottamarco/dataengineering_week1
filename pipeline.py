# Main libraries import
import logging
import sys
import os
import argparse
import pandas as pd
import gzip
from sqlalchemy import create_engine
from time import time

# Setup logging
logging.basicConfig(filename='development.log', format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = int(params.port)
    database_name = params.db_name
    table_name = params.table_name
    csv_url = params.csv_url
    
    csv_name = 'ny_taxis_data.csv'
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-09.csv.gz'
    print('ciao')
    os.system(f"wget {csv_url} -O {csv_name}")


    logging.info("Creating engine")
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')

    logging.info("Ingesting data.")
    with gzip.open(csv_name, 'rt', encoding='utf-8') as f:
        df_iter = pd.read_csv(f, iterator=True, chunksize=50000)

        df_first_chunk = next(df_iter)  # Read the first chunk
        logging.info("Creating table.")
        # Now use the first chunk (a DataFrame) to get the schema
        pd.io.sql.get_schema(df_first_chunk, name=args.table_name, con=engine)

        df_first_chunk.head(0).to_sql(name=table_name, con=engine, if_exists='replace')


    with gzip.open(csv_name, 'rt', encoding='utf-8') as f:
        df_iter = pd.read_csv(f, iterator=True, chunksize=50000)

        i = 0
        while True:
            try:
                i += 1
                t_start = time()
                logging.info(f"Importing chunk: {i}")
                df = next(df_iter)
                df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
                df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
                df.to_sql(name=table_name, con=engine, if_exists='append')
                t_end = time()

                logging.info(f"Importing chunk took {round(t_end - t_start,2)} seconds.")
                
            except StopIteration:
                logging.info("Importing completed")
                break


        logging.info("Job completed")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')

    # user, password, host, port, database name, table name, url of csv
    parser.add_argument('--user', help='Username for postgres.')
    parser.add_argument('--password', help='Password for postgres.')
    parser.add_argument('--host', help='Postgres Host.')
    parser.add_argument('--port', help='Postgres Port.')
    parser.add_argument('--db_name', help='Database name.')
    parser.add_argument('--table_name', help='Table name.')
    parser.add_argument('--csv_url', help='URL of the CSV file.')

    args = parser.parse_args()
    main(args)