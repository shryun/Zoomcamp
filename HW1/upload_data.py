import pandas as pd
from sqlalchemy import create_engine
import time
import argparse
import zipfile
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name[0]
    url = params.url[0]  # First URL from the list

    # Determine file type and name
    file_name = url.split('/')[-1]
    is_zip = file_name.endswith('.csv.gz')
    csv_name = file_name.replace('.csv.gz', '.csv')

    # Download the file
    print(f"Downloading {url}...")
    os.system(f"wget {url} -O {file_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #define chunk size
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)

    #upload the first chunk
    df = next(df_iter)
    
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    #df.head(n=0) ensures that no actual data is written, but the column names and data types of the DataFrame are used to create the table
    #to_sql() is used to write a pandas dataframe to a database table
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    #insert the rest of the chunk
    while True:
        t_start = time.time()

        df = next(df_iter)

        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

        df.to_sql(name = table_name, con = engine, if_exists = 'append')

        t_end = time.time()

        print('inserted another chunk, took %.3f second' % (t_end - t_start))

def zone(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name[1]
    url = params.url[1]  # Second URL argument is the taxi zone lookup CSV

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    print(f"Uploading {url}...")

    # Load zone data
    df = pd.read_csv(url)

    # Upload zone data to a separate table
    df.to_sql(name='zones', con=engine, if_exists='replace', index=False)
    print(f"{url} uploaded successfully!")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='upload csv data to postgres')

    parser.add_argument('--user', help='user name for postgres') 
    parser.add_argument('--password', help='password for postgres') 
    parser.add_argument('--host', help='host for postgres') 
    parser.add_argument('--port', help='port for postgres') 
    parser.add_argument('--db', help='database for postgres') 
    parser.add_argument('--table_name',  args='+', help='name of the table where we will write the results to') 
    parser.add_argument('--url', nargs='+', help='url of the csv file') 

    args = parser.parse_args()

    main(args)
    zone(args)