import urllib.request
import shutil
import zipfile
import pandas as pd
import sqlite3
import os

DATA_URI = 'https://gtfs.rhoenenergie-bus.de/GTFS.zip'
DATABASE_PATH = 'gtfs.sqlite'
TABLE_NAME = 'stops'


def automated_data_pipeline():

    # download zip file and choose the relevant file out of its content
    urllib.request.urlretrieve(DATA_URI, 'data.zip')
    with zipfile.ZipFile('data.zip', 'r') as zip_ref:
        zip_ref.extractall('data')
    df = pd.read_csv('data/stops.txt', encoding='utf-8', delimiter=',')  # encoding keeps german umlauts

    # keep only specified columns
    columns_to_keep = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']
    df = df.drop(columns=set(df.columns) - set(columns_to_keep))
    df['stop_id'] = df['stop_id'].astype(int)
    df['stop_name'] = df['stop_name'].astype(str)
    df['stop_lat'] = df['stop_lat'].astype(float)
    df['stop_lon'] = df['stop_lon'].astype(float)
    df['zone_id'] = df['zone_id'].astype(int)
    data_types = ['INTEGER', 'TEXT', 'REAL', 'REAL', 'INTEGER']
    assert len(data_types) == len(columns_to_keep), 'Data type specification invalid.'

    # filter data: only keep stops from zone 2001
    df = df[df['zone_id'] == 2001]

    # validate that string field `stop_name` contains actually only alphabetic text
    df = df[df['stop_name'].str.match(r'^[A-Za-z\säöüÄÖ,.ßÜ\/-]+$')]  # set of charecters to allow

    # validate coordinate value range
    min_val, max_val = -90.0, 90.0
    df = df[df['stop_lon'].between(min_val, max_val)]
    df = df[df['stop_lat'].between(min_val, max_val)]

    # validate data in general
    df = df.dropna()

    # persist data
    column_types = dict(zip(df.columns, data_types))
    conn = sqlite3.connect(DATABASE_PATH)
    n = df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False, dtype=column_types)
    conn.close()
    print(f'Saved {n} records into database at {os.path.abspath(DATABASE_PATH)}.')

    # clean artifacts
    shutil.rmtree('data')
    os.remove('data.zip')


if __name__ == '__main__':
    automated_data_pipeline()
