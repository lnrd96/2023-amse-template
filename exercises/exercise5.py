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
    df = pd.read_csv('data/stops.txt', encoding='ISO-8859-1', delimiter=',')  # encoding keeps german umlauts

    # keep only specified columns
    columns_to_keep = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon', 'zone_id']
    df = df.drop(columns=set(df.columns) - set(columns_to_keep))
    data_types = ['INTEGER', 'TEXT', 'REAL', 'REAL', 'INTEGER']
    assert len(data_types) == len(columns_to_keep), 'Data type specification invalid.'

    # filter data: only keep stops from zone 2001
    df = df[df['zone_id'] == 2001]

    # validate that string field `stop_name` contains actually only alphabetic text
    df = df[df['stop_name'].str.match(r'^[A-Za-z\säöüÄÖÜ\/-]+$')]  # set of charecters to allow

    # validate coordinate value range
    min_val, max_val = -90.0, 90.0
    df = df[df['stop_lon'].between(min_val, max_val)]
    df = df[df['stop_lat'].between(min_val, max_val)]

    # validate data in general
    df = df.dropna()

    # persist data
    column_types = dict(zip(df.columns, data_types))
    conn = sqlite3.connect(DATABASE_PATH)
    df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False, dtype=column_types)
    conn.close()
    print('Saved data into database.')

    # clean artifacts
    shutil.rmtree('data')
    os.remove('data.zip')


if __name__ == '__main__':
    automated_data_pipeline()
