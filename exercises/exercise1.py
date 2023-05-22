import subprocess
import pandas as pd
import requests
import sqlite3
import io

DATA_URI = 'https://opendata.rhein-kreis-neuss.de/explore/dataset/rhein-kreis-neuss-flughafen-weltweit/download/?format=csv'
DATABASE_PATH = 'airports.sqlite'
TABLE_NAME = 'airports'


def automated_data_pipeline():

    # download data
    response = requests.get(DATA_URI)
    if response.status_code != 200:
        print('Error downloading data from %s: %s' % (DATA_URI, response.status_code))
        return
    else:
        print('Successfully downloaded data.')

    # convert data to pandas dataframe
    data = io.BytesIO(response.content)
    df = pd.read_csv(data, delimiter=';')
    print('Loaded data into pandas dataframe.')

    # assign fitting built-in SQLite types to all columns
    df = df.drop(columns=['geo_punkt'])  # one fact in one place
    column_types = {
        'column1': 'INTEGER',
        'column2': 'TEXT',
        'column3': 'TEXT',
        'column4': 'TEXT',
        'column5': 'TEXT',
        'column6': 'TEXT',
        'column7': 'REAL',
        'column8': 'REAL',
        'column9': 'INTEGER',
        'column10': 'REAL',
        'column11': 'TEXT',
        'column12': 'TEXT',
    }

    # persist data into database, directly from pandas
    conn = sqlite3.connect(DATABASE_PATH)
    df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False, dtype=column_types)
    conn.close()
    print('Saved data into database.')


if __name__ == '__main__':
    subprocess.Popen(['python3', '-m', 'pip', 'install', 'requests'])
    automated_data_pipeline()
