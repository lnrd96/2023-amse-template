import sys
import pandas as pd
import sqlite3
import io

DATA_URI = 'https://opendata.rhein-kreis-neuss.de/explore/dataset/rhein-kreis-neuss-flughafen-weltweit/download/?format=csv'
DATABASE_PATH = 'airports.sqlite'
TABLE_NAME = 'airports'


def automated_data_pipeline():

    df = pd.read_csv(DATA_URI, delimiter=';')
    print('Loaded data into pandas dataframe.')

    # assign fitting built-in SQLite types to all columns
    # df = df.drop(columns=['geo_punkt'])  # one fact in one place
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
        'geo_punkt': 'TEXT'
    }

    # persist data into database, directly from pandas
    conn = sqlite3.connect(DATABASE_PATH)
    df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False, dtype=column_types)
    conn.close()
    print('Saved data into database.')


if __name__ == '__main__':
    automated_data_pipeline()
