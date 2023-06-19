import sys
import pandas as pd
import sqlite3
import io

DATA_URI = 'https://www-genesis.destatis.de/genesis/downloads/00/tables/46251-0021_00.csv'
DATABASE_PATH = 'cars.sqlite'
TABLE_NAME = 'cars'


def automated_data_pipeline():

    # load data, skip metadata and select columns
    cols = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z', 'AA', 'AB', 'AC', 'AD', 'AE', 'AF', 'AG', 'AH', 'AI', 'AJ', 'AK', 'AL', 'AM', 'AN', 'AO', 'AP', 'AQ', 'AR', 'AS', 'AT', 'AU', 'AV', 'AW', 'AX', 'AY', 'AZ', 'BA', 'BB', 'BC', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI', 'BJ', 'BK', 'BL', 'BM', 'BN', 'BO', 'BP', 'BQ', 'BR', 'BS', 'BT', 'BU', 'BV', 'BW', 'BX', 'BY', 'BZ', 'CA', 'CB', 'CC', 'CD', 'CE']
    use_cols = ['A', 'B', 'C', 'M', 'W', 'AG', 'AQ', 'BA', 'BK', 'BU']
    df = pd.read_csv(DATA_URI, encoding='ISO-8859-1', skiprows=6, skipfooter=4, delimiter=';', names=cols, usecols=use_cols)

    # assign column names
    column_names = ['date', 'CIN', 'name', 'petrol', 'diesel', 'gas', 'electro', 'hybrid', 'plugInHybrid', 'others']
    data_types = ['TEXT', 'TEXT', 'TEXT', 'INTEGER' * 7]
    df = df.rename(columns=dict(zip(df.columns, column_names)))

    # validate data
    df = df.dropna()
    df = df[~(df == '-').any(axis=1)]

    regex_cin = r'0?[0-9]{4}'  # zero can be there four digits must be there
    df['CIN'] = df['CIN'].astype(str)
    valid_cin = df['CIN'].str.match(regex_cin)
    df = df[valid_cin]

    exclude_columns = ['date', 'CIN', 'name']
    columns_to_check = df.drop(columns=exclude_columns).astype(int)
    valid_others = (columns_to_check > 0).all(axis=1)
    df = df[valid_others]

    # persist data
    column_types = dict(zip(column_names, data_types))
    conn = sqlite3.connect(DATABASE_PATH)
    df.to_sql(TABLE_NAME, conn, if_exists='replace', index=False, dtype=column_types)
    conn.close()
    print('Saved data into database.')


if __name__ == '__main__':
    automated_data_pipeline()
