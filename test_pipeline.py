import pandas as pd 
import sqlite3 
import pathlib as pl

def interpretAsCsv(data):
    return pd.read_csv(data)

def transform(data):
    return data.dropna()

def load(data, db_name):
    conn = sqlite3.connect (db_name)
    data.to_sql('example_table', conn, if_exists='replace', index=False)
    conn.close()

def etl_pipeline (data, db_name):
    data = interpretAsCsv(data)
    data = transform(data)
    load(data, db_name)
    
def assertIsFile(path):
        if not pl.Path(path).resolve().is_file():
            raise AssertionError("File does not exist: %s" % str(path))
        
if __name__ == '__main__':
    etl_pipeline('/Users/emre/2023-amse-template/data/06/2021-06-01-prices.csv','/Users/emre/2023-amse-template/data/gasprices_test.sqlite')
    etl_pipeline('/Users/emre/2023-amse-template/data/zst9952_2021.csv', '/Users/emre/2023-amse-template/data/trafficcount_test.sqlite')
    assertIsFile('/Users/emre/2023-amse-template/data/gasprices_test.sqlite')
    assertIsFile('/Users/emre/2023-amse-template/data/trafficcount_test.sqlite')