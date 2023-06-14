import pandas as pd 
import sqlite3 
import pathlib as pl

def interpretAsCsv(data):
    return pd.read_csv(data, sep=',')

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
    etl_pipeline('https://dev.azure.com/tankerkoenig/_git/tankerkoenig-data?path=/prices/2021/06/2021-06-06-prices.csv','./gasprices_test.sqlite')
    assertIsFile('./gasprices_test.sqlite')