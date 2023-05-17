import sqlite3
import pandas as pd
import glob
import os

conn = sqlite3.connect('data/traffic_count.sqlite')
c = conn.cursor()
c.execute('''CREATE TABLE traffic_count (tknr int, state int, roadclass text, date int, weekday int, hour int, traffic_count_d1 int, traffic_count_d2 int)''')

df_traffic_count = pd.read_csv('data/zst9952_2021.csv', sep=';', usecols=['TKNR', 'Zst', 'Land', 'Strklas', 'Strnum', 'Datum', 'Wotag', 'Stunde', 'KFZ_R1', 'KFZ_R2'])
df_traffic_count.to_sql('traffic_count', conn, if_exists='replace', index=False)

conn2 = sqlite3.connect('data/gas_prices.sqlite')
c2 = conn.cursor()
c2.execute('''CREATE TABLE gas_prices (date text, station_uuid text, diesel real, e5 real, e10 real, dieselchange int, e5change int, e10change int)''')

path = '/data/06'
all_files = glob.glob(os.path.join(path, '/*.csv'))
li = []
for filename in all_files:
    df = pd.read_csv(filename, index_col=None, header=0)
    li.append(df)

df_gas_prices = pd.concat(li, axis=0, ignore_index=True)
df_gas_prices.to_sql('gas_prices', conn2, if_exists='replace', index=False)
