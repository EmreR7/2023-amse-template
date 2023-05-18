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
c2.execute('''CREATE TABLE gas_prices (date datetime, station_uuid text, diesel float, e5 float, e10 float, dieselchange int, e5change int, e10change int)''')

df_gas_prices = pd.concat(map(pd.read_csv, glob.glob(os.path.join('', "data/06/*.csv"))))
gas_stations = ['9bd5320e-aea5-497d-a58f-e22b0c9a8990', 'ebfed486-9666-44ec-9dc4-ec474a9d5c1c', '1c797890-605a-47d4-b6e5-5c3f9d6adefe',
                '75ba1407-d856-447f-bfc7-ec82e9cd9d43', '64c43b30-4e90-4eae-aab5-44358c1ce7e5']
df_reduced_stations_gas_prices = df_gas_prices[df_gas_prices['station_uuid'].isin(gas_stations)]
df_reduced_stations_gas_prices.sort_values(by='date', inplace=True)
df_reduced_stations_gas_prices.to_sql('gas_prices', conn2, if_exists='replace', index=False)
