import pandas as pd
import sqlite3

conn = sqlite3.connect('trainstops.sqlite')
c = conn.cursor()
c.execute('''CREATE TABLE trainstops (EVA_NR INT, DS100 VARCHAR, IFOPT TEXT, NAME TEXT, Verkehr TEXT, Laenge FLOAT, Breite FLOAT, Betreiber_Name TEXT, Betreiber_Nr INT)''')
datatypes = {
            "EVA_NR": int,
            "DS100": str,
            "IFOPT": str,
            "NAME": str,
            "Verkehr": str,
            "Laenge": float,
            "Breite": float,
            "Betreiber_Name": str,
            "Betreiber_Nr": int
        }
df = pd.read_csv('https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV', sep=';', decimal=',', usecols=['EVA_NR', 'DS100', 'IFOPT', 'NAME', 'Verkehr', 'Laenge', 'Breite', 'Betreiber_Name', 'Betreiber_Nr'])
valid_verkehr_values = ['FV', 'RV', 'nur DPN']
df = df.dropna(inplace=False)
df = df.astype(datatypes)
df = df[df['Verkehr'].isin(valid_verkehr_values)]
df = df[(df['Laenge'] >= -90.0) & (df['Laenge'] <= 90.0)]
df = df[(df['Breite'] >= -90.0) & (df['Breite'] <= 90.0)]
regex = r"^[a-zA-Z]{2}:\d+:\d+(:\d+)?$"
df = df[df['IFOPT'].str.contains(regex)]
df.to_sql('trainstops', conn, if_exists='replace', index=False) 