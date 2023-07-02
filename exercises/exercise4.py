import urllib.request
import zipfile
import pandas as pd
import sqlite3

url = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
local_file_path = "data/mowesta-dataset-20221107.zip"
csv_filename = "data.csv"

conn = sqlite3.connect('temperatures.sqlite')
c = conn.cursor()

urllib.request.urlretrieve(url, local_file_path)

# Open the zip file
with zipfile.ZipFile(local_file_path, 'r') as zip_ref:
    # Check if the csv file exists in the zip file
    if csv_filename in zip_ref.namelist():
        # Extract the csv file
        zip_ref.extract(csv_filename, "data/")  # Specify the path to extract the file
        print("CSV file extracted successfully!")
    else:
        print("CSV file not found in the zip file.")

df = pd.read_csv('data/' + csv_filename, sep=';', decimal=',', index_col=False, usecols=['Geraet', 'Hersteller', 'Model', 'Monat', 'Temperatur in °C (DWD)', 'Batterietemperatur in °C', 'Geraet aktiv'])
df = df.dropna(inplace=False)
#df = df.astype(datatypes)
df.rename(columns={'Temperatur in °C (DWD)': 'Temperatur', 'Batterietemperatur in °C': 'Batterietemperatur'}, inplace=True)
df["Temperatur"] = df["Temperatur"] * 9 / 5 + 32
df["Batterietemperatur"] = df["Batterietemperatur"] * 9 / 5 + 32
df = df[df["Geraet"] > 0 & (df["Monat"] > 0)]
df.to_sql('temperatures', conn, if_exists='replace', index=False) 
