import sqlite3
import geopandas as gpd
import pandas as pd
import os

if os.path.exists('data/predios.db'):
  os.remove('data/predios.db')

conn = sqlite3.connect('data/predios.db')
cursor = conn.cursor()

column_types = {
    'CLAVE_LOTE': 'TEXT',
    'VIVTOT': 'INTEGER',
    'TVIVHAB': 'INTEGER',
    'VIVPAR_DES': 'INTEGER',
    'VPH_AUTOM': 'INTEGER',
    'POBTOT': 'INTEGER',
    'building_area': 'REAL',
    'green_area': 'REAL',
    'parking_area': 'REAL',
    'park_area': 'REAL',
    'equipment_area': 'REAL',
    'unused_area': 'REAL',
    'num_workers': 'INTEGER',
    'num_establishments': 'INTEGER',
    'services_nearby': 'INTEGER',
    'comercio': 'INTEGER',
    'servicios': 'INTEGER',
    'salud': 'INTEGER',
    'educacion': 'INTEGER',
    'adj_comercio': 'INTEGER',
    'adj_servicios': 'INTEGER',
    'adj_salud': 'INTEGER',
    'adj_educacion': 'INTEGER',
    'total_score': 'REAL',
    'lot_area': 'REAL',
    'building_ratio': 'REAL',
    'parking_ratio': 'REAL',
    'park_ratio': 'REAL',
    'green_ratio': 'REAL',
    'unused_ratio': 'REAL',
    'equipment_ratio': 'REAL',
    'num_properties': 'INTEGER',
    'wasteful_area': 'REAL',
    'wasteful_ratio': 'REAL',
    'used_area': 'REAL',
    'used_ratio': 'REAL',
    'occupancy': 'INTEGER',
    'utilization_area': 'REAL',
    'occupancy_density': 'REAL',
    'home_density': 'REAL',
    'lot_ratio': 'REAL',
    'utilization_ratio': 'REAL',
    'combined_score': 'REAL',
    'ALTURA': 'REAL',
}

create_table_query = f'''CREATE TABLE IF NOT EXISTS predios (
    {', '.join([f'{column} {column_types[column]}' for column in column_types])}
)'''
print(create_table_query)
cursor.execute(create_table_query)


gdf = pd.read_csv('data/processed/predios.csv')
gdf['CLAVE_LOTE'] = gdf['CLAVE_LOTE'].astype(str)
print(gdf.columns)
desired_columns = [column for column in gdf.columns if column in column_types]
gdf = gdf[desired_columns]
print(gdf)
insert_query = f'INSERT INTO predios ({", ".join(desired_columns)}) VALUES ({"?, " * (len(desired_columns) - 1)}?)'
print(insert_query)

for index, row in gdf.iterrows():
  values = tuple(row[column] for column in desired_columns)
  cursor.execute(insert_query, values)

conn.commit()
conn.close()
