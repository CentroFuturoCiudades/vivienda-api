import argparse
import os
import sqlite3

import geopandas as gpd
import pandas as pd

TYPE_MAPPING = {
    'int64': 'INTEGER',
    'float64': 'REAL',
    'object': 'TEXT',
}


def get_args():
  parser = argparse.ArgumentParser(description='Join establishments with lots')
  parser.add_argument('lots_file', type=str, help='The file with all the data')
  parser.add_argument('sql_file', type=str, help='The file with all the data')
  parser.add_argument('-v', '--view', action='store_true')
  return parser.parse_args()


if __name__ == '__main__':
  args = get_args()

  gdf = gpd.read_file(args.lots_file, crs='EPSG:4326')
  column_types_mapping = {column: TYPE_MAPPING[gdf[column].dtype.name]
                          for column in gdf.columns if column != 'geometry'}
  desired_columns = list(column_types_mapping.keys())

  create_table_query = f'''CREATE TABLE IF NOT EXISTS predios (
      {', '.join([f'{column} {column_types_mapping[column]}' for column in column_types_mapping])}
  )'''
  if os.path.exists(args.sql_file):
    os.remove(args.sql_file)

  conn = sqlite3.connect(args.sql_file)
  cursor = conn.cursor()

  print(create_table_query)
  cursor.execute(create_table_query)

  insert_query = f'INSERT INTO predios ({", ".join(desired_columns)}) VALUES ({"?, " * (len(desired_columns) - 1)}?)'
  print(insert_query)

  for index, row in gdf.iterrows():
    values = tuple(row[column] for column in desired_columns)
    cursor.execute(insert_query, values)

  conn.commit()
  conn.close()
