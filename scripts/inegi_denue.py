import geopandas as gpd
import pandas as pd
from utils.utils import map_sector_to_sector, join, COLUMN_ID
import matplotlib.pyplot as plt
import sys
import argparse

BUFFER_PARKING = 0.00002

# Join the establishments with the lots by their proximity in each block
# TODO: Ensure establishments are distributed to the correct lots
# TODO: Explore using buffers to join establishments with lots
# TODO: Find a faster and more elegant way to join establishments considering their block


def assign_by_buffer(gdf_block_lots: gpd.GeoDataFrame, gdf_denue: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
  _gdf_denue = gdf_denue.to_crs('EPSG:32614')
  _gdf_block_lots = gdf_block_lots.to_crs('EPSG:32614')
  _gdf_block_lots['geometry'] = _gdf_block_lots.buffer(10)
  gdf_temp = gpd.GeoDataFrame(columns=gdf_denue.columns)
  for block_id in gdf_block_lots['CVE_GEO'].unique():
    establishments_in_block = _gdf_denue.loc[_gdf_denue['CVE_GEO'] == block_id]
    lots_in_block2 = gdf_block_lots.loc[gdf_block_lots['CVE_GEO'] == block_id]
    lots_in_block = _gdf_block_lots.loc[_gdf_block_lots['CVE_GEO'] == block_id]
    if not establishments_in_block.empty:
      fig, ax = plt.subplots()
      lots_in_block.plot(ax=ax, color='blue')
      lots_in_block2.to_crs('EPSG:32614').plot(ax=ax, color='green')
      establishments_in_block.plot(ax=ax, color='red')
      plt.show()
      print(f'Block {block_id} has {len(establishments_in_block)} establishments')
      joined_in_block = gpd.sjoin(establishments_in_block, lots_in_block, how="left")
      joined_in_block = joined_in_block.drop(columns=['CVE_GEO_left', 'index_right'])
      joined_in_block = joined_in_block.rename(columns={'CVE_GEO_right': 'CVE_GEO'})
      gdf_temp = pd.concat([gdf_temp, joined_in_block])
  gdf_temp = gdf_temp.reset_index(drop=True)
  establishment_counts = gdf_temp.groupby(['CVE_GEO', COLUMN_ID]).agg({
      'num_establishments': 'count',
      'num_workers': 'sum',
      'sector': lambda x: dict(x.value_counts())
  })
  gdf_lots = gdf_block_lots.merge(establishment_counts, on=['CVE_GEO', COLUMN_ID], how='left')
  return gdf_lots


def assign_by_proximity(gdf_block_lots: gpd.GeoDataFrame, gdf_denue: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
  _gdf_denue = gdf_denue.to_crs('EPSG:32614')
  _gdf_block_lots = gdf_block_lots.to_crs('EPSG:32614')
  _gdf_denue['num_establishments'] = 1
  new_gdf = gpd.sjoin_nearest(_gdf_denue, _gdf_block_lots, how='left', max_distance=10)
  print(new_gdf.columns)
  final = new_gdf.groupby(COLUMN_ID).agg({
      'num_establishments': 'count',
      'num_workers': 'sum',
      'sector': lambda x: dict(x.value_counts())
  })
  final = _gdf_block_lots.merge(final, on=COLUMN_ID, how='left')
#   fig, ax = plt.subplots()
#   _gdf_block_lots.plot(ax=ax, color='gray')
#   final.plot(ax=ax, column='num_establishments')
#   _gdf_denue.plot(ax=ax, color='red', markersize=1)
#   plt.show()
  return final.to_crs('EPSG:4326')


def get_args():
  parser = argparse.ArgumentParser(description='Join establishments with lots')
  parser.add_argument('input_folder', type=str, help='The folder containing the input data')
  parser.add_argument('output_folder', type=str, help='The folder to save the output data')
  parser.add_argument('column_id', type=str, help='The column to use as the identifier for the lots')
  return parser.parse_args()


if __name__ == '__main__':
  parser = get_args()
  INPUT_FOLDER = parser.input_folder
  OUTPUT_FOLDER = parser.output_folder
  COLUMN_ID = parser.column_id

  # Load the polygons for the lots
  gdf_lots = gpd.read_file(f'{INPUT_FOLDER}/blocks.geojson').to_crs('EPSG:4326')
  gdf_lots['area'] = gdf_lots.to_crs('EPSG:6933').area

  columns = ['VIVTOT', 'TVIVHAB', 'VIVPAR_DES', 'VPH_AUTOM', 'POBTOT']
  gdf_lots[columns] = gdf_lots[columns].apply(pd.to_numeric, errors='coerce').fillna(0)

  # Load the coordinates for the establishments
  gdf_denue = gpd.read_file(f'{INPUT_FOLDER}/denue.geojson', crs='EPSG:4326')
  gdf_denue = gdf_denue.rename(columns={'id': 'DENUE_ID'})
  # Get which sector each establishment belongs to
  gdf_denue['sector'] = gdf_denue['codigo_act'].apply(lambda x: str(x)[:2]).astype(int)
  gdf_denue['sector'] = gdf_denue['sector'].apply(map_sector_to_sector)
  # gdf_denue['date'] = pd.to_datetime(gdf_denue['fecha_alta'], format='%Y-%m')
  # gdf_denue['year'] = gdf_denue['date'].dt.year

  gdf_lots = assign_by_proximity(gdf_lots, gdf_denue)
  print(gdf_lots)
  gdf_lots.reset_index()[[COLUMN_ID, 'geometry']].to_file(f'{OUTPUT_FOLDER}/predios.geojson', driver='GeoJSON')
  gdf_lots.reset_index().drop(columns=['geometry']).to_csv(f"{OUTPUT_FOLDER}/predios.csv", index=False)
