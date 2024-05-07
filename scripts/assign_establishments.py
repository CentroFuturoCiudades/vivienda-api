import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
import argparse
from utils.constants import AMENITIES_MAPPING


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
  establishment_counts = gdf_temp.groupby(['CVE_GEO', 'ID']).agg({
      'num_establishments': 'count',
      'num_workers': 'sum',
      'sector': lambda x: dict(x.value_counts())
  })
  gdf_lots = gdf_block_lots.merge(establishment_counts, on=['CVE_GEO', 'ID'], how='left')
  return gdf_lots


def assign_by_proximity(gdf_block_lots: gpd.GeoDataFrame, gdf_denue: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
  _gdf_denue = gdf_denue.to_crs('EPSG:32614')
  _gdf_block_lots = gdf_block_lots.to_crs('EPSG:32614')
  _gdf_denue['num_establishments'] = 1
  new_gdf = gpd.sjoin_nearest(_gdf_denue, _gdf_block_lots, how='left', max_distance=10)
  sector_columns = [x['column'] for x in AMENITIES_MAPPING if x['type'] == 'establishment']
  gdf_final = new_gdf.groupby('ID').agg({
      'num_establishments': 'count',
      'num_workers': 'sum',
      **{x: 'sum' for x in sector_columns}
  })
  gdf_final = _gdf_block_lots.merge(gdf_final, on='ID', how='left')
  columns = ['num_establishments', 'num_workers', *sector_columns]
  gdf_final[columns] = gdf_final[columns].fillna(0)
  return gdf_final.to_crs('EPSG:4326')


def get_args():
  parser = argparse.ArgumentParser(description='Join establishments with lots')
  parser.add_argument('lots_file', type=str, help='The file with all the data')
  parser.add_argument('gpkg_file', type=str, help='The file with all the data')
  parser.add_argument('-v', '--view', action='store_true')
  return parser.parse_args()


if __name__ == '__main__':
  args = get_args()

  # Load the polygons for the lots
  gdf_lots = gpd.read_file(args.lots_file, layer='population').to_crs('EPSG:4326')
  gdf_lots['area'] = gdf_lots.to_crs('EPSG:6933').area / 10_000

  # Load the coordinates for the establishments
  gdf_denue = gpd.read_file(args.gpkg_file, layer='establishments', crs='EPSG:4326')
  gdf_denue = gdf_denue.rename(columns={'id': 'DENUE_ID'})
  # Get which sector each establishment belongs to
  gdf_denue['codigo_act'] = gdf_denue['codigo_act'].astype(str)
  columns_establishments = [x for x in AMENITIES_MAPPING if x['type'] == 'establishment']
  for item in columns_establishments:
    gdf_denue[item['column']] = gdf_denue.query(item['query']).any(axis=1)
    gdf_denue[item['column']] = gdf_denue[item['column']].fillna(0).astype(int)
  # gdf_denue['date'] = pd.to_datetime(gdf_denue['fecha_alta'], format='%Y-%m')
  # gdf_denue['year'] = gdf_denue['date'].dt.year

  gdf_lots = assign_by_proximity(gdf_lots, gdf_denue)
  gdf_lots.to_file(args.lots_file, layer='establishments', driver='GPKG')

  if args.view:
    gdf_lots.plot(column='supermercado', legend=True)
    plt.show()
