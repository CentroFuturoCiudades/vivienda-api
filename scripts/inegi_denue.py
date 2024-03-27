import geopandas as gpd
import pandas as pd
from utils.utils import map_sector_to_sector, join
import matplotlib.pyplot as plt
import sys

OUTPUT_FOLDER = sys.argv[1]
BUFFER_PARKING = 0.00002
FOLDER_FINAL = sys.argv[2]

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
  establishment_counts = gdf_temp.groupby(['CVE_GEO', 'CLAVE_LOTE']).agg({
      'num_establishments': 'count',
      'num_workers': 'sum',
      'sector': lambda x: dict(x.value_counts())
  })
  gdf_lots = gdf_block_lots.merge(establishment_counts, on=['CVE_GEO', 'CLAVE_LOTE'], how='left')
  return gdf_lots


def assign_by_proximity(gdf_block_lots: gpd.GeoDataFrame, gdf_denue: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
  _gdf_denue = gdf_denue.to_crs('EPSG:32614')
  _gdf_block_lots = gdf_block_lots.to_crs('EPSG:32614')
  _gdf_denue['num_establishments'] = 1
  new_gdf = gpd.sjoin_nearest(_gdf_denue, _gdf_block_lots, how='left', max_distance=10)
  final = new_gdf.groupby('CLAVE_LOTE').agg({
      'num_establishments': 'count',
      'num_workers': 'sum',
      'sector': lambda x: dict(x.value_counts())
  })
  final = _gdf_block_lots.merge(final, on='CLAVE_LOTE', how='left')
#   fig, ax = plt.subplots()
#   _gdf_block_lots.plot(ax=ax, color='gray')
#   final.plot(ax=ax, column='num_establishments')
#   _gdf_denue.plot(ax=ax, color='red', markersize=1)
#   plt.show()
  return final.to_crs('EPSG:4326')


if __name__ == '__main__':
  # Load the polygons for the lots
  gdf_predios_dt = gpd.read_file(f'{OUTPUT_FOLDER}/predios.geojson').to_crs('EPSG:4326')
  gdf_predios_dt = gdf_predios_dt.dissolve(by='CLAVE_LOTE').reset_index()
  gdf_predios_dt
  print(gdf_predios_dt)
  gdf_predios_dt['ALTURA'] = gdf_predios_dt['ALTURA'].apply(lambda x: x.split(
      ' ')[0] if x and str(x).find(' ') > 0 else x).fillna(1).astype(float)
  gdf_predios_dt['predio_area'] = gdf_predios_dt['geometry'].to_crs('EPSG:6933').area
  gdf_predios_dt = gdf_predios_dt[gdf_predios_dt['predio_area'] > 0]

  # Load the polygons for the blocks
  BLOCK_NUMERIC_COLUMNS = [
      'VIVTOT',
      'TVIVHAB',
      'VIVPAR_DES',
      'VPH_AUTOM',
      'POBTOT',
  ]
  gdf_blocks = gpd.read_file(f'{OUTPUT_FOLDER}/blocks.geojson').set_index('CVE_GEO')
  gdf_blocks['block_area'] = gdf_blocks['geometry'].to_crs('EPSG:6933').area
  gdf_blocks[BLOCK_NUMERIC_COLUMNS] = gdf_blocks[BLOCK_NUMERIC_COLUMNS].applymap(
      lambda x: 0 if x == '*' or x == 'N/D' else x).astype(float)

  # Load the coordinates for the establishments
  gdf_denue = gpd.read_file(f'{OUTPUT_FOLDER}/denue.geojson', crs='EPSG:4326')
  gdf_denue = gdf_denue.rename(columns={'id': 'DENUE_ID'})

  # Get which sector each establishment belongs to
  gdf_denue['sector'] = gdf_denue['codigo_act'].apply(lambda x: str(x)[:2]).astype(int)
  gdf_denue['sector'] = gdf_denue['sector'].apply(map_sector_to_sector)
  gdf_denue['date'] = pd.to_datetime(gdf_denue['fecha_alta'], format='%Y-%m')
  gdf_denue['year'] = gdf_denue['date'].dt.year

  # Join demographic information with the lots
  gdf_block_lots = join(gdf_blocks, gdf_predios_dt, {
      'block_area': 'first',
      'CVEGEO': 'first',
      **{column: 'first' for column in BLOCK_NUMERIC_COLUMNS}
  }).rename(columns={'CVEGEO': 'CVE_GEO'})
  # TODO: Find an algorithm to accuratetly distribute demographic information for each lot
  gdf_block_lots['block_percentage'] = gdf_block_lots.apply(
      lambda row: row['predio_area'] /
      row['block_area'] if row['block_area'] > row['predio_area'] else 1,
      axis=1)
  gdf_lots = assign_by_proximity(gdf_block_lots, gdf_denue)
  gdf_lots.reset_index()[['CLAVE_LOTE', 'geometry']].to_file(f'{FOLDER_FINAL}/lots.geojson', driver='GeoJSON')
  gdf_lots.reset_index().drop(columns=['geometry']).to_csv(f"{FOLDER_FINAL}/predios.csv", index=False)
