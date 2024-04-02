import geopandas as gpd
import pandas as pd
from utils.utils import map_sector_to_sector, join, COLUMN_ID
import matplotlib.pyplot as plt
import sys

OUTPUT_FOLDER = sys.argv[1]
BUFFER_PARKING = 0.00002
FOLDER_FINAL = sys.argv[2]

if __name__ == '__main__':
  # Load the polygons for the lots
  gdf_predios_dt = gpd.read_file(f'{OUTPUT_FOLDER}/predios.geojson').to_crs('EPSG:4326')
  gdf_predios_dt = gdf_predios_dt.dissolve(by=COLUMN_ID).reset_index()
  if 'ALTURA' in gdf_predios_dt.columns:
    gdf_predios_dt['ALTURA'] = gdf_predios_dt['ALTURA'].apply(lambda x: x.split(
        ' ')[0] if x and str(x).find(' ') > 0 else x).fillna(1).astype(float)
  else:
    gdf_predios_dt['ALTURA'] = 1
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

  # Join demographic information with the lots
  gdf_block_lots = join(gdf_blocks, gdf_predios_dt, {
      'block_area': 'first',
      'CVE_GEO': 'first',
      **{column: 'first' for column in BLOCK_NUMERIC_COLUMNS}
  }).rename(columns={'CVE_GEO': 'CVE_GEO'})
  # TODO: Find an algorithm to accuratetly distribute demographic information for each lot
  gdf_block_lots['block_percentage'] = gdf_block_lots.apply(
      lambda row: row['predio_area'] /
      row['block_area'] if row['block_area'] > row['predio_area'] else 1,
      axis=1)