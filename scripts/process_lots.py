import geopandas as gpd
import pandas as pd
import requests
import zipfile
from io import BytesIO
import shutil
import argparse
import matplotlib.pyplot as plt

from utils.constants import URL_MZA_2020, CSV_PATH_MZA_2020, KEEP_COLUMNS


def gather_data(state_code: int) -> pd.DataFrame:
  response = requests.get(URL_MZA_2020.format(state_code))
  zip_content = BytesIO(response.content)
  with zipfile.ZipFile(zip_content) as zip_ref:
    zip_ref.extractall("temp_data")
  df = pd.read_csv(CSV_PATH_MZA_2020.format(state_code))
  shutil.rmtree('temp_data')
  df['CVEGEO'] = df['ENTIDAD'].astype(str).str.zfill(2) + \
      df['MUN'].astype(str).str.zfill(3) + \
      df['LOC'].astype(str).str.zfill(4) + \
      df['AGEB'].astype(str).str.zfill(4) + \
      df['MZA'].astype(str).str.zfill(3)
  return df


def process_blocks(
        gdf_bounds: gpd.GeoDataFrame,
        gdf_blocks: gpd.GeoDataFrame,
        gdf_lots: gpd.GeoDataFrame,
        state_code: int) -> gpd.GeoDataFrame:
  # gdf_lots = gdf_lots.dropna(subset=['CLAVE_LOTE'])
  gdf_blocks = gdf_blocks.drop_duplicates(subset='CVEGEO')
  gdf_blocks = gdf_blocks[['CVEGEO', 'geometry']]
  gdf_blocks['block_area'] = gdf_blocks.to_crs('EPSG:6933').area / 10_000
  gdf_blocks = gpd.sjoin(gdf_blocks, gdf_bounds, predicate='intersects').drop(columns=['index_right'])
  gdf_lots = gpd.sjoin(gdf_lots, gdf_blocks, how='left', predicate='intersects')

  df = gather_data(state_code)
  df = df[['CVEGEO', *KEEP_COLUMNS]].set_index('CVEGEO')
  df = df.apply(pd.to_numeric, errors='coerce').fillna(0)
  gdf_lots = gdf_lots.merge(df, on='CVEGEO', how='inner')
  gdf_lots['P_0A5'] = gdf_lots['P_0A2'] + gdf_lots['P_3A5']
  gdf_lots['P_6A14'] = gdf_lots['POB0_14'] - gdf_lots['P_0A5']
  gdf_lots['P_25A64'] = gdf_lots['POB15_64'] - gdf_lots['P_15A17'] - gdf_lots['P_18A24']
  gdf_lots['P_65MAS'] = gdf_lots['POB65_MAS']
  gdf_lots = gdf_lots.drop(columns=['index_right', 'P_0A2', 'P_3A5', 'POB0_14', 'P_6A11', 'POB15_64', 'POB65_MAS'])
  return gdf_lots


def get_args():
  parser = argparse.ArgumentParser(description='Join establishments with lots')
  parser.add_argument('gpkg_file', type=str, help='The file with all the data')
  parser.add_argument('output_file', type=str, help='The file with the bounds of the area')
  parser.add_argument('state_code', type=int, help='The state code')
  parser.add_argument('-v', '--view', action='store_true')
  return parser.parse_args()


if __name__ == '__main__':
  args = get_args()
  df = gather_data(args.state_code)

  gdf_bounds = gpd.read_file(args.gpkg_file, layer='bounds', crs='EPSG:4326')
  gdf_bounds = gdf_bounds[['geometry']]
  gdf_blocks = gpd.read_file(args.gpkg_file, layer='blocks', crs='EPSG:4326')
  gdf_lots = gpd.read_file(args.gpkg_file, layer='lots', crs='EPSG:4326')

  gdf_lots = process_blocks(gdf_bounds, gdf_blocks, gdf_lots, args.state_code)
  gdf_lots.set_index('ID').to_file(args.output_file, layer='population', driver='GPKG')
  if args.view:
    gdf_lots.plot(column='P_65MAS', legend=True, markersize=1)
    plt.show()