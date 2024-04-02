import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from utils.utils import fit_to_boundaries, fill, SECTORS_MAP
from shapely.wkt import loads
from scripts.raster_to_geojson import to_gdf
import rioxarray
import re
import sys
import requests
import zipfile
from io import BytesIO
import shutil
import ee
import argparse

regex_ppl = r'([0-9]+) a ([0-9]+) personas'
BUFFER_DISTANCE = 1609.34
BUILDING_CONFIDENCE = 0.65
URL_MZA_2010 = 'https://www.inegi.org.mx/contenidos/programas/ccpv/2010/datosabiertos/ageb_y_manzana/resageburb_{0}_2010_csv.zip'
URL_MZA_2020 = 'https://www.inegi.org.mx/contenidos/programas/ccpv/2020/datosabiertos/ageb_manzana/ageb_mza_urbana_{0}_cpv2020_csv.zip'
CSV_PATH_MZA_2010 = 'temp_data/resultados_ageb_urbana_{0}_cpv2010/conjunto_de_datos/resultados_ageb_urbana_{0}_cpv2010.csv'
CSV_PATH_MZA_2020 = 'temp_data/ageb_mza_urbana_{0}_cpv2020/conjunto_de_datos/conjunto_de_datos_ageb_urbana_{0}_cpv2020.csv'
STATE_CODE = 25

ee.Initialize()


def gdf_to_ee_polygon(gdf: gpd.GeoDataFrame):
  polygon_geojson = gdf.geometry.iloc[0].__geo_interface__
  coords = polygon_geojson['coordinates'][0][0]
  ee_polygon = ee.Geometry.Polygon(coords)
  return ee_polygon


def map_sector_to_sector(codigo_act: int) -> str:
  for sector in SECTORS_MAP:
    for low, high in sector['range']:
      if low <= codigo_act <= high:
        return sector['sector']


def process_green_area(gdf_bounds: gpd.GeoDataFrame):
  # Get the medium and high vegetation
  ee_polygon = gdf_to_ee_polygon(gdf_bounds)
  image = ee.Image("JRC/GHSL/P2023A/GHS_BUILT_C/2018")
  image = image.clip(ee_polygon)
  image = image.eq(2).Or(image.eq(3))
  download_url = image.getDownloadURL({
      'scale': 10,
      'region': image.geometry().getInfo(),
      'format': 'GeoTIFF',
      'crs': 'EPSG:4326'
  })
  response = requests.get(download_url)
  with open(f'{INPUT_FOLDER}/builtup.tif', 'wb') as f:
    f.write(response.content)
  raster = rioxarray.open_rasterio(f'{INPUT_FOLDER}/builtup.tif')
  gdf_builtup = to_gdf(raster).dissolve()
  gdf_builtup.to_file(f'{INPUT_FOLDER}/builtup.geojson', driver='GeoJSON')


def clean_denue(gdf_bounds: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
  df_denue = pd.read_csv('data/Primavera/denue_inegi_25_.csv', encoding='latin-1')
  geometry_denue = gpd.points_from_xy(df_denue.longitud, df_denue.latitud)
  gdf_denue = gpd.GeoDataFrame(df_denue, geometry=geometry_denue, crs='EPSG:4326')
  print(df_denue)
  _gdf_bounds = gdf_bounds.to_crs('EPSG:32614').buffer(BUFFER_DISTANCE).to_crs('EPSG:4326')
  gdf_denue = fit_to_boundaries(gdf_denue, _gdf_bounds.unary_union)
  gdf_denue = fill(gdf_denue, ['cve_ent', 'cve_mun', 'cve_loc', 'ageb', 'manzana'])
  gdf_denue['per_ocu'] = gdf_denue['per_ocu'].map(lambda x: re.match(regex_ppl, x.strip()))
  gdf_denue['per_ocu'] = gdf_denue['per_ocu'].map(lambda x: (int(x.groups()[0]) + int(x.groups()[1])) / 2 if x else x)
  gdf_denue = gdf_denue.dropna(subset=['per_ocu'])
  gdf_denue['sector'] = gdf_denue['codigo_act'].apply(lambda x: str(x)[:2]).astype(int)
  gdf_denue['sector'] = gdf_denue['sector'].apply(map_sector_to_sector)
  gdf_denue = gdf_denue.rename(columns={'per_ocu': 'num_workers'})
  return gdf_denue


def process_blocks(gdf_bounds: gpd.GeoDataFrame):
  gdf_predios = gpd.read_file(f'{INPUT_FOLDER}/predios.geojson').to_crs(epsg=4326)
  gdf_predios = gpd.sjoin(gdf_predios, gdf_bounds, predicate='within')

  response = requests.get(URL_MZA_2020.format(STATE_CODE))
  zip_content = BytesIO(response.content)
  with zipfile.ZipFile(zip_content) as zip_ref:
    zip_ref.extractall("temp_data")
  df = pd.read_csv(CSV_PATH_MZA_2020.format(STATE_CODE))
  shutil.rmtree('temp_data')
  df['CVEGEO'] = df['ENTIDAD'].astype(str).str.zfill(2) + \
      df['MUN'].astype(str).str.zfill(3) + \
      df['LOC'].astype(str).str.zfill(4) + \
      df['AGEB'].astype(str).str.zfill(4) + \
      df['MZA'].astype(str).str.zfill(3)
  gdf_predios = gdf_predios.merge(df, on='CVEGEO', how='inner')
  gdf_predios = gdf_predios.rename(columns={'CVEGEO': 'CVE_GEO'})
  gdf_predios['ID'] = gdf_predios['CVE_GEO']
  # gdf_predios[['CVEGEO', 'geometry']].to_file(f'{FOLDER}/blocks.geojson', driver='GeoJSON')
  gdf_predios.drop(columns=['index_right']).to_file(f'{INPUT_FOLDER}/blocks.geojson', driver='GeoJSON')


def process_buildings(gdf_bounds: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
  ee_polygon = gdf_to_ee_polygon(gdf_bounds)
  image_collection = ee.FeatureCollection('GOOGLE/Research/open-buildings/v3/polygons') \
      .filterBounds(ee_polygon) \
      .filter(f'confidence >= {BUILDING_CONFIDENCE}')
  download_url = image_collection.getDownloadURL(filetype='geojson', filename='open_buildings')
  response = requests.get(download_url)
  with open(f'{INPUT_FOLDER}/buildings.geojson', 'wb') as f:
    f.write(response.content)


def get_args():
  parser = argparse.ArgumentParser(description='Join establishments with lots')
  parser.add_argument('input_folder', type=str, help='The folder with the input data')
  parser.add_argument('output_folder', type=str, help='The folder to save the output data')
  parser.add_argument('column_id', type=str, help='The column to use as the identifier for the lots')
  return parser.parse_args()


if __name__ == '__main__':
  args = get_args()
  INPUT_FOLDER = args.input_folder
  OUTPUT_FOLDER = args.output_folder
  COLUMN_ID = args.column_id
  gdf_bounds = gpd.read_file(f'{INPUT_FOLDER}/poligono.geojson', crs='EPSG:4326')

  process_blocks(gdf_bounds)
  # gdf_bounds = gpd.read_file(f'{FOLDER}/blocks.geojson', crs='EPSG:4326')
  process_green_area(gdf_bounds)

  gdf_denue = clean_denue(gdf_bounds)
  gdf_denue.to_file(f'{INPUT_FOLDER}/denue.geojson', driver='GeoJSON')

  process_buildings(gdf_bounds)
