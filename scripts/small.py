import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from utils.utils import fit_to_boundaries, fill, SECTORS_MAP
from shapely.wkt import loads
from scripts.raster_to_geojson import to_gdf
import rioxarray
import re
import sys
import argparse

regex_ppl = r'([0-9]+) a ([0-9]+) personas'
BUFFER_DISTANCE = 1609.34


def map_sector_to_sector(codigo_act: int) -> str:
  for sector in SECTORS_MAP:
    for low, high in sector['range']:
      if low <= codigo_act <= high:
        return sector['sector']


def clean_builup(gdf_bounds: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
  raster = rioxarray.open_rasterio('data/builtup.tif')
  gdf_builtup = to_gdf(raster).dissolve()
  gdf_builtup = gdf_bounds.overlay(gdf_builtup, how='difference', keep_geom_type=False)
  return gdf_builtup


def clean_denue(gdf_bounds: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
  df_denue = pd.read_csv('data/inegi/denue.csv', encoding='latin-1')
  geometry_denue = gpd.points_from_xy(df_denue.longitud, df_denue.latitud)
  gdf_denue = gpd.GeoDataFrame(df_denue, geometry=geometry_denue, crs='EPSG:4326')
  _gdf_bounds = gdf_bounds.to_crs('EPSG:32614').buffer(BUFFER_DISTANCE).to_crs('EPSG:4326')
  gdf_denue = fit_to_boundaries(gdf_denue, _gdf_bounds.unary_union)
  gdf_denue = fill(gdf_denue, ['cve_ent', 'cve_mun', 'cve_loc', 'ageb', 'manzana'])
  gdf_denue['per_ocu'] = gdf_denue['per_ocu'].map(lambda x: re.match(regex_ppl, x.strip()))
  gdf_denue['per_ocu'] = gdf_denue['per_ocu'].map(lambda x: (int(x.groups()[0]) + int(x.groups()[1])) / 2 if x else x)
  gdf_denue = gdf_denue.dropna(subset=['per_ocu'])
  gdf_denue['sector'] = gdf_denue['codigo_act'].apply(lambda x: str(x)[:2]).astype(int)
  gdf_denue['date'] = pd.to_datetime(gdf_denue['fecha_alta'], format='%Y-%m')
  gdf_denue['year'] = gdf_denue['date'].dt.year
  gdf_denue['sector'] = gdf_denue['sector'].apply(map_sector_to_sector)
  gdf_denue = gdf_denue.rename(columns={'per_ocu': 'num_workers'})
  return gdf_denue


def clean_blocks(gdf_bounds: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
  gdf_blocks = gpd.read_file('data/inegi/geo_manzanas/19m.shp').to_crs('EPSG:4326')
  gdf_blocks = fit_to_boundaries(gdf_blocks, gdf_bounds.unary_union)
  gdf_blocks = fill(gdf_blocks, ['CVE_ENT', 'CVE_MUN', 'CVE_LOC', 'CVE_AGEB', 'CVE_MZA']).set_index('CVE_GEO')
  gdf_blocks = gpd.GeoDataFrame(gdf_blocks, geometry='geometry', crs='EPSG:4326')
  df_entorno = pd.read_csv('data/inegi/entorno_manzanas.csv')
  df_entorno = fill(df_entorno, ['ENT', 'MUN', 'LOC', 'AGEB', 'MZA']).set_index('CVE_GEO')
  df_entorno = df_entorno.drop(columns=['ENT', 'MUN', 'LOC', 'AGEB', 'MZA'])
  df_pob = pd.read_csv('data/inegi/pob_manzanas.csv')
  df_pob = fill(df_pob, ['ENTIDAD', 'MUN', 'LOC', 'AGEB', 'MZA']).set_index('CVE_GEO')
  df_pob = df_pob.drop(columns=['ENTIDAD', 'MUN', 'LOC', 'AGEB', 'MZA'])
  gdf_blocks = pd.concat([gdf_blocks, df_entorno, df_pob], axis=1, join="inner")
  gdf_blocks = gpd.GeoDataFrame(gdf_blocks, geometry='geometry')
  numeric_columns = ['POBTOT', 'TVIVHAB', 'ADOQ_N', 'C_RPEAT_N',
                     'C_RAUTO_N', 'C_PASOPEAT_N', 'C_CICLOVIA_N', 'C_CICLOCARRIL_N',
                     'C_RAMPA_N', 'C_SEMAFOROPEAT_N', 'C_ESTACIONBICI_N', 'C_ALUM_N',
                     'C_ARBOL_N', 'C_BANQ_N', 'C_PARADATRANS_N', 'TOTVIAL']
  gdf_blocks[numeric_columns] = gdf_blocks[numeric_columns].apply(pd.to_numeric, errors='coerce')
  # gdf_blocks = gdf_blocks.merge(gdf_denue, how='left', left_index=True, right_index=True)
  # gdf_blocks = gpd.GeoDataFrame(gdf_blocks, geometry='geometry', crs='EPSG:4326')
  return gdf_blocks


def small_buildings(gdf_bounds: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
  chunk_size = 50000
  chunks = []
  for chunk in pd.read_csv('data/buildings.csv', chunksize=chunk_size):
    chunk['geometry'] = chunk['geometry'].apply(loads)
    gdf_chunk = gpd.GeoDataFrame(chunk, geometry='geometry', crs='EPSG:4326')
    filtered_chunk = fit_to_boundaries(gdf_chunk, gdf_bounds.unary_union)
    chunks.append(filtered_chunk)
  gdf_buildings = pd.concat(chunks, ignore_index=True)
  gdf_buildings = gdf_buildings.loc[gdf_buildings['confidence'] >= 0.65]
  gdf_buildings = gpd.GeoDataFrame(gdf_buildings, geometry='geometry', crs='EPSG:4326')
  return gdf_buildings


def get_args():
  parser = argparse.ArgumentParser(description='Join establishments with lots')
  parser.add_argument('input_folder', type=str, help='The folder with the input data')
  parser.add_argument('column_id', type=str, help='The column to use as the identifier for the lots')
  return parser.parse_args()


if __name__ == '__main__':
  args = get_args()
  INPUT_FOLDER = args.input_folder
  COLUMN_ID = args.column_id

  gdf_bounds = gpd.read_file(f'{INPUT_FOLDER}/poligono.geojson', crs='EPSG:4326')

  # gdf_builtup = clean_builup(gdf_bounds)
  # gdf_builtup.to_file(f'{INPUT_FOLDER}/builtup.geojson', driver='GeoJSON')

  # gdf_denue = clean_denue(gdf_bounds)
  # gdf_denue.to_file(f'{INPUT_FOLDER}/denue.geojson', driver='GeoJSON')

  gdf_blocks = clean_blocks(gdf_bounds)
  gdf_lots = gpd.read_file(f'{INPUT_FOLDER}/predios.geojson').to_crs('EPSG:4326')
  gdf_lots['CVE_GEO'] = gdf_lots['CVEGEO']
  print(gdf_blocks.columns.to_list())
  print(gdf_lots.columns.to_list())

  # gdf_blocks.to_file(f'{INPUT_FOLDER}/blocks.geojson', driver='GeoJSON')

  # gdf_buildings = small_buildings(gdf_bounds)
  # gdf_buildings.to_file(f'{INPUT_FOLDER}/buildings.geojson', driver='GeoJSON')
