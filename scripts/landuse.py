import geopandas as gpd
import pandas as pd
import osmnx as ox
from shapely.geometry import box, Polygon, MultiPolygon
from utils.utils import normalize, remove_outliers, fill, bbox, boundaries, map_sector_to_sector, join, COLUMN_ID
import matplotlib.pyplot as plt
from osmnx.distance import nearest_nodes
import numpy as np
from shapely.geometry import Point
from itertools import product
import networkx as nx
import sys
import argparse

PARK_TAGS = {
    'leisure': 'park',
    'landuse': 'recreation_ground'
}
EQUIPMENT_TAGS = {
    'amenity': [
        'place_of_worship',
        'school',
        'university'],
    'leisure': [
        'sports_centre',
        'pitch',
        'stadium'],
    'building': ['school'],
    'landuse': ['cemetery']
}
PARKING_FILTER = '["highway"~"service"]'
PARKING_TAGS = {
    'amenity': 'parking'
}
BUFFER_PARKING = 0.00002


def overlay_multiple(gdf_initial: gpd.GeoDataFrame, gdfs: list[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
  previous_gdfs = []
  gdf_lots = gdf_initial.reset_index()
  for gdf in gdfs:
    gdf = gdf.dissolve()
    gdf_lots = gdf_lots.overlay(gdf, how='difference', keep_geom_type=False)
    gdf_filtered = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]
    gdf = gdf_initial.reset_index().overlay(gdf_filtered, how='intersection', keep_geom_type=False)
    for previous_gdf in previous_gdfs:
      gdf = gdf.overlay(previous_gdf, how='difference')
    previous_gdfs.append(gdf)
  previous_gdfs.append(gdf_lots)
  return previous_gdfs[::-1]


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

  gdf_bounds = gpd.read_file(f'{INPUT_FOLDER}/poligono.geojson', crs='EPSG:4326').unary_union

  gdf_lots = gpd.read_file(f'{OUTPUT_FOLDER}/predios.geojson', crs='EPSG:4326')
  gdf_lots['lot_area'] = gdf_lots.to_crs('EPSG:6933').area

  # Load the polygons for parking lots
  G_service_highways = ox.graph_from_polygon(
      gdf_bounds,
      custom_filter=PARKING_FILTER,
      network_type='all',
      retain_all=True
  )
  gdf_service_highways = ox.graph_to_gdfs(G_service_highways, nodes=False).reset_index()
  gdf_parking_amenities = ox.geometries_from_polygon(gdf_bounds, tags=PARKING_TAGS).reset_index()
  gdf_parking_amenities = gdf_parking_amenities[(gdf_parking_amenities['element_type'] != 'node')]
  gdf_parking_amenities['geometry'] = gdf_parking_amenities['geometry'].intersection(gdf_bounds)
  gdf_combined = gpd.GeoDataFrame(
      pd.concat([gdf_service_highways, gdf_parking_amenities], ignore_index=True), crs='EPSG:4326')
  unified_geometry = gdf_combined.dissolve().buffer(BUFFER_PARKING).unary_union
  external_polygons = [Polygon(poly.exterior) for poly in unified_geometry.geoms]
  gdf_parking = gpd.GeoDataFrame(geometry=external_polygons, crs='EPSG:4326')

  # Load the polygons for parks
  gdf_parks = ox.geometries_from_polygon(gdf_bounds, tags=PARK_TAGS).reset_index()
  gdf_parks = gdf_parks[gdf_parks['element_type'] != 'node']
  gdf_parks['geometry'] = gdf_parks['geometry'].intersection(gdf_bounds)

  # Load the polygons for equipment equipments (schools, universities and places of worship)
  gdf_equipment = ox.geometries_from_polygon(gdf_bounds, tags=EQUIPMENT_TAGS).reset_index()
  gdf_equipment = gdf_equipment[gdf_equipment['element_type'] != 'node']
  gdf_equipment['geometry'] = gdf_equipment['geometry'].intersection(gdf_bounds)

  # Load the polygons for the building footprints
  gdf_buildings = gpd.read_file(f'{INPUT_FOLDER}/buildings.geojson', crs='EPSG:4326')

  gdfs_mapping = [
      {"name": 'unused', "color": 'blue'},
      {"name": 'green', "color": 'lightgreen'},
      {"name": 'parking', "color": 'gray'},
      {"name": 'park', "color": 'green'},
      {"name": 'equipment', "color": 'red'},
      {"name": 'building', "color": 'orange'},
  ]

  # Load the polygons for the builtup areas
  gdf_builtup = gpd.read_file(f'{INPUT_FOLDER}/builtup.geojson', crs='EPSG:4326').reset_index(drop=True)

  gdfs = overlay_multiple(
      gdf_lots, [gdf_buildings, gdf_equipment, gdf_parks, gdf_parking, gdf_builtup])
  fig, ax = plt.subplots()
  for gdf, item in zip(gdfs, gdfs_mapping):
    gdf.plot(ax=ax, color=item['color'], alpha=0.5)
  plt.show()
  for gdf, item in zip(gdfs, gdfs_mapping):
    gdf = gdf.set_index(COLUMN_ID).dissolve(by=COLUMN_ID)
    column_area = f'{item["name"]}_area'
    column_ratio = f'{item["name"]}_ratio'
    gdf[column_area] = gdf.to_crs('EPSG:6933').area
    gdf[column_ratio] = gdf[column_area] / gdf['lot_area']
    gdf = gdf.reset_index()[[COLUMN_ID, column_area, column_ratio, 'geometry']]
    print(gdf)
    gdf.to_file(f'{OUTPUT_FOLDER}/{item["name"]}.geojson', driver='GeoJSON')
