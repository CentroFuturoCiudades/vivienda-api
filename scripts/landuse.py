import geopandas as gpd
import pandas as pd
import osmnx as ox
from shapely.geometry import box, Polygon, MultiPolygon
import matplotlib.pyplot as plt
from osmnx.distance import nearest_nodes
import numpy as np
from shapely.geometry import Point
from itertools import product
import networkx as nx
import sys
import argparse

from utils.utils import remove_outliers, normalize
from utils.constants import PARKING_FILTER, PARKING_TAGS, PARK_TAGS, EQUIPMENT_TAGS, BUFFER_PARKING, GDFS_MAPPING

# def overlay_multiple(gdf_initial: gpd.GeoDataFrame, gdfs: list[gpd.GeoDataFrame]) -> list[gpd.GeoDataFrame]:
#     results = []
#     # gdf_initial = gdf_initial[gdf_initial.geometry.type.isin(['Polygon', 'MultiPolygon'])][['geometry']].dissolve()
#     gdf_initial = gdf_initial[['geometry']]
#     union_gdf = None
#     for gdf in gdfs:
#         # gdf = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])][['geometry']]#.dissolve()
#         gdf = gdf[['geometry']]
#         gdf = gdf_initial.overlay(gdf, how='intersection')
#         gdf = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]
#         if union_gdf is not None:
#           gdf = gdf.overlay(union_gdf, how='difference')
#           union_gdf = union_gdf.overlay(gdf, how='union')
#         else:
#           union_gdf = gdf
#         union_gdf = union_gdf[union_gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]
#         print(union_gdf.geometry.type.unique())
#         results.append(gdf)
#     gdf_lots = gdf_initial.overlay(union_gdf, how='difference')
#     gdf_lots = gdf_lots[gdf_lots.geometry.type.isin(['Polygon', 'MultiPolygon'])]
#     print(gdf_lots.geometry.type.unique())
#     results.append(gdf_lots)

#     # Reverse the list to maintain the order of overlays
#     return results[::-1]


def overlay_multiple(gdf_initial: gpd.GeoDataFrame, gdfs: list[gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
  previous_gdfs = []
  gdf_lots = gdf_initial.reset_index()
  for gdf in gdfs:
    gdf = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])][['geometry']]
    gdf_lots = gdf_lots.overlay(gdf, how='difference')
    gdf_filtered = gdf[gdf.geometry.type.isin(['Polygon', 'MultiPolygon'])]
    gdf = gdf_initial.reset_index().overlay(gdf_filtered, how='intersection', keep_geom_type=False)
    for previous_gdf in previous_gdfs:
      gdf = gdf.overlay(previous_gdf, how='difference')
    previous_gdfs.append(gdf)
  previous_gdfs.append(gdf_lots)
  return previous_gdfs[::-1]


def get_args():
  parser = argparse.ArgumentParser(description='Join establishments with lots')
  parser.add_argument('lots_file', type=str, help='The file with all the data')
  parser.add_argument('gpkg_file', type=str, help='The file with all the data')
  parser.add_argument('-v', '--view', action='store_true')
  return parser.parse_args()


if __name__ == '__main__':
  args = get_args()

  gdf_bounds = gpd.read_file(args.gpkg_file, layer='bounds', crs='EPSG:4326').unary_union
  gdf_lots = gpd.read_file(args.lots_file, layer='establishments', crs='EPSG:4326')
  gdf_lots['lot_area'] = gdf_lots.to_crs('EPSG:6933').area / 10_000

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

  gdf_buildings = gpd.read_file(args.gpkg_file, layer='buildings', crs='EPSG:4326')
  gdf_vegetation = gpd.read_file(args.gpkg_file, layer='vegetation', crs='EPSG:4326').reset_index(drop=True)

  gdfs = overlay_multiple(gdf_lots, [gdf_buildings, gdf_equipment, gdf_parks, gdf_parking, gdf_vegetation])
  if args.view:
    fig, ax = plt.subplots()
    for gdf, item in zip(gdfs, GDFS_MAPPING):
      gdf.plot(ax=ax, color=item['color'], alpha=0.5)
    plt.show()
  for gdf, item in zip(gdfs, GDFS_MAPPING):
    gdf = gdf.set_index('ID').dissolve(by='ID')
    column_area = f'{item["name"]}_area'
    column_ratio = f'{item["name"]}_ratio'
    gdf[column_area] = gdf.to_crs('EPSG:6933').area / 10_000
    gdf[column_ratio] = gdf[column_area] / gdf['lot_area']
    gdf = gdf.reset_index()[['ID', column_area, column_ratio, 'geometry']]
    gdf.to_file(args.lots_file, layer=item['name'], driver='GPKG')
    gdf_lots = gdf_lots.merge(gdf.drop(columns='geometry'), on='ID', how='left')
    gdf_lots[[column_area, column_ratio]] = gdf_lots[[column_area, column_ratio]].fillna(0)
  print(gdf_lots)

  gdf_lots.to_file(args.lots_file, layer='landuse', driver='GPKG')
