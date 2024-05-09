import osmnx as ox
import pandas as pd
import geopandas as gpd
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt
import argparse
import pandana as pdna
import os
import pickle

from utils.utils import normalize
from utils.constants import WALK_RADIUS, WALK_SPEED, MAX_ESTABLISHMENTS, PROXIMITY_MAPPING, ACCESIBILITY_MAPPING, AMENITIES_MAPPING


def get_proximity(network, categories_mapping, distance, walk_speed):
  results = []
  for category, num_pois in categories_mapping.items():
    proximity = network.nearest_pois(distance=distance, category=category, num_pois=num_pois, include_poi_ids=False)
    # Calculate time in minutes for the farthest POI in list
    proximity['minutes_' + category] = proximity[num_pois] / (walk_speed * 60)
    # Select only the relevant column and rename it
    proximity = proximity[['minutes_' + category]]
    # keep the maximum time for each lot
    results.append(proximity)

  # get the maximum time for each category
  final_proximity = pd.concat(results, axis=1)
  final_proximity['minutes'] = final_proximity.max(axis=1)
  return final_proximity


def get_accessibility(network, categories, distance, decay='linear'):
  results = []
  for category in categories:
    accessibility = network.aggregate(distance, type='sum', decay=decay, name=category)
    results.append(accessibility)
  final_accessibility = pd.concat(results, axis=1)
  final_accessibility['accessibility'] = final_accessibility.sum(axis=1)
  return final_accessibility[['accessibility']]


def load_network(filename, gdf_bounds, radius):
  if os.path.exists(filename):
    network = pdna.Network.from_hdf5(filename)
  else:
    extended_boundary = gdf_bounds.to_crs('EPSG:3043').geometry.buffer(radius).to_crs('EPSG:4326').unary_union
    G = ox.graph_from_polygon(extended_boundary, network_type='walk')
    nodes, edges = ox.graph_to_gdfs(G)
    edges = edges.reset_index()
    network = pdna.Network(nodes['x'], nodes['y'], edges['u'], edges['v'], edges[['length']])
    network.save_hdf5(filename)
  return network


def get_all_info(
        network,
        gdf,
        proximity_mapping
):
  for sector in proximity_mapping.keys():
    points = gdf.loc[gdf[sector] > 0]
    network.set_pois(
        category=sector,
        x_col=points.geometry.x,
        y_col=points.geometry.y,
        maxdist=WALK_RADIUS,
        maxitems=MAX_ESTABLISHMENTS
    )

  for sector in ACCESIBILITY_MAPPING:
    points = gdf.loc[gdf[sector] > 0]
    network.set(points['node_ids'], name=sector)

  proximity = get_proximity(network, proximity_mapping, WALK_RADIUS, WALK_SPEED)
  accessibility = get_accessibility(network, ACCESIBILITY_MAPPING, WALK_RADIUS)
  gdf_final = accessibility.merge(proximity, left_index=True, right_index=True, how='left')
  return gdf_final


def get_args():
  parser = argparse.ArgumentParser(description='Join establishments with lots')
  parser.add_argument('lots_file', type=str, help='The file with all the data')
  parser.add_argument('gpkg_file', type=str, help='The file with all the data')
  parser.add_argument('folder', type=str, help='The folder')
  parser.add_argument('-v', '--view', action='store_true')
  return parser.parse_args()


if __name__ == '__main__':
  args = get_args()

  gdf_lots = gpd.read_file(args.lots_file, layer='landuse', crs='EPSG:4326')
  gdf_bounds = gpd.read_file(args.gpkg_file, layer='bounds', crs='EPSG:4326')
  gdf_establishments = gpd.read_file(args.gpkg_file, layer='establishments', crs='EPSG:4326')
  gdf_establishments['codigo_act'] = gdf_establishments['codigo_act'].astype(str)
  gdf_parks = gpd.read_file(args.lots_file, layer='park', crs='EPSG:4326')
  gdf_parks = gdf_parks.loc[gdf_parks['park_ratio'] > 0.2]

  gdfs_mapping = {
      'home': gdf_lots,
      'establishment': gdf_establishments,
      'park': gdf_parks,
  }

  for item in AMENITIES_MAPPING:
    column = 'proximity_' + item['column']
    current_gdf = gdfs_mapping[item['type']]
    current_gdf[column] = current_gdf.query(item['query']).any(axis=1)
    current_gdf[column] = current_gdf[column].fillna(False).astype(int)

  # TODO: Load pedestrian network from folder of project
  pedestrian_network = load_network(f'{args.folder}/pedestrian_network.hd5', gdf_bounds, WALK_RADIUS)
  pedestrian_network.precompute(WALK_RADIUS)

  gdf_aggregate = gpd.GeoDataFrame()
  for gdf in gdfs_mapping.values():
    geometry = gpd.points_from_xy(gdf.geometry.centroid.x, gdf.geometry.centroid.y)
    gdf = gpd.GeoDataFrame(gdf, crs='EPSG:4326', geometry=geometry)
    gdf['node_ids'] = pedestrian_network.get_node_ids(gdf.geometry.x, gdf.geometry.y)
    columns = ['proximity_' + x['column'] for x in AMENITIES_MAPPING if 'proximity_' + x['column'] in gdf.columns]
    gdf = gdf[['node_ids', 'geometry', *columns]]
    gdf_aggregate = pd.concat([gdf_aggregate, gdf], ignore_index=True)
  gdf_aggregate = gdf_aggregate.fillna(0)
  # TODO: Save lots from folder of project
  gdf_aggregate.to_file(f'{args.folder}/lots.gpkg', layer='points_accessibility', driver='GPKG')

  df_accessibility = get_all_info(pedestrian_network, gdf_aggregate, PROXIMITY_MAPPING)
  gdf_lots['node_ids'] = pedestrian_network.get_node_ids(gdf_lots.geometry.centroid.x, gdf_lots.geometry.centroid.y)
  gdf_lots = gdf_lots.merge(df_accessibility, left_on='node_ids', right_index=True, how='left')
  gdf_lots = gdf_lots.drop(columns=['node_ids'])

  if args.view:
    fig, ax = plt.subplots()
    ax.set_axis_off()
    gdf_lots.plot(ax=ax, column='minutes', cmap='Reds_r', legend=True)
    gdf_establishments[gdf_establishments['proximity_salud'] > 0].plot(ax=ax, color='cyan', markersize=6)
    gdf_establishments[gdf_establishments['proximity_educacion'] > 0].plot(ax=ax, color='purple', markersize=6)
    gdf_establishments[gdf_establishments['proximity_servicios'] > 0].plot(ax=ax, color='green', markersize=6)
    gdf_establishments[gdf_establishments['proximity_supermercado'] > 0].plot(ax=ax, color='red', markersize=6)
    gdf_parks[gdf_parks['park_area'] < 0.5].plot(ax=ax, color='green', alpha=0.5)
    plt.show()
  gdf_lots.to_file(args.lots_file, layer='accessibility', driver='GPKG')
