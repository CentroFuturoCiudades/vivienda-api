import osmnx as ox
import pandas as pd
import geopandas as gpd
import networkx as nx
import numpy as np
from utils.utils import normalize
import matplotlib.pyplot as plt
import sys
import argparse

# walk radius of 1 mile
WALK_RADIUS = 1609.34
SECTORS = ['comercio', 'servicios', 'salud', 'educacion']
OUTPUT_FOLDER = sys.argv[1]


def calculate_batch_walking_times(G, sources, targets):
  walking_speed_m_per_s = 1.4  # Average walking speed in meters per second
  # Calculate shortest path lengths in meters for all source-target pairs
  lengths = dict(nx.all_pairs_dijkstra_path_length(G, cutoff=900, weight='length'))  # 900 seconds = 15 minutes

  # Filter lengths for our sources and targets, convert to time in minutes
  walking_times = {}
  for source in sources:
    for target in targets:
      if source in lengths and target in lengths[source]:
        # Convert length to walking time (in minutes)
        walking_times[(source, target)] = lengths[source][target] / walking_speed_m_per_s / 60

  return walking_times


def get_nearby_services(gdf_lots, gdf_denue, radius=WALK_RADIUS, sectors=SECTORS, decay_rate=0.1):
  # Ensure both GeoDataFrames use the same CRS for geographic calculations
  gdf_lots = gdf_lots.to_crs("EPSG:4326")
  gdf_lots = gdf_lots.drop(columns=['sector'])
  gdf_denue = gdf_denue.to_crs("EPSG:4326")
  gdf_denue = gdf_denue.loc[gdf_denue['sector'].isin(sectors)]

  # Generate the pedestrian network graph around the combined area of interest
  unified_area = gdf_lots.unary_union.convex_hull | gdf_denue.unary_union.convex_hull
  G = ox.graph_from_polygon(unified_area, network_type='walk')

  # Find nearest network nodes to lot centroids and amenities
  gdf_lots['nearest_node'] = ox.distance.nearest_nodes(G, gdf_lots.geometry.centroid.x, gdf_lots.geometry.centroid.y)
  gdf_denue['nearest_node'] = ox.distance.nearest_nodes(G, gdf_denue.geometry.x, gdf_denue.geometry.y)

  # Get unique source and target nodes for batch processing
  source_nodes = gdf_lots['nearest_node'].unique()
  target_nodes = gdf_denue['nearest_node'].unique()

  # Calculate batch walking times between sources and targets
  walking_times = calculate_batch_walking_times(G, source_nodes, target_nodes)

  # Convert walking_times to DataFrame for easier processing
  times_df = pd.DataFrame([(s, t, time) for (s, t), time in walking_times.items()],
                          columns=['source', 'target', 'time'])

  # Merge walking times back to amenities based on 'nearest_node'
  gdf_denue_times = pd.merge(gdf_denue, times_df, left_on='nearest_node', right_on='target')
  # Filter amenities within walking radius (15 minutes)
  gdf_denue_within_radius = gdf_denue_times[gdf_denue_times['time'] <= radius / 60]
  # Calculate proximity scores
  gdf_denue_within_radius['proximity_score'] = np.exp(-decay_rate * gdf_denue_within_radius['time'])

  sector_weight_mapping = {
      'comercio': 0.1,
      'servicios': 0.2,
      'salud': 0.4,
      'educacion': 0.3
  }
  gdf_denue_within_radius = gdf_denue_within_radius.groupby('source').agg({
      'proximity_score': 'sum',
      'time': 'mean',
      'sector': lambda x: dict(x.value_counts())
  }).reset_index()
  test = pd.merge(
      gdf_lots,
      gdf_denue_within_radius,
      left_on='nearest_node',
      right_on='source',
      how='left')
  test['sector'] = test['sector'].fillna({i: 0 for i in sectors})
  test['services_nearby'] = 0
  for sector in SECTORS:
    # check if type dict
    test[f'adj_{sector}'] = test['sector'].apply(lambda x: x.get(sector, 0) if isinstance(x, dict) else 0)
    test['services_nearby'] += test[f'adj_{sector}']
  test = gpd.GeoDataFrame(test.drop(columns=["sector"]), geometry=gdf_lots.geometry, crs="EPSG:4326")

  sector_weight_mapping = {
      'comercio': 0.1,
      'servicios': 0.2,
      'salud': 0.4,
      'educacion': 0.3
  }
  test['accessibility_score'] = 0
  for sector in SECTORS:
    test['accessibility_score'] += normalize(test[f'adj_{sector}']) * sector_weight_mapping[sector]
  test['accessibility_score'] = normalize(test['accessibility_score'])
  return test.set_index(COLUMN_ID)


def get_args():
  parser = argparse.ArgumentParser(description='Join establishments with lots')
  parser.add_argument('output_folder', type=str, help='The folder to save the output data')
  parser.add_argument('column_id', type=str, help='The column to use as the identifier for the lots')
  return parser.parse_args()


if __name__ == '__main__':
  args = get_args()
  OUTPUT_FOLDER = args.output_folder
  COLUMN_ID = args.column_id

  gdf_lots = gpd.read_file(f'{OUTPUT_FOLDER}/predios.geojson').to_crs('EPSG:4326')
  gdf_lots[COLUMN_ID] = gdf_lots[COLUMN_ID].astype(str)
  df_lots = pd.read_csv(f'{OUTPUT_FOLDER}/predios.csv', dtype={COLUMN_ID: str})
  gdf_denue = gpd.read_file(f'{OUTPUT_FOLDER}/denue.geojson').to_crs('EPSG:4326')
  gdf_lots = pd.merge(gdf_lots, df_lots, on=COLUMN_ID, how='left')

  gdf_lots = get_nearby_services(gdf_lots, gdf_denue)

  fig, ax = plt.subplots(ncols=5)
  gdf_lots.plot(ax=ax[0], column='accessibility_score')
  gdf_lots.plot(ax=ax[1], column='adj_comercio')
  gdf_lots.plot(ax=ax[2], column='comercio')
  gdf_lots.plot(ax=ax[3], column='adj_servicios')
  gdf_lots.plot(ax=ax[4], column='servicios')
  plt.show()

  gdf_lots.reset_index()[[COLUMN_ID, 'geometry']].to_file(f'{OUTPUT_FOLDER}/predios.geojson', driver='GeoJSON')
  gdf_lots.reset_index().drop(columns=['geometry']).to_csv(f"{OUTPUT_FOLDER}/predios.csv", index=False)
