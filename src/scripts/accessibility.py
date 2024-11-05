import argparse
import os

import geopandas as gpd
import osmnx as ox
import pandana as pdna
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from src.scripts.utils.constants import (
    AMENITIES_MAPPING,
    MAX_ESTABLISHMENTS,
    WALK_RADIUS,
    WALK_SPEED,
    BOUNDS_FILE,
    AMENITIES_FILE,
    PROCESSED_BLOCKS_FILE,
    ACCESSIBILITY_BLOCKS_FILE,
    ACCESSIBILITY_FILE,
    PEDESTRIAN_NETWORK_FILE,
)
import time
from functools import lru_cache

BETA_GRAVITY = {item['name']: 1 / (item['radius'] / 3) for item in AMENITIES_MAPPING}
NUM_POIS = 5
HAS_RUN_POIS = False


def load_network(filename, gdf_bounds, radius):
    if os.path.exists(filename):
        network = pdna.Network.from_hdf5(filename)
    else:
        extended_boundary = (
            gdf_bounds.to_crs("EPSG:3043")
            .geometry.buffer(radius)
            .to_crs("EPSG:4326")
            .unary_union
        )
        G = ox.graph_from_polygon(extended_boundary, network_type="walk")
        nodes, edges = ox.graph_to_gdfs(G)
        edges = edges.reset_index()
        network = pdna.Network(
            nodes["x"], nodes["y"], edges["u"], edges["v"], edges[["length"]]
        )
        network.save_hdf5(filename)
    return network


def calculate_accessibility(gdf_blocks, gdf_amenities, network, amenities_mapping):
    gdf_aggregate = gpd.GeoDataFrame()
    for item in amenities_mapping:
        sector = item['name']
        to_gdf = gdf_amenities.query(item["query_to"])
        gdf_blocks['population'] = gdf_blocks.eval(item["pob_query"])
        maxdist = item['radius']
        if to_gdf.empty:
            continue
        network.set_pois(
            category=sector,
            x_col=to_gdf.geometry.centroid.x,
            y_col=to_gdf.geometry.centroid.y,
            maxdist=maxdist, 
            maxitems=NUM_POIS
        )
        proximity = network.nearest_pois(
            distance=maxdist,
            category=sector,
            num_pois=NUM_POIS,
            include_poi_ids=True
        )
        proximity = proximity.reset_index()

        proximity = proximity.rename(columns={x: f'distance{x}' for x in range(1, NUM_POIS + 1)})

        proximity = pd.wide_to_long(proximity, stubnames=['distance', 'poi'], i='osmid', j='num_poi', sep='').reset_index()
        proximity = proximity.merge(gdf_blocks[['node_ids', f'population']], left_on='osmid', right_on='node_ids', how='left')
        proximity = proximity.rename(columns={'poi': 'destination_id', 'osmid': 'origin_id', 'num_poi': 'num_amenity'})
        proximity = proximity[~proximity['destination_id'].isnull() & ~proximity['origin_id'].isnull() & ~proximity['num_amenity'].isnull()]
        proximity['amenity'] = sector
        proximity['destination_id'] = proximity['destination_id'].astype(int)
        proximity['origin_id'] = proximity['origin_id'].astype(int)
        proximity['num_amenity'] = proximity['num_amenity'].astype(int)

        attraction_values = to_gdf.eval(item["attraction_query"])
        proximity['attraction'] = attraction_values.reindex(proximity['destination_id']).fillna(0).values

        proximity['gravity'] = 1 / np.exp(BETA_GRAVITY[sector] * proximity['distance'])
        proximity['pob_reach'] = proximity['population'] * proximity['gravity']
        proximity['minutes'] = proximity['distance'] / WALK_SPEED
        
        gdf_aggregate = pd.concat([gdf_aggregate, proximity], ignore_index=True)
    return gdf_aggregate


def calculate_destination_metrics(gdf_aggregate):
    gdf_destinations = gdf_aggregate.groupby('destination_id').agg({'pob_reach': 'sum', 'attraction': 'first'})
    gdf_destinations['opportunities_ratio'] = gdf_destinations.apply(lambda x: x['attraction'] / x['pob_reach'] if x['pob_reach'] > 0 else 0, axis=1)
    return gdf_destinations


def calculate_accessibility_scores(gdf_aggregate, gdf_blocks, gdf_destinations):
    gdf_aggregate['accessibility_score'] = gdf_aggregate.apply(lambda x: gdf_destinations['opportunities_ratio'].loc[x['destination_id']] * x['gravity'] if x['destination_id'] in gdf_destinations['opportunities_ratio'].index else 0, axis=1)
    accessibility_scores = gdf_aggregate.groupby(['origin_id', 'amenity']).agg({'accessibility_score': 'sum', 'minutes': 'min'})
    accessibility_scores = accessibility_scores.groupby('origin_id').agg({'accessibility_score': 'sum', 'minutes': 'max'})
    gdf_blocks = gdf_blocks.merge(accessibility_scores, left_on="node_ids", right_index=True, how="left")
    gdf_blocks['accessibility_score'] = np.log(gdf_blocks['accessibility_score'] + 1) * 12.5
    gdf_blocks['accessibility_score'] = gdf_blocks['accessibility_score'].clip(0, 100) / 100
    # gdf_blocks['accessibility_score'] = (gdf_blocks['accessibility_score'] - gdf_blocks['accessibility_score'].median()) / gdf_blocks['accessibility_score'].std()
    return gdf_blocks


def plot_results(gdf_blocks, gdf_amenities):
    fig, ax = plt.figure(1), plt.subplot()
    ax.set_axis_off()
    gdf_blocks.plot(ax=ax, column='minutes', cmap='Reds', legend=True)

    fig, ax = plt.figure(2), plt.subplot()
    ax.set_axis_off()
    gdf_blocks.plot(ax=ax, column='accessibility_score', scheme='quantiles', k=10, cmap='Reds', legend=True)
    gdf_amenities.plot(ax=ax, column='opportunities_ratio', legend=True, cmap='Blues', scheme='quantiles')

    fig, ax = plt.figure(3), plt.subplot()
    ax.set_axis_off()
    gdf_blocks.plot(ax=ax, column='accessibility_score', scheme='quantiles', k=10, cmap='Reds', legend=True)
    gdf_amenities.plot(ax=ax, column='attraction', legend=True, cmap='Blues', scheme='quantiles')

    fig, ax = plt.figure(4), plt.subplot()
    ax.set_axis_off()
    gdf_blocks.plot(ax=ax, column='accessibility_score', scheme='quantiles', k=10, cmap='Reds', legend=True)
    gdf_amenities.plot(ax=ax, column='pob_reach', legend=True, cmap='Blues', scheme='quantiles')

    plt.show()

def get_args():
    parser = argparse.ArgumentParser(
        description="Join establishments with lots")
    parser.add_argument("input_dir", type=str,
                        help="The folder all the original data")
    parser.add_argument("output_dir", type=str,
                        help="The folder to save the output data")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


VISITS_AMENITIES_FILE = "visits_amenities.fgb"
if __name__ == "__main__":
    args = get_args()

    # TODO: Add capillas and comedores to amenities but not to the total consideration
    gdf_bounds = gpd.read_file(f"{args.input_dir}/{BOUNDS_FILE}", engine="pyogrio").to_crs("EPSG:4326")
    gdf_blocks = gpd.read_file(f"{args.output_dir}/{PROCESSED_BLOCKS_FILE}", engine="pyogrio").to_crs("EPSG:4326")
    gdf_blocks = gdf_blocks.set_index("cvegeo")
    gdf_amenities = gpd.read_file(f"{args.output_dir}/{VISITS_AMENITIES_FILE}", engine="pyogrio").to_crs("EPSG:4326")
    gdf_amenities['area'] = gdf_amenities.to_crs("EPSG:6933").area

    pedestrian_network = pdna.Network.from_hdf5(f"{args.input_dir}/{PEDESTRIAN_NETWORK_FILE}")
    pedestrian_network.precompute(WALK_RADIUS)

    gdf_blocks['node_ids'] = pedestrian_network.get_node_ids(gdf_blocks.geometry.centroid.x, gdf_blocks.geometry.centroid.y)
    gdf_blocks['node_ids'] = gdf_blocks['node_ids'].apply(lambda x: int(x) if pd.notnull(x) else None)
    gdf_amenities['node_ids'] = pedestrian_network.get_node_ids(gdf_amenities.geometry.centroid.x, gdf_amenities.geometry.centroid.y)
    gdf_amenities = gdf_amenities[["node_ids", "geometry", "name", "amenity", "num_workers", "students", "teachers", "area", "num_visits", "visits_category"]]
    gdf_amenities['node_ids'] = gdf_amenities['node_ids'].apply(lambda x: int(x) if pd.notnull(x) else None)

    df_aggregate = calculate_accessibility(gdf_blocks, gdf_amenities, pedestrian_network, AMENITIES_MAPPING)
    df_aggregate.to_csv(f"{args.output_dir}/accessibility_trips.csv")
    gdf_destinations = calculate_destination_metrics(df_aggregate)
    gdf_blocks = calculate_accessibility_scores(df_aggregate, gdf_blocks, gdf_destinations)

    gdf_destinations = gdf_destinations[gdf_destinations['opportunities_ratio'] > 0]
    df_amenities = gdf_amenities.merge(
        gdf_destinations, left_index=True, right_index=True, how="left"
    )
    gdf_amenities = gpd.GeoDataFrame(df_amenities, crs="EPSG:4326")
    gdf_blocks.to_file(f"{args.output_dir}/{ACCESSIBILITY_BLOCKS_FILE}", engine="pyogrio")

    gdf_amenities.to_file(f"{args.output_dir}/{AMENITIES_FILE}", engine="pyogrio")
    gdf_accessibility_points = gdf_amenities.copy()
    gdf_accessibility_points['geometry'] = gdf_accessibility_points.centroid
    gdf_accessibility_points.to_file(f"{args.output_dir}/{ACCESSIBILITY_FILE}", engine="pyogrio")

    plot_results(gdf_blocks, gdf_amenities)
