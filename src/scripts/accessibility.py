import argparse
import os

import geopandas as gpd
import matplotlib.pyplot as plt
import osmnx as ox
import pandana as pdna
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np

from src.utils.constants import (
    ACCESIBILITY_MAPPING,
    AMENITIES_MAPPING,
    MAX_ESTABLISHMENTS,
    PROXIMITY_MAPPING,
    WALK_RADIUS,
    WALK_SPEED,
)
from src.utils.utils import normalize
from functools import lru_cache

BETA_GRAVITY = 0.001


def get_proximity(network, categories_mapping):
    results = []
    for category, details in categories_mapping.items():
        distance = details['radius']
        num_pois = details['num_pois']
        proximity = network.nearest_pois(
            distance=distance,
            category=category,
            num_pois=num_pois,
            include_poi_ids=False,
        )
        results.append(proximity[num_pois] / (WALK_SPEED * 60))

    # get the maximum time for each category
    final_proximity = pd.concat(results, axis=1)
    final_proximity.columns = [
        category for category in categories_mapping.keys()]
    # set an accessibility score from all categories as a ponderated average using importance from the categories
    mapping_importance = {
        item['name']: item['importance']
        for item in AMENITIES_MAPPING
    }
    final_proximity['accessibility'] = final_proximity.apply(
        lambda x: sum([x[category] * categories_mapping[category]['importance'] for category in categories_mapping.keys()]), axis=1)
    final_proximity["minutes"] = final_proximity.max(axis=1)
    return final_proximity


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


def calculate_accessibility(network, gdf, amenities_mapping):
    new_proximity_mapping = {}

    for item in amenities_mapping:
        sector = item["name"]
        num_pois = 1  # Assuming you want to find the nearest one
        points = gdf.loc[gdf['amenity'] == sector]

        if points.empty:
            continue
        network.set_pois(
            category=sector,
            x_col=points.geometry.centroid.x,
            y_col=points.geometry.centroid.y,
            maxdist=item['radius'],
            maxitems=num_pois,
        )
        new_proximity_mapping[sector] = {
            'radius': item['radius'],
            'num_pois': num_pois,
            'importance': item['importance'],
        }
    proximity = get_proximity(network, new_proximity_mapping)
    return proximity


def get_args():
    parser = argparse.ArgumentParser(
        description="Join establishments with lots")
    parser.add_argument("bounds_file", type=str,
                        help="The file with all the data")
    parser.add_argument("landuse_file", type=str,
                        help="The file with all the data")
    parser.add_argument(
        "establishments_file", type=str, help="The file with all the data"
    )
    parser.add_argument("amenities_file", type=str,
                        help="The file with all the data")
    parser.add_argument(
        "pedestrian_net_file", type=str, help="The file with all the data"
    )
    parser.add_argument(
        "accessibility_points_file", type=str, help="The file with all the data"
    )
    parser.add_argument("output_file", type=str, help="The folder")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    gdf_lots = gpd.read_file(
        args.landuse_file, engine="pyogrio").to_crs("EPSG:4326")
    gdf_bounds = gpd.read_file(
        args.bounds_file, engine="pyogrio").to_crs("EPSG:4326")
    gdf_establishments = gpd.read_file(
        args.establishments_file, engine="pyogrio").to_crs("EPSG:4326")
    gdf_establishments["codigo_act"] = gdf_establishments["codigo_act"].astype(
        str)
    gdf_amenities = gpd.read_file(
        args.amenities_file, engine="pyogrio").to_crs("EPSG:4326")

    for item in gdf_amenities:
        item = item.replace(" ", "_")

    gdfs_mapping = {
        "home": gdf_lots,
        "establishment": gdf_establishments,
        "amenity": gdf_amenities,
    }

    # TODO: Load pedestrian network from folder of project
    pedestrian_network = load_network(
        args.pedestrian_net_file, gdf_bounds, WALK_RADIUS)
    pedestrian_network.precompute(WALK_RADIUS)

    accessibility_scores = {}
    gdf_aggregate = gpd.GeoDataFrame()

    for item in AMENITIES_MAPPING:
        from_gdf = gdfs_mapping[item["from"]]

        if "query_from" in item:
            from_gdf = from_gdf.query(item["query_from"])
        to_gdf = gdfs_mapping[item["to"]]
        if "query_to" in item:
            to_gdf = to_gdf.query(item["query_to"])

        if from_gdf.empty or to_gdf.empty:
            continue
        from_gdf['node_ids'] = pedestrian_network.get_node_ids(
            from_gdf.geometry.centroid.x, from_gdf.geometry.centroid.y
        )

        to_gdf['node_ids'] = pedestrian_network.get_node_ids(
            to_gdf.geometry.centroid.x, to_gdf.geometry.centroid.y
        )

        item_gdf = to_gdf

        item_gdf['node_ids'] = pedestrian_network.get_node_ids(
            item_gdf.geometry.centroid.x, item_gdf.geometry.centroid.y
        )

        item_gdf = item_gdf[["node_ids", "geometry", "amenity"]]

        gdf_aggregate = pd.concat([gdf_aggregate, item_gdf], ignore_index=True)
        gdf_aggregate = gdf_aggregate.fillna(0)
        gdf_aggregate.to_file(args.accessibility_points_file)

        sector = item['name']
        to_gdf = to_gdf[["node_ids", "geometry"]]
        to_gdf['category'] = sector
        amount = item.get("amount", 1)
        importance = item['importance']
        pedestrian_network.set_pois(
            category=sector,
            x_col=to_gdf.geometry.centroid.x,
            y_col=to_gdf.geometry.centroid.y,
            maxdist=item['radius'],
            maxitems=20,
        )
        proximity = pedestrian_network.nearest_pois(
            distance=item['radius'],
            category=sector,
            num_pois=20,
            include_poi_ids=False,
        )

        for i in range(1, 21):
            from_gdf[f'distance_{i}'] = from_gdf['node_ids'].map(proximity[i])
            within_radius = from_gdf[f'distance_{i}'] < item['radius']
            from_gdf[f'gravity_temp_{i}'] = np.where(
                within_radius,
                from_gdf["POBTOT"] * importance /
                np.exp(BETA_GRAVITY * from_gdf[f'distance_{i}']),
                0
            )
        distance = proximity[amount]
        minutes = distance / (WALK_SPEED * 60)
        from_gdf['distance'] = from_gdf['node_ids'].map(distance)
        from_gdf['minutes'] = from_gdf['node_ids'].map(minutes)

        from_gdf['gravity_score'] = from_gdf[[
            f'gravity_temp_{i}' for i in range(1, 21)]].sum(axis=1)
        accessibility_scores.update(from_gdf.groupby(
            'node_ids')['gravity_score'].sum().to_dict())

    accessibility_df = pd.DataFrame.from_dict(
        accessibility_scores, orient='index', columns=['accessibility_score'])
    gdf_lots = gdf_lots.merge(
        accessibility_df, left_on="node_ids", right_index=True, how="left"
    )
    gdf_lots['accessibility_score'] = normalize(gdf_lots['accessibility_score'])
    print(gdf_lots['accessibility_score'].describe())
    gdf_lots.to_file(args.output_file, engine="pyogrio")

    fig, ax = plt.subplots()
    ax.set_axis_off()
    gdf_lots.plot(ax=ax, column='accessibility_score',
                  scheme='quantiles', k=10, cmap='Reds')
    new_gdf_amenities = gpd.GeoDataFrame()
    for x in AMENITIES_MAPPING:
        tmp = gdf_amenities.query(x["query_to"])
        new_gdf_amenities = gpd.GeoDataFrame(
            pd.concat([tmp, new_gdf_amenities], ignore_index=True)
        )
    new_gdf_amenities.plot(ax=ax, column='amenity', legend=True, cmap='tab20')
    plt.show()
