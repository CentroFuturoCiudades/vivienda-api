import argparse
import os

import geopandas as gpd
import matplotlib.pyplot as plt
import osmnx as ox
import pandana as pdna
import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from utils.constants import (
    ACCESIBILITY_MAPPING,
    AMENITIES_MAPPING,
    MAX_ESTABLISHMENTS,
    PROXIMITY_MAPPING,
    WALK_RADIUS,
    WALK_SPEED,
)


def get_proximity(network, categories_mapping, distance, walk_speed):
    results = []
    for category, num_pois in categories_mapping.items():
        proximity = network.nearest_pois(
            distance=distance,
            category=category,
            num_pois=num_pois,
            include_poi_ids=False,
        )
        results.append(proximity[num_pois] / (walk_speed * 60))

    # get the maximum time for each category
    final_proximity = pd.concat(results, axis=1)
    final_proximity["minutes"] = final_proximity.max(axis=1)
    return final_proximity[['minutes']]


def get_accessibility(network, categories, distance, decay="linear"):
    results = []
    for category in categories:
        accessibility = network.aggregate(
            distance, type="sum", decay=decay, name=category
        )
        results.append(accessibility)
    final_accessibility = pd.concat(results, axis=1)
    final_accessibility["accessibility"] = final_accessibility.sum(axis=1)
    return final_accessibility[["accessibility"]]


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


def get_all_info(network, gdf, proximity_mapping):
    new_proximity_mapping = {}
    for sector, num_pois in proximity_mapping.items():
        points = gdf.loc[gdf['amenity'] == sector]
        if points.empty:
            continue
        network.set_pois(
            category=sector,
            x_col=points.geometry.x,
            y_col=points.geometry.y,
            maxdist=WALK_RADIUS,
            maxitems=MAX_ESTABLISHMENTS,
        )
        new_proximity_mapping[sector] = num_pois

    proximity = get_proximity(
        network, new_proximity_mapping, WALK_RADIUS, WALK_SPEED)
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

    gdfs_mapping = {
        "home": gdf_lots,
        "establishment": gdf_establishments,
        "amenity": gdf_amenities,
    }

    # TODO: Load pedestrian network from folder of project
    pedestrian_network = load_network(
        args.pedestrian_net_file, gdf_bounds, WALK_RADIUS)
    pedestrian_network.precompute(WALK_RADIUS)

    gdf_aggregate = gpd.GeoDataFrame()
    for item in AMENITIES_MAPPING:
        item_gdf = gdfs_mapping[item["to"]]
        item_gdf = item_gdf[item_gdf['amenity'] == item['name']]
        if item_gdf.empty:
            continue
        item_gdf['node_ids'] = pedestrian_network.get_node_ids(
            item_gdf.geometry.centroid.x, item_gdf.geometry.centroid.y
        )
        item_gdf = item_gdf[["node_ids", "geometry", "amenity"]]
        gdf_aggregate = pd.concat([gdf_aggregate, item_gdf], ignore_index=True)
    gdf_aggregate = gdf_aggregate.fillna(0)
    gdf_aggregate.to_file(args.accessibility_points_file)

    df_accessibility = get_all_info(
        pedestrian_network, gdf_aggregate, PROXIMITY_MAPPING
    )
    gdf_lots["node_ids"] = pedestrian_network.get_node_ids(
        gdf_lots.geometry.centroid.x, gdf_lots.geometry.centroid.y
    )
    gdf_lots = gdf_lots.merge(
        df_accessibility, left_on="node_ids", right_index=True, how="left"
    )
    gdf_lots = gdf_lots.drop(columns=["node_ids"])
    scaler = MinMaxScaler()

    gdf_lots['car_ratio'] = gdf_lots["VPH_AUTOM"] / gdf_lots["VIVTOT"]
    gdf_lots['pob_no_car'] = gdf_lots['POBTOT'] * (1 - gdf_lots['car_ratio'])

    gdf_lots['accessibility_score'] = (1 - scaler.fit_transform(
        gdf_lots[['minutes']])) * scaler.fit_transform(gdf_lots[['pob_no_car']])

    gdf_lots.to_file(args.output_file, engine="pyogrio")
    if args.view:
        fig, ax = plt.subplots()
        ax.set_axis_off()
        gdf_lots.plot(ax=ax, column="minutes",
                      cmap="Reds_r", legend=True, alpha=0.5)
        plt.show()
