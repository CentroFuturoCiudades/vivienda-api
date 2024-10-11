import argparse
import os

import geopandas as gpd
import osmnx as ox
import pandana as pdna
import pandas as pd
import numpy as np

from src.scripts.utils.constants import (
    AMENITIES_MAPPING,
    MAX_ESTABLISHMENTS,
    WALK_RADIUS,
    WALK_SPEED,
)

# TODO: Use beta with recommended radius of the amenities
BETA_GRAVITY = {item['name']: 1 / (item['radius'] / 3) for item in AMENITIES_MAPPING}
NUM_POIS = 5


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
        results.append(proximity[num_pois] / WALK_SPEED)

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
    import matplotlib.pyplot as plt
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
    gdf_amenities['area'] = gdf_amenities.to_crs("EPSG:3043").area

    for item in gdf_amenities:
        item = item.replace(" ", "_")

    # TODO: Load pedestrian network from folder of project
    pedestrian_network = load_network(
        args.pedestrian_net_file, gdf_bounds, WALK_RADIUS)
    pedestrian_network.precompute(WALK_RADIUS)

    accessibility_scores = {}
    gdf_aggregate = gpd.GeoDataFrame()
    mean_size = gdf_lots['geometry'].area.median()

    for item in AMENITIES_MAPPING:
        to_gdf = gdf_amenities
        if "query_to" in item:
            to_gdf = to_gdf.query(item["query_to"])

        gdf_lots['node_ids'] = pedestrian_network.get_node_ids(
            gdf_lots.geometry.centroid.x, gdf_lots.geometry.centroid.y
        )
        gdf_lots = gdf_lots.reset_index()
        gdf_lots['index'] = gdf_lots['index'].astype(str)
        gdf_lots = gdf_lots.set_index('index')

        to_gdf['node_ids'] = pedestrian_network.get_node_ids(
            to_gdf.geometry.centroid.x, to_gdf.geometry.centroid.y
        )

        to_gdf = to_gdf[["node_ids", "geometry", "amenity", "num_workers", "students", "teachers", "area"]]

        sector = item['name']
        gdf_amenities['category'] = sector
        amount = item.get("amount", 1)
        importance = item['importance']
        pedestrian_network.set_pois(
            category=sector,
            x_col=to_gdf.geometry.centroid.x,
            y_col=to_gdf.geometry.centroid.y,
            maxdist=WALK_RADIUS,
            maxitems=NUM_POIS,
        )
        proximity = pedestrian_network.nearest_pois(
            distance=WALK_RADIUS,
            category=sector,
            num_pois=NUM_POIS,
            include_poi_ids=True,
        )
        proximity['amenity'] = sector
        gdf_lots['population'] = gdf_lots.eval(item["pob_query"])
        proximity = proximity.reset_index().merge(gdf_lots[['node_ids', 'population']], left_on='osmid', right_on='node_ids', how='left')
        proximity = proximity.groupby('osmid').agg('first').reset_index().drop(columns=['node_ids'])
        num_poi_cols = range(1, NUM_POIS + 1)
        poi_cols = [f'poi{col}' for col in num_poi_cols]
        proximity = proximity.rename(columns={x: f'distance{x}' for x in num_poi_cols})
        proximity = pd.wide_to_long(proximity, stubnames=['distance', 'poi'], i='osmid', j='num_poi', sep='').reset_index()
        proximity = proximity[proximity['distance'] < item['radius']]
        proximity = proximity.rename(columns={'poi': 'destination_id', 'osmid': 'origin_id', 'num_poi': 'num_establishment'})
        # proximity['attraction'] = proximity['destination_id'].apply(lambda x: max(to_gdf.loc[x]['geometry'].area, mean_size) if x in to_gdf.index else mean_size)
        attraction_values = to_gdf.eval(item["attraction_query"])
        proximity['attraction'] = proximity.apply(lambda x: attraction_values.loc[x['destination_id']] if x['destination_id'] in to_gdf.index else 0, axis=1)
        proximity['gravity'] = proximity.apply(lambda x: 1 / np.exp(BETA_GRAVITY[x['amenity']] * x['distance']), axis=1)
        proximity['pob_reach'] = proximity['population'] * proximity['gravity']
        proximity['minutes'] = proximity['distance'] / WALK_SPEED

        gdf_aggregate = pd.concat([gdf_aggregate, proximity], ignore_index=True)
    
    gdf_destinations = gdf_aggregate.groupby('destination_id').agg({
        'pob_reach': 'sum',
        'attraction': 'first'
    })
    gdf_destinations['opportunities_ratio'] = gdf_destinations.apply(lambda x: x['attraction'] / x['pob_reach'] if x['pob_reach'] > 0 else 0, axis=1)
    gdf_aggregate['accessibility_score'] = gdf_aggregate.apply(lambda x: gdf_destinations['opportunities_ratio'].loc[x['destination_id']] * x['gravity']
        if x['destination_id'] in gdf_destinations['opportunities_ratio'].index else 0, axis=1)
    accessibility_scores = gdf_aggregate.groupby(['origin_id', 'amenity']).agg({'accessibility_score': 'sum', 'minutes': 'min'})
    accessibility_scores = accessibility_scores.groupby('origin_id').agg({'accessibility_score': 'sum', 'minutes': 'max'})
    gdf_lots = gdf_lots.merge(
        accessibility_scores, left_on="node_ids", right_index=True, how="left"
    )
    gdf_lots = gdf_lots.drop(columns=["node_ids"])
    gdf_destinations = gdf_destinations[gdf_destinations['opportunities_ratio'] > 0]
    gdf_amenities = gdf_amenities.merge(
        gdf_destinations, left_index=True, right_index=True, how="left"
    )
    gdf_lots.to_file(args.output_file, engine="pyogrio")

    gdf_accessibility_points = gdf_amenities.copy()
    gdf_accessibility_points['geometry'] = gdf_accessibility_points.centroid
    gdf_accessibility_points.to_file(args.accessibility_points_file)

    fig, ax = plt.figure(1), plt.subplot()
    ax.set_axis_off()
    gdf_lots.plot(ax=ax, column='minutes', cmap='Reds', legend=True)
    fig, ax = plt.figure(2), plt.subplot()
    ax.set_axis_off()
    gdf_lots.plot(ax=ax, column='accessibility_score',
                  scheme='quantiles', k=10, cmap='Reds', legend=True)
    gdf_amenities.plot(ax=ax, column='opportunities_ratio', legend=True, cmap='Blues', scheme='quantiles')

    fig, ax = plt.figure(3), plt.subplot()
    ax.set_axis_off()
    gdf_lots.plot(ax=ax, column='accessibility_score',
                  scheme='quantiles', k=10, cmap='Reds', legend=True)
    gdf_amenities.plot(ax=ax, column='attraction', legend=True, cmap='Blues', scheme='quantiles')

    fig, ax = plt.figure(4), plt.subplot()
    ax.set_axis_off()
    gdf_lots.plot(ax=ax, column='accessibility_score',
                  scheme='quantiles', k=10, cmap='Reds', legend=True)
    gdf_amenities.plot(ax=ax, column='pob_reach', legend=True, cmap='Blues', scheme='quantiles')
    plt.show()
