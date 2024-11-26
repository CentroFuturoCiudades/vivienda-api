import argparse
import json
import math
import time

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import osmnx as ox

from src.scripts.utils.utils import normalize, remove_outliers
from src.scripts.utils.constants import LANDUSE_LOTS_FILE, ACCESSIBILITY_BLOCKS_FILE, BOUNDS_FILE, ZONING_REGULATIONS_FILE, UTILIZATION_LOTS_FILE


def gather_overture_data(bbox: tuple) -> gpd.GeoDataFrame:
    table = overturemaps.record_batch_reader("building", bbox).read_all()
    table = table.combine_chunks()
    df = table.to_pandas()
    gdf = gpd.GeoDataFrame(
        df[["num_floors", "height"]],
        geometry=gpd.GeoSeries.from_wkb(df["geometry"]),
        crs="EPSG:4326",
    )
    gdf["num_floors"] = gdf["num_floors"].fillna(1)
    return gdf


def get_zone_info(row, item):
    zoning_info = ZONING_REGULATIONS["zoning"].get(row["zoning"], [])
    for info in zoning_info:
        if "criteria" not in info or eval(info["criteria"], {}, row):
            return info.get(item, np.nan)
    return np.nan


def get_args():
    parser = argparse.ArgumentParser(
        description="Join establishments with lots")
    parser.add_argument("input_dir", type=str,
                        help="The folder all the original data")
    parser.add_argument("output_dir", type=str,
                        help="The folder to save the output data")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    gdf_bounds = gpd.read_file(
        f"{args.input_dir}/{BOUNDS_FILE}", crs="EPSG:4326")
    ZONING_REGULATIONS = json.load(
        open(f"{args.input_dir}/{ZONING_REGULATIONS_FILE}", "r"))
    for zoning, regulations in ZONING_REGULATIONS["zoning"].items():
        for i in regulations:
            if "IVE" in i and "density" not in i:
                i["density"] = i["CUS"] * 10_000 / i["IVE"]
    gdf_lots = gpd.read_file(
        f"{args.output_dir}/{LANDUSE_LOTS_FILE}", engine="pyogrio").to_crs("EPSG:4326")
    gdf_blocks = gpd.read_file(
        f"{args.output_dir}/{ACCESSIBILITY_BLOCKS_FILE}", engine="pyogrio").to_crs("EPSG:4326")
    gdf_lots = pd.merge(gdf_lots.drop(columns=["block_area"]), gdf_blocks.drop(
        columns=["geometry"]), on="cvegeo", how="left")
    gdf_lots = gpd.GeoDataFrame(gdf_lots, crs="EPSG:4326")

    G = ox.graph_from_polygon(gdf_bounds.buffer(
        0.001).unary_union, network_type='drive')
    gdf_edges = ox.graph_to_gdfs(G, nodes=False, edges=True)
    gdf_edges['name'] = gdf_edges['name'].apply(
        lambda x: x[0] if isinstance(x, list) else x)
    gdf_edges = gdf_edges[gdf_edges["name"].isin(ZONING_REGULATIONS["streets"])]
    buffer = 0.00015
    gdf_lots['street_names'] = gdf_lots.buffer(buffer).apply(
        lambda x: gdf_edges.loc[gdf_edges.intersects(x), 'name'].tolist()
    )

    fig, ax = plt.subplots(figsize=(15, 15))
    ax.set_axis_off()
    gdf_lots.buffer(buffer).plot(ax=ax, color='red')
    gdf_lots.plot(ax=ax, color='lightgray')
    gdf_lots[gdf_lots['street_names'].apply(len) > 0].plot(ax=ax, column='zoning', legend=True)
    plt.show()

    gdf_lots["num_properties"] = gdf_lots["tvivhab"] + \
        gdf_lots["num_establishments"]
    gdf_lots["wasteful_area"] = (
        gdf_lots["unused_area"] +
        gdf_lots["parking_area"] + gdf_lots["green_area"]
    )
    gdf_lots["wasteful_ratio"] = (
        gdf_lots["unused_ratio"] +
        gdf_lots["parking_ratio"] + gdf_lots["green_ratio"]
    )
    gdf_lots["occupancy"] = gdf_lots["pobtot"] + gdf_lots["num_workers"]
    gdf_lots["underutilized_area"] = gdf_lots["wasteful_area"] / \
        gdf_lots["occupancy"]
    gdf_lots["underutilized_area"] = remove_outliers(
        gdf_lots["underutilized_area"], 0, 0.9
    )
    gdf_lots["underutilized_ratio"] = gdf_lots.apply(
        lambda x: (
            x["wasteful_ratio"] / (x["occupancy"] + 1)
            if x["wasteful_ratio"] > 0.25
            else 0
        ),
        axis=1,
    )
    gdf_lots["underutilized_ratio"] = remove_outliers(
        gdf_lots["underutilized_ratio"], 0, 0.9
    )
    gdf_lots["underutilized_ratio"] = normalize(
        gdf_lots["underutilized_ratio"])

    gdf_lots["occupancy_density"] = gdf_lots.apply(
        lambda x: x["occupancy"] /
        x["building_area"] if x["building_area"] > 0 else 0,
        axis=1,
    )
    gdf_lots["occupancy_density"] = remove_outliers(
        gdf_lots["occupancy_density"], 0, 0.9
    )

    gdf_lots["home_density"] = gdf_lots.apply(
        lambda x: (
            (x["vivtot"] + x["num_workers"]) / x["building_area"]
            if x["building_area"] > 0
            else 0
        ),
        axis=1,
    )
    gdf_lots["home_density"] = remove_outliers(
        gdf_lots["home_density"], 0, 0.9)

    # gdf_lots["combined_score"] = (1 - normalize(gdf_lots["minutes"])) + normalize(
    #     gdf_lots["underutilized_area"]
    # )
    # gdf_lots["combined_score"] = normalize(gdf_lots["combined_score"])

    gdf_lots["latitud"] = gdf_lots["geometry"].centroid.y
    gdf_lots["longitud"] = gdf_lots["geometry"].centroid.x

    gdf_bounds = gpd.read_file(
        f"{args.input_dir}/{BOUNDS_FILE}", crs="EPSG:4326")
    bounds = gdf_bounds.geometry[0]
    bbox = bounds.bounds
    gdf_lots["lot_area"] = gdf_lots.to_crs("EPSG:32614").area # m2
    gdf_lots["building_area"] = gdf_lots.apply(lambda x: x["building_area"] if x["building_ratio"] > 0.1 else 0, axis=1)
    gdf_lots["building_ratio"] = gdf_lots.apply(lambda x: x["building_ratio"] if x["building_ratio"] > 0.1 else 0, axis=1)

    gdf_lots["max_cos"] = gdf_lots.apply(
        lambda x: get_zone_info(x, "COS"), axis=1)
    gdf_lots["max_building_area"] = gdf_lots["lot_area"] * gdf_lots["max_cos"] # m2
    gdf_lots["max_cus"] = gdf_lots.apply(
        lambda x: get_zone_info(x, "CUS"), axis=1)
    gdf_lots["max_builtup_area"] = gdf_lots["lot_area"] * gdf_lots["max_cus"] # m2
    gdf_lots["min_cav"] = gdf_lots.apply(
        lambda x: get_zone_info(x, "CAV"), axis=1)
    gdf_lots["max_num_levels"] = gdf_lots.apply(
        lambda x: get_zone_info(x, "num_levels"), axis=1
    )
    gdf_lots["max_density"] = gdf_lots.apply(
        lambda x: get_zone_info(x, "density"), axis=1) # viv/ha
    gdf_lots["max_home_units"] = gdf_lots.apply(
        lambda x: (
            np.nan
            if np.isnan(x["max_density"])
            else x["lot_area"] * x["max_density"] / 10_000
        ),
        axis=1,
    )
    gdf_lots["max_home_units"] = (
        gdf_lots["max_home_units"]
        .fillna(1)
        .apply(lambda x: math.floor(x) if x < np.inf else np.inf)
    )
    gdf_lots["max_home_units"] = gdf_lots["max_home_units"].apply(
        lambda x: 1 if x < 1 else x
    )
    gdf_lots['building_area'] = gdf_lots['building_area'] * 10_000 # m2
    # gdf_lots['cos'] = gdf_lots['building_area'] * 10_000
    gdf_lots['builtup_area'] = gdf_lots['num_levels'] * gdf_lots['building_area']
    gdf_lots['cos'] = gdf_lots['building_area'] / gdf_lots['lot_area']
    gdf_lots['cus'] = gdf_lots['builtup_area'] / gdf_lots['lot_area']
    gdf_lots['block_cus'] = gdf_lots.groupby('cvegeo')['cus'].transform('sum')
    # gdf_lots['home_units'] = gdf_lots['vivtot'] * gdf_lots['cus'] / \
    #     gdf_lots['block_cus']  # assuming all units are the same size
    # gdf_lots['home_units'] = np.ceil(gdf_lots['home_units'])
    # gdf_lots['density'] = gdf_lots['home_units'] / (gdf_lots['lot_area'] * 10_000)
    gdf_lots['density'] = gdf_lots['vivtot'] / gdf_lots['block_area']
    counts = gdf_lots.groupby('cvegeo').size()
    gdf_lots['home_units'] = gdf_lots['vivtot'] / gdf_lots['cvegeo'].map(counts)

    gdf_lots['population'] = gdf_lots['prom_ocup'] * gdf_lots['home_units']
    gdf_lots["potential_home_units"] = (
        gdf_lots["max_home_units"] - gdf_lots["home_units"]
    )

    # gdf_lots["optimal_CAV"] = gdf_lots["min_CAV"] - gdf_lots["green_ratio"]
    # gdf_lots["diff_CAV"] = gdf_lots["min_CAV"] - (
    #     gdf_lots["green_ratio"] + gdf_lots["unused_ratio"] + gdf_lots["parking_ratio"]
    # )

    gdf_lots.to_file(
        f"{args.output_dir}/{UTILIZATION_LOTS_FILE}", engine="pyogrio")

    if args.view:
        fig, ax = plt.subplots(ncols=3, figsize=(30, 30))
        ax[0].set_axis_off()
        ax[1].set_axis_off()
        ax[2].set_axis_off()
        ax[0].set_title("Máximo de viviendas por predio")
        ax[1].set_title("Viviendas estimadas por predio")
        ax[2].set_title("Diff Máximo de viviendas por predio")
        gdf_lots.plot(ax=ax[0], color="lightgray")
        gdf_lots[gdf_lots["max_home_units"] > 0].plot(
            ax=ax[0], column="max_home_units", scheme="quantiles", k=10, legend=True
        )
        gdf_lots.plot(ax=ax[1], color="lightgray")
        gdf_lots[gdf_lots["density"] > 0].plot(
            ax=ax[1], column="density", scheme="quantiles", k=10, legend=True
        )
        gdf_lots.plot(ax=ax[2], color="lightgray")
        gdf_lots[gdf_lots["potential_home_units"] > 0].plot(
            ax=ax[2],
            column="potential_home_units",
            scheme="quantiles",
            k=10,
            legend=True,
        )
        plt.show()
