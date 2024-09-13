import argparse
import json
import math
import time

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
# import overturemaps

from src.utils.utils import normalize, remove_outliers


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
    zoning_info = ZONING_REGULATIONS.get(row["zoning"], [])
    for info in zoning_info:
        if "criteria" not in info or eval(info["criteria"], {}, row):
            return info.get(item, np.nan)
    return np.nan


def get_args():
    parser = argparse.ArgumentParser(description="Join establishments with lots")
    parser.add_argument("bounds_file", type=str, help="The file with all the data")
    parser.add_argument(
        "accessibility_file", type=str, help="The file with all the data"
    )
    parser.add_argument(
        "zonification_file", type=str, help="The file with all the data"
    )
    parser.add_argument(
        "zoning_regulations_file", type=str, help="The file with all the data"
    )
    parser.add_argument("output_file", type=str, help="The file with all the data")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    ZONING_REGULATIONS = json.load(open(args.zoning_regulations_file, "r"))
    for zoning, regulations in ZONING_REGULATIONS.items():
        for i in regulations:
            if "IVE" in i and "density" not in i:
                i["density"] = i["CUS"] * 10_000 / i["IVE"]
    gdf_lots = gpd.read_file(args.accessibility_file, engine="pyogrio").to_crs("EPSG:4326")

    gdf_lots["num_properties"] = gdf_lots["TVIVHAB"] + gdf_lots["num_establishments"]
    gdf_lots["wasteful_area"] = (
        gdf_lots["unused_area"] + gdf_lots["parking_area"] + gdf_lots["green_area"]
    )
    gdf_lots["wasteful_ratio"] = (
        gdf_lots["unused_ratio"] + gdf_lots["parking_ratio"] + gdf_lots["green_ratio"]
    )
    gdf_lots["occupancy"] = gdf_lots["POBTOT"] + gdf_lots["num_workers"]
    gdf_lots["underutilized_area"] = gdf_lots["wasteful_area"] / gdf_lots["occupancy"]
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
    gdf_lots["underutilized_ratio"] = normalize(gdf_lots["underutilized_ratio"])

    gdf_lots["occupancy_density"] = gdf_lots.apply(
        lambda x: x["occupancy"] / x["building_area"] if x["building_area"] > 0 else 0,
        axis=1,
    )
    gdf_lots["occupancy_density"] = remove_outliers(
        gdf_lots["occupancy_density"], 0, 0.9
    )

    gdf_lots["home_density"] = gdf_lots.apply(
        lambda x: (
            (x["VIVTOT"] + x["num_workers"]) / x["building_area"]
            if x["building_area"] > 0
            else 0
        ),
        axis=1,
    )
    gdf_lots["home_density"] = remove_outliers(gdf_lots["home_density"], 0, 0.9)

    gdf_lots["combined_score"] = (1 - normalize(gdf_lots["minutes"])) + normalize(
        gdf_lots["underutilized_area"]
    )
    gdf_lots["combined_score"] = normalize(gdf_lots["combined_score"])

    gdf_lots["latitud"] = gdf_lots["geometry"].centroid.y
    gdf_lots["longitud"] = gdf_lots["geometry"].centroid.x

    gdf_zonfication = gpd.read_file(args.zonification_file, crs="EPSG:4326").to_crs(
        epsg=4326
    )
    gdf_zonfication = gdf_zonfication[["ID", "zoning", "geometry"]]

    gdf_lots["ID"] = gdf_lots["ID"].apply(lambda x: int(float(x))).astype(str)
    gdf_lots = gdf_lots[gdf_lots.geometry.is_valid]

    # merge by ID
    gdf_lots = gdf_lots.merge(gdf_zonfication, left_on="ID", right_on="ID", how="inner")
    gdf_lots = gpd.GeoDataFrame(
        gdf_lots.drop(columns=["geometry_x", "geometry_y"]),
        geometry=gdf_lots.geometry_x,
    )

    start = time.time()
    gdf_bounds = gpd.read_file(args.bounds_file, crs="EPSG:4326")
    bounds = gdf_bounds.geometry[0]
    bbox = bounds.bounds
    # gdf = gather_overture_data(bbox)
    # gdf_temp = gdf_lots.overlay(gdf, how="intersection")
    # gdf_temp = gdf_temp.groupby("ID").agg({"num_floors": "max"})
    # gdf_lots = gdf_lots.merge(gdf_temp, on="ID")
    gdf_lots["num_floors"] = 1

    gdf_lots["max_COS"] = gdf_lots.apply(lambda x: get_zone_info(x, "COS"), axis=1)
    gdf_lots["max_CUS"] = gdf_lots.apply(lambda x: get_zone_info(x, "CUS"), axis=1)
    gdf_lots["min_CAV"] = gdf_lots.apply(lambda x: get_zone_info(x, "CAV"), axis=1)
    gdf_lots["max_height"] = gdf_lots.apply(
        lambda x: get_zone_info(x, "height"), axis=1
    )
    gdf_lots["diff_height"] = gdf_lots["max_height"] - gdf_lots["num_floors"]

    gdf_lots["max_home_units"] = gdf_lots.apply(
        lambda x: get_zone_info(x, "density"), axis=1
    )
    gdf_lots["max_home_units"] = gdf_lots.apply(
        lambda x: (
            np.nan
            if np.isnan(x["max_home_units"])
            else x["lot_area"] * x["max_home_units"]
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
    gdf_lots["building_volume"] = gdf_lots["building_area"] * gdf_lots["num_floors"]
    gdf_lots["building_volume_block"] = gdf_lots.groupby("CVEGEO")[
        "building_volume"
    ].transform("sum")
    gdf_lots["units_per_built_area"] = (
        gdf_lots["VIVTOT"] / gdf_lots["building_volume_block"]
    )
    gdf_lots["units_estimate"] = (
        gdf_lots["units_per_built_area"] * gdf_lots["building_area"]
    )
    gdf_lots["units_estimate"] = (
        gdf_lots["units_estimate"]
        .fillna(1)
        .apply(lambda x: math.ceil(x) if x < np.inf else np.inf)
    )
    gdf_lots['population'] = gdf_lots['PROM_OCUP'] * gdf_lots['units_estimate']
    gdf_lots["potential_new_units"] = (
        gdf_lots["max_home_units"] - gdf_lots["units_estimate"]
    )

    gdf_lots["optimal_CAV"] = gdf_lots["min_CAV"] - gdf_lots["green_ratio"]
    gdf_lots["diff_CAV"] = gdf_lots["min_CAV"] - (
        gdf_lots["green_ratio"] + gdf_lots["unused_ratio"] + gdf_lots["parking_ratio"]
    )

    gdf_lots.to_file(args.output_file, engine="pyogrio")

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
        gdf_lots[gdf_lots["units_estimate"] > 0].plot(
            ax=ax[1], column="units_estimate", scheme="quantiles", k=10, legend=True
        )
        gdf_lots.plot(ax=ax[2], color="lightgray")
        gdf_lots[gdf_lots["potential_new_units"] > 0].plot(
            ax=ax[2],
            column="potential_new_units",
            scheme="quantiles",
            k=10,
            legend=True,
        )
        plt.show()
