import os
import argparse

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import re

from src.scripts.utils.constants import AMENITIES_MAPPING, DENUE_TO_AMENITY_MAPPING, AMENITIES_FILE_MAPPING, LOTS_FILE, BOUNDS_FILE, ESTABLISHMENTS_LOTS_FILE, PROCESSED_BLOCKS_FILE, ESTABLISHMENTS_FILE, ASSIGN_ESTABLISHMENTS_FILE, AMENITIES_FILE
from src.scripts.utils.utils import convert_distance
import numpy as np


def assign_by_proximity(
    gdf_lots: gpd.GeoDataFrame, gdf_denue: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    gdf_denue["num_establishments"] = 1
    gdf_new_denue = gpd.sjoin_nearest(
        gdf_denue, gdf_lots[["lot_id", "geometry"]], how="left", max_distance=convert_distance(10)
    )
    gdf_new_denue = gdf_new_denue.drop_duplicates(subset=gdf_denue.denue_id.name)
    gdf_new_denue = gdf_new_denue.rename(columns={"ID": "lot_id"}).drop(columns=["index_right"])
    gdf_new_lots = gdf_new_denue.groupby("lot_id").agg(
        {
            "num_establishments": "count",
            "num_workers": "sum",
        }
    )
    gdf_new_lots = gdf_lots.merge(gdf_new_lots, on="lot_id", how="left")
    columns = ["num_establishments", "num_workers"]
    gdf_new_lots[columns] = gdf_new_lots[columns].fillna(0)
    return gdf_new_lots, gdf_new_denue


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

    gdf_lots = gpd.read_file(f"{args.input_dir}/{LOTS_FILE}", crs="EPSG:4326")
    gdf_blocks = gpd.read_file(f"{args.output_dir}/{PROCESSED_BLOCKS_FILE}", crs="EPSG:4326")
    gdf_blocks = gdf_blocks[['cvegeo', 'block_area', 'geometry']]
    gdf_lots = gpd.sjoin(gdf_lots, gdf_blocks, how="left", predicate="intersects")
    gdf_lots = (
        gdf_lots.groupby("lot_id")
        .agg({"zoning": "first", "geometry": "first", "cvegeo": "first"})
        .reset_index()
    )
    gdf_lots = gpd.GeoDataFrame(gdf_lots, crs="EPSG:4326")
    rest_gdf_blocks = gdf_blocks[~gdf_blocks["cvegeo"].isin(gdf_lots["cvegeo"])]
    _gdf_lots = gdf_lots[~gdf_lots.apply(
        lambda lot: rest_gdf_blocks.iloc[list(rest_gdf_blocks.sindex.intersection(lot.geometry.bounds))]["geometry"]
        .intersects(lot.geometry).any(), axis=1
    )]
    gdf_lots = gpd.GeoDataFrame(
        pd.concat([_gdf_lots, rest_gdf_blocks], ignore_index=True)
    )
    gdf_lots['lot_id'] = range(1, len(gdf_lots) + 1)
    gdf_lots['lot_id'] = gdf_lots['lot_id'].astype(str)
    # gdf_lots['lot_id'] = gdf_lots['lot_id'].ffill()

    gdf_denue = gpd.read_file(f"{args.output_dir}/{ESTABLISHMENTS_FILE}", crs="EPSG:4326")
    gdf_denue = gdf_denue.rename(columns={"id": "denue_id"})
    gdf_denue["codigo_act"] = gdf_denue["codigo_act"].astype(str)
    gdf_denue["amenity"] = None
    for item in DENUE_TO_AMENITY_MAPPING:
        gdf_denue.loc[gdf_denue.eval(item["query"]), "amenity"] = item["name"]

    gdf_denue["date"] = pd.to_datetime(gdf_denue["fecha_alta"], format='%Y-%m', errors='coerce')
    mask = gdf_denue["date"].isna()
    gdf_denue.loc[mask, "date"] = pd.to_datetime(gdf_denue.loc[mask, "fecha_alta"], format='%Y %m', errors='coerce')
    gdf_denue["year"] = gdf_denue["date"].dt.year
    gdf_denue = gdf_denue[
        ["denue_id", "amenity", "nom_estab", "codigo_act", "num_workers", "year", "geometry"]
    ]
    gdf_lots, gdf_denue = assign_by_proximity(gdf_lots, gdf_denue)

    if os.path.exists(f"{args.input_dir}/{AMENITIES_FILE}"):
        gdf_amenities = gpd.read_file(f"{args.input_dir}/{AMENITIES_FILE}", crs="EPSG:4326")
        gdf_amenities = gdf_amenities[gdf_amenities["amenity"].isin(AMENITIES_FILE_MAPPING.keys())]
        gdf_amenities["amenity"] = gdf_amenities["amenity"].replace(AMENITIES_FILE_MAPPING)
        gdf_denue_amenities = gdf_denue[gdf_denue["amenity"].notnull()].rename({"nom_estab": "name"}, axis=1)
        gdf_amenities = gpd.GeoDataFrame(
            pd.concat([gdf_amenities, gdf_denue_amenities],
                    ignore_index=True),
            crs="EPSG:4326",
        )
    else:
        gdf_amenities = gdf_denue[gdf_denue["amenity"].notnull()].rename({"nom_estab": "name"}, axis=1)

    gdf_lots.to_file(f"{args.output_dir}/{ESTABLISHMENTS_LOTS_FILE}")
    gdf_denue.to_file(f"{args.output_dir}/{ASSIGN_ESTABLISHMENTS_FILE}")
    gdf_amenities.to_file(f"{args.output_dir}/{AMENITIES_FILE}")

    if args.view:
        gdf_amenities.plot(column="amenity", legend=True)
        plt.show()
