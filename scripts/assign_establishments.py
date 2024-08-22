import argparse

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import re

from utils.constants import AMENITIES_MAPPING, DENUE_TO_AMENITY_MAPPING
import numpy as np


mapping = {
    r"^623": "Asistencia social",
    r"^6215": "Laboratorios clínicos",
    r"^6212": "Otros consultorios",
    r"^6213": "Otros consultorios",
    r"^6211": "Consultorios médicos",
    r"^6221": "Hospital general",
    r"^6222": "Hospitales psiquiátricos",
    r"^6223": "Hospitales otras especialidades",
    r"^46411": "Farmacia",
    r"^71394": "Clubs deportivos y de acondicionamiento físico",
    r"^51213": "Cine",
    r"^7139": "Otros Servicios recreativos",
    r"^7112": "Espectáculos deportivos",
    r"^7131": "Parques recreativos",
    r"^71212": "Sitios históricos",
    r"^71213": "Jardines botánicos y zoológicos",
    r"^7223": "Grutas, parques naturales o patrimonio cultural",
    r"^7111": "Espectáculos artísticos y culturales",
    r"^71211": "Museos",
    r"^51921": "Biblioteca",
    r"^6244": "Guarderia",
    r"^61111": "Educación Preescolar",
    r"^61112": "Educación Primaria",
    r"^61113": "Educación Secundaria",
    r"^61114": "Educación Secundaria Técnica",
    r"^61115": "Educacion Media Técnica",
    r"^61116": "Educación Media Superior",
    r"^61121": "Educación Técnica Superior",
    r"^6113": "Educación Superior",
}


def map_equipment_type(clee):
    for pattern, equipment_type in mapping.items():
        if re.match(pattern, clee):
            return equipment_type
    return np.nan


def assign_by_buffer(
    gdf_block_lots: gpd.GeoDataFrame, gdf_denue: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    _gdf_denue = gdf_denue.to_crs("EPSG:32614")
    _gdf_block_lots = gdf_block_lots.to_crs("EPSG:32614")
    _gdf_block_lots["geometry"] = _gdf_block_lots.buffer(10)
    gdf_temp = gpd.GeoDataFrame(columns=gdf_denue.columns)
    for block_id in gdf_block_lots["CVE_GEO"].unique():
        establishments_in_block = _gdf_denue.loc[_gdf_denue["CVE_GEO"] == block_id]
        lots_in_block2 = gdf_block_lots.loc[gdf_block_lots["CVE_GEO"] == block_id]
        lots_in_block = _gdf_block_lots.loc[_gdf_block_lots["CVE_GEO"] == block_id]
        if not establishments_in_block.empty:
            fig, ax = plt.subplots()
            lots_in_block.plot(ax=ax, color="blue")
            lots_in_block2.to_crs("EPSG:32614").plot(ax=ax, color="green")
            establishments_in_block.plot(ax=ax, color="red")
            plt.show()
            print(f"Block {block_id} has {
                  len(establishments_in_block)} establishments")
            joined_in_block = gpd.sjoin(
                establishments_in_block, lots_in_block, how="left"
            )
            joined_in_block = joined_in_block.drop(
                columns=["CVE_GEO_left", "index_right"]
            )
            joined_in_block = joined_in_block.rename(
                columns={"CVE_GEO_right": "CVE_GEO"}
            )
            gdf_temp = pd.concat([gdf_temp, joined_in_block])
    gdf_temp = gdf_temp.reset_index(drop=True)
    establishment_counts = gdf_temp.groupby(["CVE_GEO", "ID"]).agg(
        {
            "num_establishments": "count",
            "num_workers": "sum",
            "sector": lambda x: dict(x.value_counts()),
        }
    )
    gdf_lots = gdf_block_lots.merge(
        establishment_counts, on=["CVE_GEO", "ID"], how="left"
    )
    return gdf_lots


def assign_by_proximity(
    gdf_block_lots: gpd.GeoDataFrame, gdf_denue: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    _gdf_denue = gdf_denue.to_crs("EPSG:32614")
    _gdf_block_lots = gdf_block_lots.to_crs("EPSG:32614")
    _gdf_denue["num_establishments"] = 1
    new_gdf = gpd.sjoin_nearest(
        _gdf_denue, _gdf_block_lots[["ID", "geometry"]], how="left", max_distance=10
    )
    gdf_final = new_gdf.groupby("ID").agg(
        {
            "num_establishments": "count",
            "num_workers": "sum",
        }
    )
    new_gdf = new_gdf.rename(columns={"ID": "ID_lot"})
    gdf_final = _gdf_block_lots.merge(gdf_final, on="ID", how="left")
    columns = ["num_establishments", "num_workers"]
    gdf_final[columns] = gdf_final[columns].fillna(0)
    return gdf_final.to_crs("EPSG:4326"), new_gdf


def get_args():
    parser = argparse.ArgumentParser(
        description="Join establishments with lots")
    parser.add_argument("population_file", type=str,
                        help="The file with all the data")
    parser.add_argument(
        "establishments_file", type=str, help="The file with all the data"
    )
    parser.add_argument("output_file", type=str,
                        help="The file with all the data")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    # Load the polygons for the lots
    gdf_lots = gpd.read_file(args.population_file).to_crs("EPSG:4326")
    gdf_lots["area"] = gdf_lots.to_crs("EPSG:6933").area / 10_000

    # Load the coordinates for the establishments
    gdf_denue = gpd.read_file(args.establishments_file, crs="EPSG:4326")
    gdf_denue = gdf_denue.rename(columns={"id": "DENUE_ID"})
    # Get which sector each establishment belongs to
    gdf_denue["codigo_act"] = gdf_denue["codigo_act"].astype(str)
    gdf_denue["amenity"] = None
    for item in DENUE_TO_AMENITY_MAPPING:
        gdf_denue.loc[gdf_denue.eval(item["query"]), "amenity"] = item["name"]
    gdf_denue = gdf_denue.dropna(subset=["amenity"])
    gdf_denue = gdf_denue[
        ["DENUE_ID", "geometry", "amenity", "nom_estab", "codigo_act", "num_workers"]
    ]
    # gdf_denue['date'] = pd.to_datetime(gdf_denue['fecha_alta'], format='%Y-%m')
    # gdf_denue['year'] = gdf_denue['date'].dt.year

    gdf_lots, gdf_denue = assign_by_proximity(gdf_lots, gdf_denue)
    gdf_lots.to_file(args.output_file)
    gdf_denue.to_file(args.establishments_file)

    if args.view:
        gdf_denue.plot(column="amenity", legend=True, alpha=0.5)
        plt.show()
