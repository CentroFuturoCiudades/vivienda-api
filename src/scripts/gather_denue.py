import argparse
import re
import zipfile
from io import BytesIO

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests
from geopandas import GeoDataFrame

from src.scripts.utils.constants import BOUNDS_FILE, ESTABLISHMENTS_FILE, CSV_DENUE, DENUE, REGEX_PPL, WALK_RADIUS
from src.scripts.utils.utils import fill, fit_to_boundaries, map_sector_to_sector


def get_denue(gdf_bounds: GeoDataFrame, state_code: str, city_name: str) -> GeoDataFrame:
    response = requests.get(DENUE.format(state_code))
    zip_content = BytesIO(response.content)
    with zipfile.ZipFile(zip_content) as zip_ref:
        zip_ref.extractall("temp_data")
    df_denue = pd.read_csv(CSV_DENUE.format(state_code), encoding="latin-1")
    geometry_denue = gpd.points_from_xy(df_denue.longitud, df_denue.latitud)
    gdf_denue = gpd.GeoDataFrame(
        df_denue, geometry=geometry_denue, crs="EPSG:4326")
    if gdf_denue:
        gdf_denue = gdf_denue[gdf_denue.geometry.within(gdf_bounds.unary_union)]
    # _gdf_bounds = (
    #     gdf_bounds.to_crs("EPSG:32614").buffer(WALK_RADIUS).to_crs("EPSG:4326")
    # )
    # gdf_denue = fit_to_boundaries(gdf_denue, _gdf_bounds.unary_union)
    gdf_denue = gdf_denue[gdf_denue["municipio"] == city_name]
    gdf_denue = fill(
        gdf_denue, ["cve_ent", "cve_mun", "cve_loc", "ageb", "manzana"])
    gdf_denue["per_ocu"] = gdf_denue["per_ocu"].map(
        lambda x: re.match(REGEX_PPL, x.strip())
    )
    gdf_denue["per_ocu"] = gdf_denue["per_ocu"].map(
        lambda x: (int(x.groups()[0]) + int(x.groups()[1])) / 2 if x else x
    )
    gdf_denue = gdf_denue.dropna(subset=["per_ocu"])
    gdf_denue["sector"] = (
        gdf_denue["codigo_act"].apply(lambda x: str(x)[:2]).astype(int)
    )
    gdf_denue["sector"] = gdf_denue["sector"].apply(map_sector_to_sector)
    gdf_denue = gdf_denue.rename(columns={"per_ocu": "num_workers"})
    return gdf_denue


def get_args():
    parser = argparse.ArgumentParser(
        description="Join establishments with lots")
    parser.add_argument("input_dir", type=str,
                        help="The folder all the original data")
    parser.add_argument("output_dir", type=str,
                        help="The folder to save the output data")
    parser.add_argument("state_code", type=int, help="The state code")
    parser.add_argument("city_name", type=str, help="The city name")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    gdf_bounds = gpd.read_file(
        f"{args.input_dir}/{BOUNDS_FILE}", crs="EPSG:4326")
    gdf_denue = get_denue(gdf_bounds, args.state_code, args.city_name)
    gdf_denue.to_file(f"{args.output_dir}/{ESTABLISHMENTS_FILE}")
    if args.view:
        gdf_denue.plot(column="sector", legend=True, markersize=1)
        plt.show()
