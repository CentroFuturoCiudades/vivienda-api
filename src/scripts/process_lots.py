import argparse
import shutil
import zipfile
from io import BytesIO

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests

from utils.constants import CSV_PATH_MZA_2020, KEEP_COLUMNS, URL_MZA_2020, MAPPING_SCORE_VARS
from utils.utils import normalize


def gather_data(state_code: int) -> pd.DataFrame:
    response = requests.get(URL_MZA_2020.format(state_code))
    zip_content = BytesIO(response.content)
    with zipfile.ZipFile(zip_content) as zip_ref:
        zip_ref.extractall("temp_data")
    df = pd.read_csv(CSV_PATH_MZA_2020.format(state_code))
    shutil.rmtree("temp_data")
    df = df.loc[
        (df["ENTIDAD"].astype(str).str.zfill(2) != "00")
        & (df["MUN"].astype(str).str.zfill(3) != "000")
        & (df["LOC"].astype(str).str.zfill(4) != "0000")
        & (df["AGEB"].astype(str).str.zfill(4) != "0000")
        & (df["MZA"].astype(str).str.zfill(3) != "000")
    ]
    df["CVEGEO"] = (
        df["ENTIDAD"].astype(str).str.zfill(2)
        + df["MUN"].astype(str).str.zfill(3)
        + df["LOC"].astype(str).str.zfill(4)
        + df["AGEB"].astype(str).str.zfill(4)
        + df["MZA"].astype(str).str.zfill(3)
    )
    return df


def process_blocks(
    gdf_bounds: gpd.GeoDataFrame,
    gdf_blocks: gpd.GeoDataFrame,
    gdf_lots: gpd.GeoDataFrame,
    state_code: int,
) -> gpd.GeoDataFrame:

    gdf_blocks = gdf_blocks.drop_duplicates(subset="CVEGEO")
    gdf_blocks = gdf_blocks[["CVEGEO", "geometry"]]
    gdf_blocks["block_area"] = gdf_blocks.to_crs("EPSG:6933").area / 10_000
    gdf_blocks = gpd.sjoin(gdf_blocks, gdf_bounds, predicate="intersects").drop(
        columns=["index_right"]
    )

    ids_to_remove = [
        '207583',
        '0',
        '477',
    ]
    gdf_lots = gdf_lots[~gdf_lots['ID'].isin(ids_to_remove)]
    gdf_lots = gdf_lots[gdf_lots['geometry'].is_valid]
    gdf_lots = gdf_lots.dissolve(by='ID', aggfunc='first')
    
    gdf_lots = gpd.sjoin(gdf_lots, gdf_blocks, how="left", predicate="intersects")
    gdf_lots = (
        gdf_lots.groupby("ID")
        .agg({"block_area": "first", "geometry": "first", "CVEGEO": "first"})
        .reset_index()
    )
    gdf_lots = gpd.GeoDataFrame(gdf_lots, crs="EPSG:4326")
    rest_gdf_blocks = gdf_blocks[~gdf_blocks["CVEGEO"].isin(gdf_lots["CVEGEO"])]
    _gdf_lots = gdf_lots[~gdf_lots["geometry"].intersects(rest_gdf_blocks.unary_union)]
    gdf_lots = gpd.GeoDataFrame(
        pd.concat([_gdf_lots, rest_gdf_blocks], ignore_index=True)
    )
    gdf_lots['ID'] = gdf_lots['ID'].fillna(method='ffill')

    df = gather_data(state_code)
    df = df[["CVEGEO", *KEEP_COLUMNS]].set_index("CVEGEO")
    df = df.apply(pd.to_numeric, errors="coerce").fillna(0)
    gdf_lots = gdf_lots.merge(df, on="CVEGEO", how="inner")
    gdf_lots['P_25A59_F'] = gdf_lots['POBFEM'] - gdf_lots['P_0A2_F'] - gdf_lots['P_3A5_F'] - gdf_lots['P_6A11_F'] - gdf_lots['P_12A14_F'] - gdf_lots['P_15A17_F'] - gdf_lots['P_18A24_F'] - gdf_lots['P_60YMAS_F']
    gdf_lots['P_25A59_M'] = gdf_lots['POBMAS'] - gdf_lots['P_0A2_M'] - gdf_lots['P_3A5_M'] - gdf_lots['P_6A11_M'] - gdf_lots['P_12A14_M'] - gdf_lots['P_15A17_M'] - gdf_lots['P_18A24_M'] - gdf_lots['P_60YMAS_M']
    gdf_lots['P_25A59'] = gdf_lots['POBTOT'] - gdf_lots['P_0A2'] - gdf_lots['P_3A5'] - gdf_lots['P_6A11'] - gdf_lots['P_12A14'] - gdf_lots['P_15A17'] - gdf_lots['P_18A24'] - gdf_lots['P_60YMAS']
    total_score = sum(MAPPING_SCORE_VARS.values())
    gdf_lots['puntuaje_hogar_digno'] = 0
    for key, value in MAPPING_SCORE_VARS.items():
        gdf_lots['puntuaje_hogar_digno'] += gdf_lots[key] * value
        gdf_lots['puntuaje_hogar_digno'] = gdf_lots['puntuaje_hogar_digno'] / (gdf_lots['TVIVPARHAB'] * total_score)
    gdf_lots['puntuaje_hogar_digno'] = normalize(gdf_lots['puntuaje_hogar_digno'])

    gdf_lots['total_cuartos'] = gdf_lots['VPH_1CUART'] + (gdf_lots['VPH_2CUART'] * 2) + (gdf_lots['VPH_3YMASC'] * 3)
    gdf_lots['total_cuartos'] = gdf_lots['total_cuartos'].fillna(0)
    gdf_lots['pob_por_cuarto'] = gdf_lots.apply(lambda x: x['POBTOT'] / x['total_cuartos'] if x['total_cuartos'] > 0 else 0, axis=1)
    return gdf_lots


def get_args():
    parser = argparse.ArgumentParser(description="Join establishments with lots")
    parser.add_argument("bounds_file", type=str, help="The file with all the data")
    parser.add_argument(
        "blocks_file", type=str, help="The file with the bounds of the area"
    )
    parser.add_argument(
        "lots_file", type=str, help="The file with the bounds of the area"
    )
    parser.add_argument(
        "output_file", type=str, help="The file with the bounds of the area"
    )
    parser.add_argument("state_code", type=int, help="The state code")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    df = gather_data(args.state_code)

    gdf_bounds = gpd.read_file(args.bounds_file, crs="EPSG:4326")
    gdf_bounds = gdf_bounds[["geometry"]]
    gdf_blocks = gpd.read_file(args.blocks_file, crs="EPSG:4326")
    gdf_lots = gpd.read_file(args.lots_file, crs="EPSG:4326")
    gdf_lots = gdf_lots[["ID", "geometry"]]

    gdf_lots = process_blocks(gdf_bounds, gdf_blocks, gdf_lots, args.state_code)
    gdf_lots.set_index("ID").to_file(args.output_file)
    if args.view:
        gdf_lots.plot(column="P_60YMAS_F", legend=True, markersize=1, alpha=0.5)
        plt.show()
