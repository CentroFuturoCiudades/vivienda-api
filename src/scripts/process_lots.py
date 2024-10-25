import argparse
import shutil
import zipfile
from io import BytesIO

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import requests
import os
import io
import re
from sklearn.preprocessing import MinMaxScaler

from src.scripts.utils.constants import CSV_PATH_MZA_2020, KEEP_COLUMNS, URL_MZA_2020, MAPPING_SCORE_VARS, BOUNDS_FILE, PROCESSED_BLOCKS_FILE
from src.scripts.utils.utils import normalize


INEGI_GEO_BLOCKS_URL = 'https://www.inegi.org.mx/contenidos/productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/rural/SHP_2023/{state}/{municipality_code}_s.zip'
INEGI_GEO_BLOCKS_REGEX = r'.*/conjunto_de_datos/\d+m\.(shp|shx|dbf|prj)'


def download_blocks_polygons(state, municipality_code, path='.'):
    output_file = os.path.join(path, f'{municipality_code}.fgb')
    if os.path.exists(output_file):
        return output_file
    url = INEGI_GEO_BLOCKS_URL.format(
        state=state, municipality_code=municipality_code)
    response = requests.get(url)

    tmp_folder = os.path.join(path, 'tmp')
    os.makedirs(tmp_folder, exist_ok=True)

    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
        files = z.namelist()
        files = [f for f in files if re.match(INEGI_GEO_BLOCKS_REGEX, f)]

        if len(files) == 0:
            raise ValueError('No shapefiles found in zip file')

        for f in files:
            filename = os.path.basename(f)
            z.extract(f, tmp_folder)
            os.rename(os.path.join(tmp_folder, f),
                      os.path.join(tmp_folder, filename))

        filename = os.path.basename(files[0])
        filename = os.path.splitext(filename)[0]

        gdf = gpd.read_file(os.path.join(
            tmp_folder, f'{filename}.shp')).to_crs('EPSG:4326')
        gdf.to_file(output_file, driver='FlatGeobuf')

    shutil.rmtree(tmp_folder)
    return output_file


def gather_inegi_blocks(state_code: int) -> pd.DataFrame:
    response = requests.get(URL_MZA_2020.format(state_code))
    zip_content = BytesIO(response.content)
    with zipfile.ZipFile(zip_content) as zip_ref:
        zip_ref.extractall("temp_data")
    df = pd.read_csv(CSV_PATH_MZA_2020.format(state_code))
    df.columns = df.columns.str.lower()
    shutil.rmtree("temp_data")
    df = df.loc[
        (df["entidad"].astype(str).str.zfill(2) != "00")
        & (df["mun"].astype(str).str.zfill(3) != "000")
        & (df["loc"].astype(str).str.zfill(4) != "0000")
        & (df["ageb"].astype(str).str.zfill(4) != "0000")
        & (df["mza"].astype(str).str.zfill(3) != "000")
    ]
    df["cvegeo"] = (
        df["entidad"].astype(str).str.zfill(2)
        + df["mun"].astype(str).str.zfill(3)
        + df["loc"].astype(str).str.zfill(4)
        + df["ageb"].astype(str).str.zfill(4)
        + df["mza"].astype(str).str.zfill(3)
    )
    return df


def gather_blocks(state_code: int, state_name: str, municipality_code: int) -> gpd.GeoDataFrame:
    df_inegi_blocks = gather_inegi_blocks(state_code)
    df_inegi_blocks = df_inegi_blocks.set_index('cvegeo')
    df_inegi_blocks = df_inegi_blocks.apply(
        pd.to_numeric, errors='coerce').fillna(0)

    vect_file = download_blocks_polygons(state_name, municipality_code)
    gdf_vector_blocks = gpd.read_file(
        vect_file, engine='pyogrio')
    gdf_vector_blocks.columns = gdf_vector_blocks.columns.str.lower()
    gdf_vector_blocks = gdf_vector_blocks.set_index('cvegeo')
    gdf_vector_blocks = gdf_vector_blocks[['geometry']]

    gdf_blocks = gdf_vector_blocks.merge(
        df_inegi_blocks, left_index=True, right_index=True)
    gdf_blocks = gpd.GeoDataFrame(gdf_blocks, geometry='geometry')
    return gdf_blocks


def process_blocks(gdf_blocks: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    # gdf_blocks.columns = gdf_blocks.columns.str.lower()
    gdf_blocks[KEEP_COLUMNS] = gdf_blocks[KEEP_COLUMNS].apply(
        pd.to_numeric, errors="coerce").fillna(0)
    gdf_blocks = gdf_blocks[[*KEEP_COLUMNS, 'geometry']]
    gdf_blocks["block_area"] = gdf_blocks.to_crs("EPSG:6933").area / 10_000
    gdf_blocks['p_25a59_f'] = gdf_blocks['pobfem'] - gdf_blocks['p_0a2_f'] - gdf_blocks['p_3a5_f'] - gdf_blocks['p_6a11_f'] - \
        gdf_blocks['p_12a14_f'] - gdf_blocks['p_15a17_f'] - \
        gdf_blocks['p_18a24_f'] - gdf_blocks['p_60ymas_f']
    gdf_blocks['p_25a59_m'] = gdf_blocks['pobmas'] - gdf_blocks['p_0a2_m'] - gdf_blocks['p_3a5_m'] - gdf_blocks['p_6a11_m'] - \
        gdf_blocks['p_12a14_m'] - gdf_blocks['p_15a17_m'] - \
        gdf_blocks['p_18a24_m'] - gdf_blocks['p_60ymas_m']
    gdf_blocks['p_25a59'] = gdf_blocks['pobtot'] - gdf_blocks['p_0a2'] - gdf_blocks['p_3a5'] - gdf_blocks['p_6a11'] - \
        gdf_blocks['p_12a14'] - gdf_blocks['p_15a17'] - \
        gdf_blocks['p_18a24'] - gdf_blocks['p_60ymas']

    gdf_blocks['puntuaje_hogar_digno'] = 0
    for key, value in MAPPING_SCORE_VARS.items():
        gdf_blocks['puntuaje_hogar_digno'] = gdf_blocks['puntuaje_hogar_digno'] + (gdf_blocks[key] / gdf_blocks['tvivparhab']) * value
    gdf_blocks['puntuaje_hogar_digno'] = (gdf_blocks['puntuaje_hogar_digno'] - gdf_blocks['puntuaje_hogar_digno'].min()) / (gdf_blocks['puntuaje_hogar_digno'].max() - gdf_blocks['puntuaje_hogar_digno'].min())

    gdf_blocks['total_cuartos'] = gdf_blocks['vph_1cuart'] + \
        (gdf_blocks['vph_2cuart'] * 2) + (gdf_blocks['vph_3ymasc'] * 3)
    gdf_blocks['total_cuartos'] = gdf_blocks['total_cuartos'].fillna(0)
    gdf_blocks['pob_por_cuarto'] = gdf_blocks.apply(
        lambda x: x['pobtot'] / x['total_cuartos'] if x['total_cuartos'] > 0 else 0, axis=1)

    return gdf_blocks


def get_args():
    parser = argparse.ArgumentParser(
        description="Join establishments with lots")
    parser.add_argument("input_dir", type=str,
                        help="The folder all the original data")
    parser.add_argument("output_dir", type=str,
                        help="The folder to save the output data")
    parser.add_argument("state_code", type=int, help="The code of the state")
    parser.add_argument("state_name", type=str, help="The name of the state")
    parser.add_argument("city_code", type=str, help="The code of the city")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    gdf_bounds = gpd.read_file(
        f"{args.input_dir}/{BOUNDS_FILE}", crs="EPSG:4326")
    gdf_bounds = gdf_bounds[["geometry"]]

    gdf_blocks = gather_blocks(
        args.state_code, args.state_name, args.city_code)
    gdf_blocks = process_blocks(gdf_blocks)

    gdf_blocks.to_file(f"{args.output_dir}/{PROCESSED_BLOCKS_FILE}")

    if args.view:
        gdf_blocks.plot(column="puntuaje_hogar_digno", legend=True,
                        markersize=1, alpha=0.5)
        plt.show()
