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

from src.scripts.utils.constants import CSV_PATH_MZA_2020, KEEP_COLUMNS, URL_MZA_2020, MAPPING_SCORE_VARS
from src.scripts.utils.utils import normalize


INEGI_GEO_BLOCKS_URL = 'https://www.inegi.org.mx/contenidos/productos/prod_serv/contenidos/espanol/bvinegi/productos/geografia/rural/SHP_2023/{state}/{municipality_code}_s.zip'
INEGI_GEO_BLOCKS_REGEX = r'.*/conjunto_de_datos/\d+m\.(shp|shx|dbf|prj)'

def download_blocks(state, municipality_code, path='.'):
  url = INEGI_GEO_BLOCKS_URL.format(state=state, municipality_code=municipality_code)
  print(url)
  
  # Download the zip file from the URL
  response = requests.get(url)
  
  # Use a temporary folder for extracting files
  tmp_folder = os.path.join(path, 'tmp')
  os.makedirs(tmp_folder, exist_ok=True)
  
  # Open the zip file in memory
  with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    # Get the list of files matching the shapefile-related extensions
    files = z.namelist()
    files = [f for f in files if re.match(INEGI_GEO_BLOCKS_REGEX, f)]
    
    # Raise an error if no relevant files are found
    if len(files) == 0:
        raise ValueError('No shapefiles found in zip file')
    
    # Extract only the necessary files
    for f in files:
      filename = os.path.basename(f)
      z.extract(f, tmp_folder)
      os.rename(os.path.join(tmp_folder, f), os.path.join(tmp_folder, filename))
    
    # Extract the base filename for the shapefile (without extension)
    filename = os.path.basename(files[0])
    filename = os.path.splitext(filename)[0]

    # Load the shapefile into a GeoDataFrame
    gdf = gpd.read_file(os.path.join(tmp_folder, f'{filename}.shp')).to_crs('EPSG:4326')

    # Define the output folder and file path
    output_file = os.path.join(path, f'{municipality_code}.fgb')

    # Save the GeoDataFrame as an FGB file
    gdf.to_file(output_file, driver='FlatGeobuf')
  
  # Clean up the temporary folder
  shutil.rmtree(tmp_folder)
  
  return output_file

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

def gather_blocks(state_code: int, state_name: str, municipality_code: int) -> gpd.GeoDataFrame:
    df_blocks = gather_data(state_code)
    df_blocks = df_blocks.set_index('CVEGEO')
    df_blocks = df_blocks.apply(pd.to_numeric, errors='coerce').fillna(0)
    
    vect_file = download_blocks(state_name, municipality_code)
    gdf_vector_blocks = gpd.read_file(vect_file, engine='pyogrio').set_index('CVEGEO')
    gdf_vector_blocks = gdf_vector_blocks[['geometry']]
    os.remove(vect_file)
    
    gdf_blocks = gdf_vector_blocks.merge(df_blocks, left_index=True, right_index=True)
    gdf_blocks = gpd.GeoDataFrame(gdf_blocks, geometry='geometry')
    return gdf_blocks


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
    gdf_blocks = gather_blocks(19, 'Nuevo_Leon', '794551077313')
    print(gdf_blocks)
    exit()
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
