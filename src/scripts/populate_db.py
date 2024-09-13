import argparse
import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pyogrio import read_dataframe
from sqlalchemy import create_engine, text
from sqlalchemy.types import Float
import psycopg2

load_dotenv()


def get_args():
    parser = argparse.ArgumentParser(description="Join establishments with lots")
    parser.add_argument("lots_file", type=str, help="The file with all the data")
    parser.add_argument("sql_file", type=str, help="The file with all the data")
    parser.add_argument("region", type=str, help="The file with all the data")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()

def clean_and_cast_types(df, mapping):
    # Convert column names to lowercase
    df.columns = [col.lower() for col in df.columns]
    mapping = {key.lower(): value for key, value in mapping.items()}
    
    # Replace NaN with None
    df = df.replace({np.nan: None})

    # Apply casting and handle problematic float values
    for column in df.columns:
        if column in mapping:
            if mapping[column] == str:
                df[column] = df[column].astype(str)
            elif mapping[column] == int:
                # Coerce non-integer values to NaN and downcast
                df[column] = pd.to_numeric(df[column], errors="coerce", downcast="integer")
            elif mapping[column] == float:
                # Coerce non-float values to NaN
                df[column] = pd.to_numeric(df[column], errors="coerce")
                # Round float columns to 6 decimal places to avoid precision issues
                df[column] = df[column].round(6)
                
                # Check for non-float values in float columns for debugging
                invalid_floats = df[column][~df[column].apply(lambda x: isinstance(x, (float, int, type(None))))]
                if not invalid_floats.empty:
                    print(f"Invalid float values found in column {column}:")
                    print(invalid_floats)
        # drop columns not in mapping
        else:
            print('Dropping', column)
            df = df.drop(columns=[column])
    
    return df

if __name__ == "__main__":
    args = get_args()

    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "reimagina_urbano_db")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")

    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    print(connection_string)
    engine = create_engine(connection_string)

    # do a select all to check if the table exists
    with engine.connect() as conn:
        # drop the table if it exists
        conn.execute(text(f"DROP TABLE IF EXISTS lots"))
        # query = conn.execute(text(f"SELECT POBTOT FROM lots LIMIT 1"))
        # query = query.fetchall()
        # print(query)
        # # get columns from the table
        # columns = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name = 'lots'"))
        # columns = columns.fetchall()
        # print(columns)
        # exit()

    mapping = {
        "block_area": float,
        "PROM_OCUP": float,
        "PRO_OCUP_C": float,
        "GRAPROES": float,
        "puntuaje_hogar_digno": float,
        "pob_por_cuarto": float,
        "area": float,
        "num_workers": int,
        "lot_area": float,
        "unused_area": float,
        "unused_ratio": float,
        "green_area": float,
        "green_ratio": float,
        "amenity_area": float,
        "amenity_ratio": float,
        "parking_area": float,
        "parking_ratio": float,
        "building_area": float,
        "building_ratio": float,
        "distance": float,
        "minutes": float,
        "gravity_score": float,
        "accessibility_score": float,
        "wasteful_area": float,
        "wasteful_ratio": float,
        "occupancy": int,
        "underutilized_area": float,
        "underutilized_ratio": float,
        "occupancy_density": float,
        "home_density": float,
        "combined_score": float,
        "latitud": float,
        "longitud": float,
        "zoning": str,
        "max_COS": float,
        "max_CUS": float,
        "min_CAV": float,
        "building_volume": float,
        "building_volume_block": float,
        "units_per_built_area": float,
        "population": int,
        "optimal_CAV": float,
        "diff_CAV": float,
        "ID": str,
        "CVEGEO": str,
        "POBTOT": int,
        "POBFEM": int,
        "POBMAS": int,
        "P_0A2": int,
        "P_0A2_F": int,
        "P_0A2_M": int,
        "P_3A5": int,
        "P_3A5_F": int,
        "P_3A5_M": int,
        "P_6A11": int,
        "P_6A11_F": int,
        "P_6A11_M": int,
        "P_12A14": int,
        "P_12A14_F": int,
        "P_12A14_M": int,
        "P_15A17": int,
        "P_15A17_F": int,
        "P_15A17_M": int,
        "P_18A24": int,
        "P_18A24_F": int,
        "P_18A24_M": int,
        "P_60YMAS": int,
        "P_60YMAS_F": int,
        "P_60YMAS_M": int,
        "PEA": int,
        "PE_INAC": int,
        "POCUPADA": int,
        "PDESOCUP": int,
        "VIVTOT": int,
        "TVIVHAB": int,
        "TVIVPAR": int,
        "VIVPAR_HAB": int,
        "VIVPAR_DES": int,
        "OCUPVIVPAR": int,
        "TVIVPARHAB": int,
        "VPH_1CUART": int,
        "VPH_2CUART": int,
        "VPH_3YMASC": int,
        "VPH_PISODT": int,
        "VPH_TINACO": int,
        "VPH_EXCSA": int,
        "VPH_DRENAJ": int,
        "VPH_C_SERV": int,
        "VPH_REFRI": int,
        "VPH_LAVAD": int,
        "VPH_AUTOM": int,
        "VPH_TV": int,
        "VPH_PC": int,
        "VPH_TELEF": int,
        "VPH_CEL": int,
        "VPH_INTER": int,
        "VPH_STVP": int,
        "VPH_SPMVPI": int,
        "VPH_CVJ": int,
        "PAFIL_IPRIV": int,
        "PCATOLICA": int,
        "PRO_CRIEVA": int,
        "POTRAS_REL": int,
        "PSIN_RELIG": int,
        "P_25A59_F": int,
        "P_25A59_M": int,
        "P_25A59": int,
        "total_cuartos": int,
        "num_establishments": int,
        "num_properties": int,
        "num_floors": float,
        "max_height": float,
        "diff_height": float,
        "max_home_units": int,
        "units_estimate": int,
        "potential_new_units": int,
    }
    df = read_dataframe(args.lots_file)
    # for x in range(1, 21):
    #     df = df.drop(columns=[f"distance_{x}", f"gravity_temp_{x}"])
    df = df.drop(columns=["geometry"])
    
    df = clean_and_cast_types(df, mapping)

    # df.to_sql(
    #     "lots",
    #     engine,
    #     if_exists="replace",
    #     index=False,
    #     index_label="ID",
    #     chunksize=5000,
    # )
    df.to_sql(
        "lots",
        engine,
        if_exists="replace",
        index=False,
        index_label="ID",
    )
