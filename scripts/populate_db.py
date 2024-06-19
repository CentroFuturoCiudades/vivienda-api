import argparse
import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pyogrio import read_dataframe
from sqlalchemy import create_engine

load_dotenv()


def get_args():
    parser = argparse.ArgumentParser(description="Join establishments with lots")
    parser.add_argument("lots_file", type=str, help="The file with all the data")
    parser.add_argument("sql_file", type=str, help="The file with all the data")
    parser.add_argument("region", type=str, help="The file with all the data")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USERNAME")
    password = os.getenv("SQL_PASSWORD")
    driver = os.getenv("SQL_DRIVER")
    connection_string = f"DRIVER={driver};SERVER=tcp:{server},1433;DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={connection_string}", fast_executemany=True
    )

    mapping = {
        "ID": str,
        "block_area": float,
        "CVEGEO": str,
        "POBTOT": int,
        "POBMAS": int,
        "POBFEM": int,
        "P_15A17": int,
        "P_18A24": int,
        "PEA": int,
        "PE_INAC": int,
        "POCUPADA": int,
        "PDESOCUP": int,
        "VIVTOT": int,
        "TVIVHAB": int,
        "TVIVPAR": int,
        "VIVPAR_HAB": int,
        "VIVPAR_DES": int,
        "VPH_AUTOM": int,
        "OCUPVIVPAR": int,
        "PROM_OCUP": float,
        "PRO_OCUP_C": float,
        "P_0A5": int,
        "P_6A14": int,
        "P_25A64": int,
        "P_65MAS": int,
        "area": float,
        "num_establishments": int,
        "num_workers": int,
        "supermercado": int,
        "salud": int,
        "educacion": int,
        "servicios": int,
        "lot_area": float,
        "unused_area": float,
        "unused_ratio": float,
        "green_area": float,
        "green_ratio": float,
        "parking_area": float,
        "parking_ratio": float,
        "park_area": float,
        "park_ratio": float,
        "equipment_area": float,
        "equipment_ratio": float,
        "building_area": float,
        "building_ratio": float,
        "proximity_age_diversity": float,
        "accessibility": float,
        "minutes_proximity_big_park": float,
        "minutes_proximity_small_park": float,
        "minutes_proximity_salud": float,
        "minutes_proximity_educacion": float,
        "minutes_proximity_servicios": float,
        "minutes_proximity_supermercado": float,
        "minutes_proximity_age_diversity": float,
        "minutes": float,
        "num_properties": int,
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
        "num_floors": int,
        "max_COS": float,
        "max_CUS": float,
        "min_CAV": float,
        "max_height": int,
        "diff_height": int,
        "max_home_units": int,
        "building_volume": float,
        "building_volume_block": float,
        "units_per_built_area": float,
        "units_estimate": int,
        "potential_new_units": int,
        "optimal_CAV": float,
        "diff_CAV": float,
    }
    df = read_dataframe(args.lots_file)
    df = df.replace({np.nan: None})
    df = pd.DataFrame(df.drop(columns=["geometry"]))
    for column in df.columns:
        if column in mapping:
            if mapping[column] == str:
                df[column] = df[column].astype(str)
            elif mapping[column] == int:
                df[column] = pd.to_numeric(
                    df[column], errors="coerce", downcast="integer"
                )
            elif mapping[column] == float:
                df[column] = pd.to_numeric(df[column], errors="coerce")
    df["region"] = args.region

    df.to_sql(
        "lots",
        engine,
        if_exists="replace",
        index=False,
        index_label="ID",
        chunksize=5000,
    )
