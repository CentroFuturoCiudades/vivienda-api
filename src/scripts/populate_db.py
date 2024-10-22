import argparse
import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.types import Float
import psycopg2

from src.utils.db import get_engine


def get_args():
    parser = argparse.ArgumentParser(description="Join establishments with lots")
    parser.add_argument("-l","--lots_file", required=False, type=str, help="The file with all the lots data")
    parser.add_argument("-b", "--blocks_file", required=False, type=str, help="The file with all the blocks data")
    parser.add_argument("-a", "--accessibility_file", required=False, type=str, help="The file with all the accessibility data")
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
            df = df.drop(columns=[column])
    
    return df

if __name__ == "__main__":
    args = get_args()

    mapping_lots = {
        "cvegeo": str,
        "lot_area": float,
        "num_workers": int,
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
        "wasteful_area": float,
        "wasteful_ratio": float,
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
        "num_establishments": int,
        "num_properties": int,
        "num_floors": float,
        "max_height": float,
        "diff_height": float,
        "max_home_units": int,
        "units_estimate": int,
        "potential_new_units": int,
        "mean_slope": float,
    }
    mapping_blocks = {
        'block_area': float,
        'pobtot': int,
        'pobfem': int,
        'pobmas': int,
        'p_0a2': int,
        'p_0a2_f': int,
        'p_0a2_m': int,
        'p_3a5': int,
        'p_3a5_f': int,
        'p_3a5_m': int,
        'p_6a11': int,
        'p_6a11_f': int,
        'p_6a11_m': int,
        'p_12a14': int,
        'p_12a14_f': int,
        'p_12a14_m': int,
        'p_15a17': int,
        'p_15a17_f': int,
        'p_15a17_m': int,
        'p_18a24': int,
        'p_18a24_f': int,
        'p_18a24_m': int,
        'p_60ymas': int,
        'p_60ymas_f': int,
        'p_60ymas_m': int,
        'pea': int,
        'pe_inac': int,
        'pocupada': int,
        'pdesocup': int,
        'vivtot': int,
        'tvivhab': int,
        'tvivpar': int,
        'vivpar_hab': int,
        'vivpar_des': int,
        'ocupvivpar': int,
        'prom_ocup': float,
        'pro_ocup_c': float,
        'tvivparhab': int,
        'vph_1cuart': int,
        'vph_2cuart': int,
        'vph_3ymasc': int,
        'vph_pisodt': int,
        'vph_tinaco': int,
        'vph_excsa': int,
        'vph_drenaj': int,
        'vph_c_serv': int,
        'vph_refri': int,
        'vph_lavad': int,
        'vph_autom': int,
        'vph_tv': int,
        'vph_pc': int,
        'vph_telef': int,
        'vph_cel': int,
        'vph_inter': int,
        'vph_stvp': int,
        'vph_spmvpi': int,
        'vph_cvj': int,
        'pafil_ipriv': int,
        'graproes': float,
        'pcatolica': int,
        'pro_crieva': int,
        'potras_rel': int,
        'psin_relig': int,
        'block_area': int,
        'p_25a59_f': int,
        'p_25a59_m': int,
        'p_25a59': int,
        'puntuaje_hogar_digno': float,
        'total_cuartos': int,
        'pob_por_cuarto': float,
        'accessibility_score': float,
        'minutes': float,
        'latitud': float,
        'longitud': float,
        "node_ids": int,
    }
    mapping_trips = {
        "origin_id": int,
        "destination_id": int,
        "num_amenity": int,
        "amenity": str,
        "distance": float,
        "minutes": float,
        "gravity": float,
        "population": int,
        "attraction": float,
        "pob_reach": float,
        "accessibility_score": float,
        "node_ids": str,
    }

    engine = get_engine()

    if args.lots_file:
        df_lots = pd.read_csv(args.lots_file)
        df_lots = df_lots.set_index("lot_id")
        df_lots = clean_and_cast_types(df_lots, mapping_lots)
        df_lots.to_sql(
            "lots",
            engine,
            if_exists="append",
            index_label="lot_id",
        )

        data = pd.read_sql_query("SELECT * FROM lots", engine)
        print(data)

    if args.blocks_file:
        df_blocks = pd.read_csv(args.blocks_file)
        df_blocks = df_blocks.set_index("cvegeo")
        df_blocks = clean_and_cast_types(df_blocks, mapping_blocks)
        df_blocks.to_sql(
            "blocks",
            engine,
            if_exists="append",
            index_label="cvegeo",
        )

        data = pd.read_sql_query("SELECT * FROM blocks", engine)
        print(data)

    if args.accessibility_file:
        df_accessibility = pd.read_csv(args.accessibility_file)
        df_accessibility = df_accessibility.set_index("origin_id")
        df_accessibility = clean_and_cast_types(df_accessibility, mapping_trips)
        df_accessibility.to_sql(
            "accessibility_trips",
            engine,
            if_exists="append",
        )

        data = pd.read_sql_query("SELECT * FROM accessibility_trips", engine)
        print(data)
