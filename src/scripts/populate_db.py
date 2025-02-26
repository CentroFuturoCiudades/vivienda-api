import argparse
import os

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.types import Float
import psycopg2
from tqdm import tqdm
from sqlalchemy import MetaData, Table

from src.utils.db import get_engine


def get_args():
    parser = argparse.ArgumentParser(
        description="Join establishments with lots")
    parser.add_argument("-l", "--lots_file", required=False,
                        type=str, help="The file with all the lots data")
    parser.add_argument("-b", "--blocks_file", required=False,
                        type=str, help="The file with all the blocks data")
    parser.add_argument("-a", "--accessibility_file", required=False,
                        type=str, help="The file with all the accessibility data")
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
            if mapping[column] in ["object", str]:
                df[column] = df[column].astype(str)
            elif mapping[column] in ["int64", "int", int]:
                # Coerce non-integer values to NaN and downcast
                df[column] = pd.to_numeric(
                    df[column], errors="coerce", downcast="integer")
            elif mapping[column] in ["float64", "float", float]:
                # Coerce non-float values to NaN
                df[column] = pd.to_numeric(df[column], errors="coerce")
                # Round float columns to 6 decimal places to avoid precision issues
                df[column] = df[column].round(6)

                # Check for non-float values in float columns for debugging
                invalid_floats = df[column][~df[column].apply(
                    lambda x: isinstance(x, (float, int, type(None))))]
                if not invalid_floats.empty:
                    print(f"Invalid float values found in column {column}:")
                    print(invalid_floats)
        # drop columns not in mapping
        else:
            df = df.drop(columns=[column])

    return df


def process_in_chunks(file, table_name, engine, index_column, mapping, chunk_size=5000):
    # Initialize a chunk iterator
    metadata = MetaData()
    # drop only if table exists
    if inspect(engine).has_table(table_name):
        _Table = Table(table_name, metadata, autoload_with=engine)
        _Table.drop(engine)
    chunks = pd.read_csv(file, chunksize=chunk_size, dtype=mapping)

    # use progress bar
    for chunk in tqdm(chunks):

        # Set the index for the chunk
        chunk = chunk.set_index(index_column)

        # Clean and cast types as needed
        chunk = clean_and_cast_types(chunk, mapping)

        # Append the chunk to the table in the database
        chunk.to_sql(
            table_name,
            engine,
            if_exists="append",
            index_label=index_column
        )

    # Query and print the full data from the table
    data = pd.read_sql_query(f"SELECT * FROM {table_name}", engine)
    print(data)
    print(data.columns.tolist())


if __name__ == "__main__":
    args = get_args()

    mapping_lots = {
        'lot_id': 'int64',
        'zoning': 'object',
        'num_levels': 'float64',
        'cvegeo': 'object',
        'num_establishments': 'float64',
        'num_workers': 'float64',
        'lot_area': 'float64',
        'unused_area': 'float64',
        'unused_ratio': 'float64',
        'green_area': 'float64',
        'green_ratio': 'float64',
        'parking_area': 'float64',
        'parking_ratio': 'float64',
        'building_ratio': 'float64',
        'num_properties': 'float64',
        'wasteful_area': 'float64',
        'wasteful_ratio': 'float64',
        'occupancy': 'float64',
        'underutilized_area': 'float64',
        'underutilized_ratio': 'float64',
        'occupancy_density': 'float64',
        'home_density': 'float64',
        'latitud': 'float64',
        'longitud': 'float64',
        'max_cos': 'float64',
        'max_building_area': 'float64',
        'max_cus': 'float64',
        'max_builtup_area': 'float64',
        'min_cav': 'float64',
        'max_num_levels': 'float64',
        'max_density': 'float64',
        'max_home_units': 'int64',
        'building_area': 'float64',
        'cos': 'float64',
        'builtup_area': 'float64',
        'cus': 'float64',
        'block_cus': 'float64',
        'home_units': 'float64',
        'density': 'float64',
        'potential_home_units': 'float64',
        'mean_slope': 'float64',
        'geometry': 'object',
    }
    mapping_blocks = {
        "cvegeo": "object",
        "block_area": "float64",
        "pobtot": "int64",
        "pobfem": "int64",
        "pobmas": "int64",
        "p_0a2": "int64",
        "p_0a2_f": "int64",
        "p_0a2_m": "int64",
        "p_3a5": "int64",
        "p_3a5_f": "int64",
        "p_3a5_m": "int64",
        "p_6a11": "int64",
        "p_6a11_f": "int64",
        "p_6a11_m": "int64",
        "p_12a14": "int64",
        "p_12a14_f": "int64",
        "p_12a14_m": "int64",
        "p_15a17": "int64",
        "p_15a17_f": "int64",
        "p_15a17_m": "int64",
        "p_18a24": "int64",
        "p_18a24_f": "int64",
        "p_18a24_m": "int64",
        "p_25a59": "int64",
        "p_25a59_f": "int64",
        "p_25a59_m": "int64",
        "p_60ymas": "int64",
        "p_60ymas_f": "int64",
        "p_60ymas_m": "int64",
        "pea": "int64",
        "pe_inac": "int64",
        "pocupada": "int64",
        "pdesocup": "int64",
        "vivtot": "int64",
        "tvivhab": "int64",
        "tvivpar": "int64",
        "vivpar_hab": "int64",
        "vivpar_des": "int64",
        "ocupvivpar": "int64",
        "prom_ocup": "float64",
        "pro_ocup_c": "float64",
        "tvivparhab": "int64",
        "vivparh_cv": "int64",
        "vph_1cuart": "int64",
        "vph_2cuart": "int64",
        "vph_3ymasc": "int64",
        "vph_pisodt": "int64",
        "vph_tinaco": "int64",
        "vph_excsa": "int64",
        "vph_drenaj": "int64",
        "vph_c_serv": "int64",
        "vph_refri": "int64",
        "vph_lavad": "int64",
        "vph_autom": "int64",
        "vph_pc": "int64",
        "graproes": "float64",
        "puntuaje_hogar_digno": "float64",
        "total_cuartos": "int64",
        "pob_por_cuarto": "float64",
        "accessibility_score": "float64",
        "minutes": "float64",
        "node_ids": "int64",
        "latitud": "float64",
        "longitud": "float64",
        'geometry': 'object',
    }
    mapping_trips = {
        "origin_id": "int64",
        "destination_id": "int64",
        "num_amenity": "int64",
        "amenity": "object",
        "distance": "float64",
        "minutes": "float64",
        "gravity": "float64",
        "population": "float64",
        "attraction": "float64",
        "pob_reach": "float64",
    }

    engine = get_engine()

    if args.lots_file:
        process_in_chunks(args.lots_file, "lots", engine,
                          index_column="lot_id", mapping=mapping_lots)

    if args.blocks_file:
        process_in_chunks(args.blocks_file, "blocks", engine,
                          index_column="cvegeo", mapping=mapping_blocks)

    if args.accessibility_file:
        process_in_chunks(args.accessibility_file, "accessibility_trips",
                          engine, index_column="origin_id", mapping=mapping_trips)
