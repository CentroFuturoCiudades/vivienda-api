import io
import json
import os
import sqlite3
import tempfile
from typing import Annotated, Any, Dict, List

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import osmnx as ox
import pandana as pdna
import pandas as pd
import pyogrio
import requests
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from osmnx.distance import nearest_nodes
from pydantic import BaseModel
from shapely import Point, Polygon
from shapely.geometry import box
from sqlalchemy import create_engine

from src.scripts.accessibility import calculate_accessibility
from src.utils.files import get_file
from src.utils.utils import get_all
import time
import pickle
from shapely.geometry import box
import asyncio
from concurrent.futures import ThreadPoolExecutor
from src.utils.constants import AMENITIES_MAPPING
from functools import lru_cache

app = FastAPI()
FOLDER = "primavera"
PROJECTS_MAPPING = {"distritotec": "distritotec", "primavera": "primavera"}
WALK_RADIUS = 1609.34


BLOB_URL = "https://reimaginaurbanostorage.blob.core.windows.net"

# hello world
@app.get("/")
async def root():
    return {"message": "Hello World"}


def get_blob_url(file_name: str) -> str:
    access_token = os.getenv("BLOB_TOKEN")
    return f"{BLOB_URL}/{FOLDER}/{file_name}?{access_token}"


ORIGINS = [
    "*",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def read_gdf_sync(filepath, bbox=None):
    return gpd.read_file(filepath, bbox=bbox, engine="pyogrio")

async def read_gdf_async(filepath, bbox=None):
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        result = await loop.run_in_executor(pool, read_gdf_sync, filepath, bbox)
    return result

@app.get("/coords")
async def get_coordinates():
    gdf_bounds = await read_gdf_async(get_file("bounds.fgb"), None)
    geom = gdf_bounds.unary_union
    return {"latitud": geom.centroid.y, "longitud": geom.centroid.x}


@lru_cache()
def load_network():
    response = requests.get(get_blob_url("pedestrian_network.hd5"))
    response.raise_for_status()
    temp_dir = os.getenv("BASE_FILE_LOCATION", "./temp")
    os.makedirs(temp_dir, exist_ok=True)
    with tempfile.NamedTemporaryFile(delete=True, dir=temp_dir) as tmp_file:
        # Write the content to the temporary file
        tmp_file.write(response.content)
        tmp_file.flush()
        pedestrian_network = pdna.Network.from_hdf5(tmp_file.name)
        pedestrian_network.precompute(WALK_RADIUS)
        return pedestrian_network

@app.post("/query")
async def custom_query(payload: Dict[Any, Any]):
    metric = payload.get("metric")
    condition = payload.get("condition")
    coordinates = payload.get("coordinates")
    proximity_mapping = payload.get("accessibility_info")
    layer = payload.get("layer", "blocks") + ".fgb"
    # get IDs of lots within the coordinates
    if coordinates:
        # get bbox from coordinates
        bbox = create_bbox(coordinates)
        polygon_gdf = gpd.GeoDataFrame(
            geometry=[Polygon(coordinates)], crs="EPSG:4326"
        )
        gdf = await read_gdf_async(get_file(get_blob_url(layer)), bbox)
        gdf = gdf[gdf.intersects(polygon_gdf.unary_union)]
        condition = f"ID IN ({', '.join(gdf.ID.astype(str).tolist())})"
    if condition:
        df_lots = get_all(
            f"""SELECT ID, ({metric}) As value, latitud, longitud, num_floors, max_height, potential_new_units FROM lots WHERE {
                condition}""",
        )
    else:
        df_lots = get_all(
            f"""SELECT ID, ({
                metric}) As value, latitud, longitud, num_floors, max_height, potential_new_units FROM lots""",
        )
    df_lots["ID"] = df_lots["ID"].astype(int).astype(str)
    df_lots["value"] = df_lots["value"].fillna(0)
    df_lots = df_lots.fillna(0)

    if metric == "minutes":
        pedestrian_network = load_network()
        df_lots["node_ids"] = pedestrian_network.get_node_ids(
            df_lots.longitud, df_lots.latitud
        )

        file = get_file(get_blob_url("accessibility_points.fgb"))
        gdf_amenities = await read_gdf_async(
            file
        )

        mapping = proximity_mapping if proximity_mapping else [{**x, "radius": 1609.34*2, "importance": 1} for x in AMENITIES_MAPPING]
        df_accessibility = calculate_accessibility(pedestrian_network, gdf_amenities, mapping)
        df_lots = df_lots.merge(
            df_accessibility, left_on="node_ids", right_index=True, how="left"
        )
        df_lots = df_lots[["ID", "minutes", "latitud", "longitud", "num_floors", "max_height", "potential_new_units"]].rename(
            columns={"minutes": "value"})
    return df_lots.to_dict(orient="records")


#Calculate multiple metrics for the selected area
@app.post("/predios")
async def get_info(payload: Dict[Any, Any]):
    lots = payload.get("lots")
    base_query = "SELECT * FROM lots"

    results = []
    chunk_size = 1000

    if lots:
        for i in range(0, len(lots), chunk_size):
            chunk = lots[i:i + chunk_size]
            chunk_str = ','.join([f"'{lot}'" for lot in chunk])
            query = f"{base_query} WHERE ID IN ({chunk_str})"
            df = get_all(query)
            results.append(df)
    else:
        df = get_all(base_query)
        results.append(df)
    
    df = pd.concat(results)

    df = df.fillna(0) # -----------------------------------------------
    print("Datos después de fillna(0) en df:", df)
    
    inegi_data = df.groupby("CVEGEO").agg({
            "POBTOT": "first",
            "POBFEM": "first",
            "POBMAS": "first",
            "VIVTOT": "first",
            "VIVPAR_HAB": "first",
            "VIVPAR_DES": "first",
            "VPH_AUTOM": "first", 
            "VPH_PC": "first", 
            "VPH_TINACO": "first", 
            "PAFIL_IPRIV": "first", 
            #"GRAPROES": "first",
            
            "P_0A2_F": "first",
            "P_0A2_M": "first",
            "P_3A5_F": "first",
            "P_3A5_M": "first",
            "P_6A11_F": "first",
            "P_6A11_M": "first",
            "P_12A14_F": "first",
            "P_12A14_M": "first",
            "P_15A17_F": "first",
            "P_15A17_M": "first",
            "P_18A24_F": "first",
            "P_18A24_M": "first",
 
            "P_60YMAS_F": "first",
            "P_60YMAS_M": "first",

            # "mean_slope": "first"
        })   
        
    inegi_data = inegi_data.fillna(0)
    print("Datos después de fillna(0) en inegi_data:", inegi_data)
            
    inegi_data = inegi_data.agg({
            "POBTOT": "sum",
            "VIVTOT": "sum",
            "VIVPAR_HAB": "sum",
            "VIVPAR_DES": "sum", # Multiplicar por 100 puntuaje_hogar_digno y dejar sin decimales escuela y seguro médico
            "VPH_AUTOM": "sum", # ---------- 
            "VPH_PC": "sum", 
            "VPH_TINACO": "sum", 
            "PAFIL_IPRIV": "sum",
            #"GRAPROES": "sum",
            "POBFEM": "sum",
            "POBMAS": "sum",
            
            "P_0A2_F": "sum",
            "P_0A2_M": "sum",
            "P_3A5_F": "sum",
            "P_3A5_M": "sum",
            "P_6A11_F": "sum",
            "P_6A11_M": "sum",
            "P_12A14_F": "sum",
            "P_12A14_M": "sum",
            "P_15A17_F": "sum",
            "P_15A17_M": "sum",
            "P_18A24_F": "sum",
            "P_18A24_M": "sum",
            
            "P_60YMAS_F": "sum",
            "P_60YMAS_M": "sum",
        })
        
    inegi_data = inegi_data.fillna(0)
    print("Datos después de la segunda agregación y fillna(0):", inegi_data)
    print(df['accessibility_score'].describe())
    
    df["car_ratio"] = df["VPH_AUTOM"] / df["VIVPAR_HAB"]
    df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
    
    df['PEA'] = df['PEA'].fillna(0)
    df['PEA'] = pd.to_numeric(df['PEA'], errors='coerce').fillna(0).astype(int)
    
    df['car_ratio'] = df['car_ratio'].fillna(0)
    df['car_ratio'] = pd.to_numeric(df['car_ratio'], errors='coerce').fillna(0).astype(int)
    
    
    df['pob_por_cuarto'] = df['pob_por_cuarto'].fillna(0)
    df['pob_por_cuarto'] = pd.to_numeric(df['pob_por_cuarto'], errors='coerce').fillna(0).astype(int)

    df['puntuaje_hogar_digno'] = df['puntuaje_hogar_digno'].fillna(0)
    df['puntuaje_hogar_digno'] = pd.to_numeric(df['puntuaje_hogar_digno'], errors='coerce').fillna(0).astype(int)

    
    inegi_data['P_25A59_F'] = inegi_data['POBFEM'] - inegi_data["P_0A2_F"] - inegi_data["P_3A5_F"] - inegi_data["P_6A11_F"] - inegi_data["P_12A14_F"] - inegi_data["P_15A17_F"] - inegi_data["P_18A24_F"] - inegi_data["P_60YMAS_F"]
    inegi_data['P_25A59_M'] = inegi_data['POBMAS'] - inegi_data["P_0A2_M"] - inegi_data["P_3A5_M"] - inegi_data["P_6A11_M"] - inegi_data["P_12A14_M"] - inegi_data["P_15A17_M"] - inegi_data["P_18A24_M"] - inegi_data["P_60YMAS_M"]
  
    
    df = df.drop(columns=["ID", "num_properties"]).agg(
        {
            "building_ratio": "mean",
            "unused_ratio": "mean",
            "green_ratio": "mean",
            "parking_ratio": "mean",
            "wasteful_ratio": "mean",
            "underutilized_ratio": "mean",
            "amenity_ratio": "mean",
            "building_area": "sum",
            "unused_area": "sum", 
            "green_area": "sum",
            "parking_area": "sum",
            "wasteful_area": "sum",
            "underutilized_area": "sum",
            "amenity_area": "sum",
            "num_establishments": "sum",
            "num_workers": "sum",
            "minutes": "mean",
            "latitud": "mean",
            "longitud": "mean",
            "car_ratio": "mean",
            "PEA": "mean",  # pob económicamente activa
            "pob_por_cuarto": "mean",
            "puntuaje_hogar_digno": "mean",
            "GRAPROES": "mean",
            "accessibility_score": "mean",
            # "mean_slope": "mean"
        }
    )
    
    df = df.fillna(0)
    
    return {**df.to_dict(), **inegi_data.to_dict()}


def create_bbox(points):
    min_x, min_y = np.min(points, axis=0)
    max_x, max_y = np.max(points, axis=0)
    return (min_x, min_y, max_x, max_y)


@app.get("/polygon/{layer}")
async def get_polygon(layer: str):
    return FileResponse(get_file(get_blob_url(f"{layer}.fgb")))


@app.post("/polygon")
async def get_polygon_segment(payload: Dict[Any, Any]):
    layer = payload.get("layer")
    coordinates = payload.get("coordinates")

    if not coordinates or len(coordinates) < 3:
        return FileResponse(get_file(get_blob_url(f"{layer}.fgb")))

    layerFile = get_file(get_blob_url(f"{layer}.fgb"))
    bbox = create_bbox(coordinates)
    polygon_gdf = gpd.GeoDataFrame(
        geometry=[Polygon(coordinates)], crs="EPSG:4326"
    )
    gdf = await read_gdf_async(layerFile, bbox)
    gdf = gdf[gdf.intersects(polygon_gdf.unary_union)]

    with io.BytesIO() as output:
        pyogrio.write_dataframe(gdf, output, driver="FlatGeobuf")
        contents = output.getvalue()
        return Response(content=contents, media_type="application/octet-stream")
