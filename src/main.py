import io
import json
import os
import sqlite3
import tempfile
from typing import Annotated, Any, Dict, List

import geopandas as gpd
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
from src.utils.files import get_file, get_blob_url
from src.utils.utils import get_all
import time
import pickle
from shapely.geometry import box
import asyncio
from concurrent.futures import ThreadPoolExecutor
from src.scripts.utils.constants import AMENITIES_MAPPING
from functools import lru_cache
import os

app = FastAPI()
PROJECTS_MAPPING = {"distritotec": "distritotec", "primavera": "primavera"}
WALK_RADIUS = 1609.34


# hello world
@app.get("/")
async def root():
    return {"message": "Hello World"}


allowed_origins_env = os.getenv("ALLOWED_ORIGINS", "")
pool = ThreadPoolExecutor()

# Split the origins by comma and remove any surrounding whitespace
allowed_origins = [origin.strip() for origin in allowed_origins_env.split(",") if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def read_gdf_sync(filepath, bbox=None):
    return gpd.read_file(filepath, bbox=bbox, engine="pyogrio")

async def read_gdf_async(filepath, bbox=None):
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(pool, read_gdf_sync, filepath, bbox)
    return result

@app.get("/coords")
async def get_coordinates():
    gdf_bounds = await read_gdf_async(get_file(get_blob_url("bounds.fgb")), None)
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
        condition = f"id IN ({', '.join([f"'{x}'" for x in gdf.ID.astype(str).tolist()])})"
    if condition:
        df_lots = get_all(
            f"""SELECT id, ({metric}) As value, latitud, longitud, num_floors, max_height, potential_new_units FROM lots WHERE {
                condition}""",
        )
    else:
        df_lots = get_all(
            f"""SELECT id, ({
                metric}) As value, latitud, longitud, num_floors, max_height, potential_new_units FROM lots""",
        )
    df_lots["ID"] = df_lots["id"].astype(int).astype(str)
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
            query = f"{base_query} WHERE id IN ({chunk_str})"
            df = get_all(query)
            results.append(df)
    else:
        df = get_all(base_query)
        results.append(df)
    
    df = pd.concat(results)

    df = df.fillna(0) # -----------------------------------------------
    print("Datos después de fillna(0) en df:", df)
    
    inegi_data = df.groupby("cvegeo").agg({
        "pobtot": "first",
        "pobfem": "first",
        "pobmas": "first",
        "vivtot": "first",
        "vivpar_hab": "first",
        "vivpar_des": "first",
        "vph_autom": "first",
        "vph_pc": "first", 
        "vph_tinaco": "first",
        "p_0a2_f": "first",
        "p_0a2_m": "first",
        "p_3a5_f": "first",
        "p_3a5_m": "first",
        "p_6a11_f": "first",
        "p_6a11_m": "first",
        "p_12a14_f": "first",
        "p_12a14_m": "first",
        "p_15a17_f": "first",
        "p_15a17_m": "first",
        "p_18a24_f": "first",
        "p_18a24_m": "first",
        "p_25a59_f": "first",
        "p_25a59_m": "first",
        "p_60ymas_f": "first",
        "p_60ymas_m": "first",
        # "mean_slope": "first"
    })
        
    inegi_data = inegi_data.fillna(0)
    print("Datos después de fillna(0) en inegi_data:", inegi_data)
            
    inegi_data = inegi_data.agg({
        "pobtot": "sum",
        "vivtot": "sum",
        "vivpar_hab": "sum",
        "vivpar_des": "sum",
        "vph_autom": "sum",
        "vph_pc": "sum",
        "vph_tinaco": "sum",
        "pobfem": "sum",
        "pobmas": "sum",
        "p_0a2_f": "sum",
        "p_0a2_f": "sum",
        "p_0a2_m": "sum",
        "p_3a5_f": "sum",
        "p_3a5_m": "sum",
        "p_6a11_f": "sum",
        "p_6a11_m": "sum",
        "p_12a14_f": "sum",
        "p_12a14_m": "sum",
        "p_15a17_f": "sum",
        "p_15a17_m": "sum",
        "p_18a24_f": "sum",
        "p_18a24_m": "sum",
        "p_25a59_f": "sum",
        "p_25a59_m": "sum",
        "p_60ymas_f": "sum",
        "p_60ymas_m": "sum",
    })
        
    inegi_data = inegi_data.fillna(0)
    print("Datos después de la segunda agregación y fillna(0):", inegi_data)
    
    df["car_ratio"] = df["vph_autom"] / df["vivpar_hab"]
    df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
    
    df['pea'] = df['pea'].fillna(0)
    df['pea'] = pd.to_numeric(df['pea'], errors='coerce').fillna(0).astype(int)
    
    df['car_ratio'] = df['car_ratio'].fillna(0)
    df['car_ratio'] = pd.to_numeric(df['car_ratio'], errors='coerce').fillna(0).astype(int)
    
    df['pob_por_cuarto'] = df['pob_por_cuarto'].fillna(0)
    df['pob_por_cuarto'] = pd.to_numeric(df['pob_por_cuarto'], errors='coerce').fillna(0).astype(int)

    df['puntuaje_hogar_digno'] = df['puntuaje_hogar_digno'].fillna(0)
    df['puntuaje_hogar_digno'] = pd.to_numeric(df['puntuaje_hogar_digno'], errors='coerce').fillna(0).astype(int)
    
    df = df.drop(columns=["id", "num_properties"]).agg(
        {
            "building_ratio": "mean",
            "unused_ratio": "mean",
            "green_ratio": "mean",
            "parking_ratio": "mean",
            "wasteful_ratio": "mean",
            "underutilized_ratio": "mean",
            # "amenity_ratio": "mean",
            "building_area": "sum",
            "unused_area": "sum", 
            "green_area": "sum",
            "parking_area": "sum",
            "wasteful_area": "sum",
            "underutilized_area": "sum",
            # "amenity_area": "sum",
            "num_establishments": "sum",
            "num_workers": "sum",
            "minutes": "mean",
            "latitud": "mean",
            "longitud": "mean",
            "car_ratio": "mean",
            "pea": "mean",  # pob económicamente activa
            "pob_por_cuarto": "mean",
            "puntuaje_hogar_digno": "mean",
            "graproes": "mean",
            "accessibility_score": "mean",
            "mean_slope": "mean"
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
