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
import time
import pickle
from shapely.geometry import box
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from shapely.geometry import shape

from src.utils.files import get_file, get_blob_url
from src.utils.db import query_metrics, select_minutes, select_accessibility_score, MAPPING_REDUCE_FUNCS, METRIC_MAPPING, get_metrics_info, select_furthest_amenity

app = FastAPI()

allowed_origins_env = os.getenv("ALLOWED_ORIGINS", "")
allowed_origins = [origin.strip()
                   for origin in allowed_origins_env.split(",") if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
pool = ThreadPoolExecutor()


def read_gdf_sync(filepath, bbox=None):
    return gpd.read_file(filepath, bbox=bbox, engine="pyogrio")


async def read_gdf_async(filepath, bbox=None):
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(pool, read_gdf_sync, filepath, bbox)
    return result


def gdf_from_coords(coords: List[List[float]]) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        geometry=[Polygon(x) for x in coords], crs="EPSG:4326"
    )


async def load_gdf(layer: str, polygon_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    bbox = box(*polygon_gdf.total_bounds)
    gdf = await read_gdf_async(get_file(get_blob_url(layer)), bbox)
    gdf = gdf[gdf.intersects(polygon_gdf.unary_union)]
    return gdf


async def get_ids(coordinates: List[List[float]], level: str) -> List[str]:
    if not coordinates or len(coordinates) == 0:
        return []
    id = "cvegeo" if level == "blocks" else "lot_id"
    polygon_gdf = gdf_from_coords(coordinates)
    gdf = await load_gdf(level + ".fgb", polygon_gdf)
    return gdf[id].astype(str).tolist()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/coords")
async def get_coordinates(project: str = None):
    if not project:
        project = "primavera"

    gdf_bounds = await read_gdf_async(get_file(get_blob_url(f"{project}_bounds.fgb")), None)
    geom = gdf_bounds.unary_union
    return {"latitude": geom.centroid.y, "longitude": geom.centroid.x}


@app.post("/query")
async def custom_query(payload: Dict[Any, Any]):
    metrics = payload.get("metrics")
    condition = payload.get("condition")
    coordinates = payload.get("coordinates")
    proximity_mapping = payload.get("accessibility_info")
    payload['group_ages'] = [POB_AGES_METRICS_MAPPING[age]
                             for age in payload['group_ages']]
    level = payload.get("level", "blocks")

    id = "cvegeo" if level == "blocks" else "lot_id"
    ids = await get_ids(coordinates, level)

    # TODO: Integrate so that it includes all selected metrics (including minutes and accessibility_score)
    if "minutes" in metrics:
        df = select_minutes(level, ids, proximity_mapping)
        df = df[[id, "minutes"]]
        df = df.rename(columns={"minutes": "value"})
    elif "accessibility_score" in metrics:
        df = select_accessibility_score(level, ids, proximity_mapping)
        df['accessibility_score'] = np.log(df['accessibility_score'] + 1) * 17
        df = df[[id, "accessibility_score"]]
        df = df.rename(columns={"accessibility_score": "value"})
    else:
        df = query_metrics(level, metrics, ids, payload)
    df = df.fillna(0)
    print(df)
    df_dict = df.to_dict(orient="records")
    quantiles = df["value"].quantile([0, 0.2, 0.4, 0.6, 0.8, 1])
    dict_quantiles = quantiles.to_dict()
    dict_quantiles = {str(k): v for k, v in dict_quantiles.items()}
    dict_quantiles["mean"] = df["value"].mean()
    dict_quantiles["std"] = df["value"].std()

    return {"stats_info": dict_quantiles, "data": df_dict}

POB_AGES_METRICS_MAPPING = {
    "0-2": "0a2",
    "3-5": "3a5",
    "6-11": "6a11",
    "12-14": "12a14",
    "15-17": "15a17",
    "18-24": "18a24",
    "25-59": "25a59",
    "60+": "60ymas",
}


@app.post("/predios")
async def get_info(payload: Dict[Any, Any]):
    coordinates = payload.get("coordinates")
    level = payload.get("type", "blocks")
    ids = await get_ids(coordinates, level)
    proximity_mapping = payload.get("accessibility_info")
    payload['group_ages'] = [POB_AGES_METRICS_MAPPING[age]
                             for age in payload['group_ages']]
    # TODO: Pass cols instead of hardcoded
    cols = [
        "poblacion",
        "viviendas_habitadas",
        "viviendas_habitadas_percent",
        "viviendas_deshabitadas",
        "viviendas_deshabitadas_percent",
        "grado_escuela",
        "area",
        "indice_bienestar",
        "viviendas_tinaco",
        "viviendas_pc",
        "viviendas_auto",
        "accessibility_score",
        "minutes",
        "density",
        "cos",
        "max_cos",
        "cus",
        "max_cus",
        "max_density",
        "max_num_levels",
        "home_units",
        "max_home_units",
        "subutilizacion",
        "subutilizacion_type",
        "num_levels",
        "per_female_group_ages",
        "per_male_group_ages",
        "per_group_ages",
        "slope",
    ]
    try:
        df = query_metrics(level, {col: col for col in cols}, ids, payload)
        print(df)
        new_cols = get_metrics_info(cols)
        new_cols = {k: v for k, v in zip(cols, new_cols)}
        if level == "lots":
            df = df.groupby("cvegeo").aggregate(
                {k: 'min' if v['level'] != "lots" else MAPPING_REDUCE_FUNCS[v['reduce']] for k, v in new_cols.items()})
        df = df.aggregate(
            {k: MAPPING_REDUCE_FUNCS[v['reduce']] for k, v in new_cols.items()})
        df = df.fillna(0)
        results = df.to_dict()
        # TODO: Implement accessibility_score part
        if "minutes" in cols:
            id = "cvegeo" if level == "blocks" else "lot_id"
            df = select_minutes(level, ids, proximity_mapping)
            df = df[[id, "minutes"]]
            df = df.aggregate({"minutes": "mean"})
            df = df.fillna(0)
            results["minutes"] = df["minutes"].item()

            df = select_furthest_amenity(level, ids, proximity_mapping)
            df = df[[id, "amenity"]]
            df = df.aggregate({"amenity": lambda x: x.value_counts().idxmax()})
            results["amenity"] = df["amenity"]
    except Exception as e:
        results = {}
    # if "accessibility_score" in cols:
    #     id = "cvegeo"
    #     df = select_accessibility_score(level, ids, proximity_mapping)
    #     df['accessibility_score'] = np.log(df['accessibility_score'] + 1) * 17
    #     df = df[[id, "accessibility_score"]]
    #     df = df.aggregate({"accessibility_score": "mean"})
    #     df = df.fillna(0)
    #     results["accessibility_score"] = df["accessibility_score"].item()
    return results


@app.get("/polygon/{layer}")
async def get_polygon(layer: str):
    return FileResponse(get_file(get_blob_url(f"{layer}.fgb")))


@app.post("/polygon")
async def get_polygon_segment(payload: Dict[Any, Any]):
    layer = payload.get("layer")
    coordinates = payload.get("coordinates")

    if not coordinates or len(coordinates) == 0:
        return FileResponse(get_file(get_blob_url(f"{layer}.fgb")))

    layerFile = get_file(get_blob_url(f"{layer}.fgb"))
    polygon_gdf = gpd.GeoDataFrame(
        geometry=[Polygon(x) for x in coordinates], crs="EPSG:4326"
    )
    bbox = box(*polygon_gdf.total_bounds)
    gdf = await read_gdf_async(layerFile, bbox)
    gdf = gdf[gdf.intersects(polygon_gdf.unary_union)]

    with io.BytesIO() as output:
        pyogrio.write_dataframe(gdf, output, driver="FlatGeobuf")
        contents = output.getvalue()
        return Response(content=contents, media_type="application/octet-stream")
