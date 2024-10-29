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
from src.utils.db import query_metrics, select_minutes, MAPPING_REDUCE_FUNCS, METRIC_MAPPING, get_metrics_info

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
    if not project :
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
    level = payload.get("level", "blocks")

    id = "cvegeo" if level == "blocks" else "lot_id"

    # TODO: Integrate so that it includes all selected metrics (including minutes and accessibility_score)
    if "minutes" in metrics:
        ids = await get_ids(coordinates, level)
        df = select_minutes(level, ids, proximity_mapping)
        df = df[[id, "minutes"]]
        df = df.rename(columns={"minutes": "value"})
        df = df.fillna(0)
        return df.to_dict(orient="records")

    ids = await get_ids(coordinates, level)
    # _metrics = {k: v for k, v in metrics.items() if k not in ["minutes", "accessibility_score"]}
    df = query_metrics(level, metrics, ids)
    df = df.fillna(0)
    return df.to_dict(orient="records")


@app.post("/predios")
async def get_info(payload: Dict[Any, Any]):
    ids = payload.get("lots")
    level = payload.get("type", "blocks")
    proximity_mapping = payload.get("accessibility_info")
    # TODO: Pass cols instead of hardcoded
    cols = [
        "poblacion",
        "viviendas_habitadas",
        "viviendas_habitadas_percent",
        "viviendas_deshabitadas",
        "viviendas_deshabitadas_percent",
        "grado_escuela",
        "indice_bienestar",
        "viviendas_tinaco",
        "viviendas_pc",
        "viviendas_auto",
        "accessibility_score",
        "minutes",
        "density",
        "max_height",
        "potencial",
        "subutilizacion",
        "subutilizacion_type",
        "p_0a2_f",
        "p_0a2_m",
        "p_3a5_f",
        "p_3a5_m",
        "p_6a11_f",
        "p_6a11_m",
        "p_12a14_f",
        "p_12a14_m",
        "p_15a17_f",
        "p_15a17_m",
        "p_18a24_f",
        "p_18a24_m",
        "p_25a59_f" ,
        "p_25a59_m",
        "p_60ymas_f",
        "p_60ymas_m",
        "slope",
    ]

    df = query_metrics(level, {col: col for col in cols}, ids)
    new_cols = get_metrics_info(cols)
    new_cols = {k: v for k, v in zip(cols, new_cols)}
    if level == "lots":
        df = df.groupby("cvegeo").aggregate({k: 'min' if v['level'] != "lots" else MAPPING_REDUCE_FUNCS[v['reduce']] for k, v in new_cols.items()})
    df = df.aggregate({k: MAPPING_REDUCE_FUNCS[v['reduce']] for k, v in new_cols.items()})
    df = df.fillna(0)
    results = df.to_dict()
    # TODO: Implement accessibility_score part
    if "minutes" in cols or "accessibility_score" in cols:
        id = "cvegeo" if level == "blocks" else "lot_id"
        df = select_minutes(level, ids, proximity_mapping)
        df = df[[id, "minutes"]]
        df = df.aggregate({"minutes": "mean"})
        df = df.fillna(0)
        results["minutes"] = df["minutes"].item()
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
