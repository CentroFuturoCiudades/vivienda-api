import io
import json
import sqlite3
import tempfile
from typing import Annotated, Any, Dict

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import osmnx as ox
import pandana as pdna
import pandas as pd
import requests
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from osmnx.distance import nearest_nodes
from pydantic import BaseModel

from chat import MESSAGES, chat_response
from scripts.accessibility import get_all_info, load_network
from utils.utils import get_all

app = FastAPI()
FOLDER = "la_primavera"
PROJECTS_MAPPING = {"distritotec": "distritotec", "primavera": "primavera"}
WALK_RADIUS = 1609.34


BLOB_URL = "https://reimaginaurbanostorage.blob.core.windows.net"


def get_blob_url(endpoint: str) -> str:
    return f"{BLOB_URL}/{FOLDER}/{endpoint}"


# add rout for new chat request
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


@app.post("/chat")
async def chat_request(item: Dict[str, str]):
    response = chat_response(item["message"])
    print(response)
    return {"history": MESSAGES, **response}


@app.get("/project/{project_name}")
async def change_project(project_name: str):
    global FOLDER
    FOLDER = PROJECTS_MAPPING.get(project_name)


@app.get("/coords")
async def get_coordinates():
    gdf_bounds = gpd.read_file(
        get_blob_url("bounds.geojson"),
        crs="EPSG:4326",
    )
    geom = gdf_bounds.unary_union
    return {"latitud": geom.centroid.y, "longitud": geom.centroid.x}


@app.get("/geojson/{clave}")
async def get_geojson(clave: str):
    gdf = gpd.read_file(get_blob_url(f"{clave}.fgb"))
    print(gdf)
    return json.loads(gdf.to_json())
    # return FileResponse(f"{FOLDER}/{clave}.geojson")


@app.post("/query")
async def custom_query(payload: Dict[Any, Any]):
    metric = payload.get("metric")
    condition = payload.get("condition")
    proximity_mapping = payload.get("accessibility_info")
    conn = sqlite3.connect(f"data/{FOLDER}/final/predios.db")
    cursor = conn.cursor()
    if condition:
        data = get_all(
            cursor,
            f"""SELECT ID, ({metric}) As value, latitud, longitud, num_floors, max_height, potential_new_units FROM predios WHERE {condition}""",
        )
    else:
        data = get_all(
            cursor,
            f"""SELECT ID, ({metric}) As value, latitud, longitud, num_floors, max_height, potential_new_units FROM predios""",
        )
    df_lots = pd.DataFrame(data)
    df_lots["ID"] = df_lots["ID"].astype(int).astype(str)
    df_lots["value"] = df_lots["value"].fillna(0)
    df_lots = df_lots.fillna(0)

    if metric == "minutes":
        # gdf_bounds = gpd.read_file(
        #     f"{BLOB_URL}/bounds.geojson",
        #     crs="EPSG:4326",
        # )
        # pedestrian_network = load_network(
        #     f"{BLOB_URL}/pedestrian_network.hd5", gdf_bounds, WALK_RADIUS
        # )
        response = requests.get(get_blob_url("pedestrian_network.hd5"))
        response.raise_for_status()
        with tempfile.NamedTemporaryFile(delete=True) as tmp_file:
            # Write the content to the temporary file
            tmp_file.write(response.content)
            tmp_file.flush()
            pedestrian_network = pdna.Network.from_hdf5(tmp_file.name)
            pedestrian_network.precompute(WALK_RADIUS)
        df_lots["node_ids"] = pedestrian_network.get_node_ids(
            df_lots.longitud, df_lots.latitud
        )
        gdf_aggregate = gpd.read_file(
            get_blob_url("accessibility_points.fgb"),
            crs="EPSG:4326",
        )

        df_accessibility = get_all_info(
            pedestrian_network, gdf_aggregate, proximity_mapping
        )
        df_lots = df_lots.merge(
            df_accessibility, left_on="node_ids", right_index=True, how="left"
        )
        df_lots = df_lots[["ID", "minutes"]].rename(columns={"minutes": "value"})
    return df_lots.to_dict(orient="records")


@app.get("/predios/")
async def get_info(predio: Annotated[list[str] | None, Query()] = None):
    conn = sqlite3.connect(f"data/{FOLDER}/final/predios.db")
    cursor = conn.cursor()
    data = get_all(
        cursor, f"""SELECT * FROM predios WHERE ID IN ({', '.join(predio)})"""
    )
    df = pd.DataFrame(data)
    df = df.drop(columns=["ID", "num_properties"]).agg(
        {
            "building_ratio": "mean",
            "unused_ratio": "mean",
            "green_ratio": "mean",
            "parking_ratio": "mean",
            "park_ratio": "mean",
            "wasteful_ratio": "mean",
            "underutilized_ratio": "mean",
            "equipment_ratio": "mean",
            "building_area": "sum",
            "unused_area": "sum",
            "green_area": "sum",
            "parking_area": "sum",
            "park_area": "sum",
            "wasteful_area": "sum",
            "underutilized_area": "sum",
            "equipment_area": "sum",
            "num_establishments": "sum",
            "num_workers": "sum",
            "POBTOT": "mean",
            "VIVTOT": "mean",
            "VIVPAR_DES": "mean",
            "servicios": "mean",
            "salud": "mean",
            "educacion": "mean",
            "accessibility": "mean",
            "minutes": "mean",
            "latitud": "mean",
            "longitud": "mean",
            "minutes_proximity_big_park": "mean",
            "minutes_proximity_small_park": "mean",
            "minutes_proximity_salud": "mean",
            "minutes_proximity_educacion": "mean",
            "minutes_proximity_servicios": "mean",
            "minutes_proximity_supermercado": "mean",
            "minutes_proximity_age_diversity": "mean",
        }
    )
    return df.to_dict()
