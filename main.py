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
from shapely.geometry import box
from sqlalchemy import create_engine

from chat import MESSAGES, chat_response
from scripts.accessibility import get_all_info, load_network
from utils.files import get_file
from utils.utils import get_all
import time

app = FastAPI()
FOLDER = "primavera"
PROJECTS_MAPPING = {"distritotec": "distritotec", "primavera": "primavera"}
WALK_RADIUS = 1609.34


BLOB_URL = "https://reimaginaurbanostorage.blob.core.windows.net"


def get_blob_url(endpoint: str) -> str:
    access_token = "sp=r&st=2024-06-27T04:05:58Z&se=2025-07-01T12:05:58Z&spr=https&sv=2022-11-02&sr=c&sig=Uz%2B9aCyARjTCTGuJiI1hWWrx8W%2B7eSlyHDM0cBlmkxE%3D"
    return f"{BLOB_URL}/{FOLDER}/{endpoint}?{access_token}"


def calculate_metrics(metric: str, condition: str, proximity_mapping: Dict[Any, Any]):
    if condition:
        df_lots = get_all(
            f"""SELECT ID, ({metric}) As value, latitud, longitud, num_floors, max_height, potential_new_units FROM lots WHERE {condition}""",
        )
    else:
        df_lots = get_all(
            f"""SELECT ID, ({metric}) As value, latitud, longitud, num_floors, max_height, potential_new_units FROM lots""",
        )
    df_lots["ID"] = df_lots["ID"].astype(int).astype(str)
    df_lots["value"] = df_lots["value"].fillna(0)
    df_lots = df_lots.fillna(0)

    if metric == "minutes":
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
    return {"success": True}


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
    if condition:
        df_lots = get_all(
            f"""SELECT ID, ({metric}) As value, latitud, longitud, num_floors, max_height, potential_new_units FROM lots WHERE {condition}""",
        )
    else:
        df_lots = get_all(
            f"""SELECT ID, ({metric}) As value, latitud, longitud, num_floors, max_height, potential_new_units FROM lots""",
        )
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

        file = get_file( get_blob_url("accessibility_points.fgb") )
        
        gdf_aggregate = pyogrio.read_dataframe(
            file
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
    df = get_all(f"""SELECT * FROM lots WHERE ID IN ({', '.join(predio)})""")
    inegi_data = df.groupby("CVEGEO").agg({
            "POBTOT": "mean",
            "VIVTOT": "mean",
            "VIVPAR_DES": "mean",
            "VPH_AUTOM": "mean",
        }).agg({
            "POBTOT": "sum",
            "VIVTOT": "sum",
            "VIVPAR_DES": "sum",
            "VPH_AUTOM": "sum",
        })
    inegi_data["car_ratio"] = inegi_data["VPH_AUTOM"] / inegi_data["VIVTOT"]
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
        }
    )
    return {**df.to_dict(), **inegi_data.to_dict()}


@app.get("/lens")
async def lens_layer(
    lat: float,
    lon: float,
    radius: float,
    metrics: List[str] = Query(None),
):
    centroid = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy([lon], [lat]), crs="EPSG:4326"
    )
    areaFrame = centroid.to_crs("EPSG:32613").buffer(radius).to_crs("EPSG:4326")

    bounding_box = box(*areaFrame.total_bounds)

    united_gdf = gpd.GeoDataFrame()

    if( metrics ):
        for metric in metrics:
            file = get_file(f"{BLOB_URL}/{FOLDER}/{metric}.fgb")
            gdf = pyogrio.read_dataframe(file, bbox=bounding_box.bounds)
            gdf = gdf[gdf.within(areaFrame.unary_union)]
            gdf["metric"] = metric
            united_gdf = pd.concat([united_gdf, gdf], ignore_index=True)

    lofsFile = get_file(f"{BLOB_URL}/{FOLDER}/lots.fgb")
    gdf = pyogrio.read_dataframe(lofsFile, bbox=bounding_box.bounds)
    gdf = gdf[gdf.within(areaFrame.unary_union)]
    gdf["metric"] = "lots"
    united_gdf = pd.concat([united_gdf, gdf], ignore_index=True)

    pyogrio.write_dataframe(united_gdf, "/tmp/lots.fgb", driver="FlatGeobuf")

    return FileResponse(
        "/tmp/lots.fgb",
    )


@app.get("/generate-metrics")
async def generateMetrics():
    data = calculate_metrics(
        "minutes",
        "",
        {
            "proximity_educacion": 1,
            "proximity_salud": 2,
            "proximity_servicios": 5,
            "proximity_small_park": 2,
            "proximity_supermercado": 1,
        },
    )
    return data


@app.get("/test")
async def dbTest():
    query = "SELECT TOP 1 * FROM lots"

    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USERNAME")
    password = os.getenv("SQL_PASSWORD")
    driver = os.getenv("SQL_DRIVER")

    connection_string = f"DRIVER={driver};SERVER=tcp:{server},1433;DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}")

    df = pd.read_sql(query, engine)

    print(df)

    return "success"

@app.post("/minutes")
async def get_minutes(payload: Dict[Any, Any]):
    metric = "minutes"
    condition = payload.get("condition")
    proximity_mapping = payload.get("accessibility_info")
    if condition:
        df_lots = get_all(
            f"""SELECT ID, ({metric}) As value, latitud, longitud, num_floors, max_height, potential_new_units FROM lots WHERE {condition}""",
        )
    else:
        df_lots = get_all(
            f"""SELECT ID, ({metric}) As value, latitud, longitud, num_floors, max_height, potential_new_units FROM lots""",
        )

    df_lots["ID"] = df_lots["ID"].astype(int).astype(str)
    df_lots["value"] = df_lots["value"].fillna(0)
    df_lots = df_lots.fillna(0)

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

    file = get_file( get_blob_url("accessibility_points.fgb") )

    gdf_aggregate = pyogrio.read_dataframe(
            file
    )

    df_accessibility = get_all_info(
        pedestrian_network, gdf_aggregate, proximity_mapping
    )
    df_lots = df_lots.merge(
        df_accessibility, left_on="node_ids", right_index=True, how="left"
    )
    df_lots = df_lots[["ID", "minutes","num_floors","potential_new_units","max_height"]].rename(columns={"minutes": "value"})
    return df_lots.to_dict(orient="records")