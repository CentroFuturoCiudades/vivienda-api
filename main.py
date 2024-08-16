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
    access_token = os.getenv("BLOB_TOKEN")
    return f"{BLOB_URL}/{FOLDER}/{endpoint}?{access_token}"


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
    return FileResponse(f"data/_primavera/final/{clave}.fgb")


@app.get("/bounds")
async def get_bounds():
    return FileResponse(f"data/_primavera/final/bounds.geojson")


@app.post("/query")
async def custom_query(payload: Dict[Any, Any]):
    metric = payload.get("metric")
    condition = payload.get("condition")
    coordinates = payload.get("coordinates")
    # get IDs of lots within the coordinates
    if coordinates:
        # get bbox from coordinates
        polygon = Polygon(coordinates)
        polygon_gdf = gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")
        bbox = create_bbox(coordinates)
        gdf = pyogrio.read_dataframe(
            get_file(get_blob_url("lots.fgb")), bbox=bbox
        )
        gdf = gdf[gdf.within(polygon_gdf.unary_union)]
        gdf = gdf[["ID"]]
        condition = f"ID IN ({', '.join(gdf.ID.astype(str).tolist())})"
    proximity_mapping = payload.get("accessibility_info")
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
        # gdf_bounds = gpd.read_file(
        #     f"{BLOB_URL}/bounds.geojson",
        #     crs="EPSG:4326",
        # )
        # pedestrian_network = load_network(
        #     f"{BLOB_URL}/pedestrian_network.hd5", gdf_bounds, WALK_RADIUS
        # )
        response = requests.get(get_blob_url("pedestrian_network.hd5"))
        response.raise_for_status()

        temp_dir = "./temp/"
        os.makedirs( temp_dir, exist_ok=True)
        with tempfile.NamedTemporaryFile(delete=True, dir=temp_dir) as tmp_file:
            # Write the content to the temporary file
            tmp_file.write(response.content)
            tmp_file.flush()
            pedestrian_network = pdna.Network.from_hdf5(tmp_file.name)
            pedestrian_network.precompute(WALK_RADIUS)
        df_lots["node_ids"] = pedestrian_network.get_node_ids(
            df_lots.longitud, df_lots.latitud
        )

        file = get_file(get_blob_url("accessibility_points.fgb"))

        gdf_aggregate = pyogrio.read_dataframe(
            file
        )

        df_accessibility = get_all_info(
            pedestrian_network, gdf_aggregate, proximity_mapping
        )
        df_lots = df_lots.merge(
            df_accessibility, left_on="node_ids", right_index=True, how="left"
        )
        df_lots = df_lots[["ID", "minutes"]].rename(
            columns={"minutes": "value"})
    return df_lots.to_dict(orient="records")


#Calculate multiple metrics for the selected area
@app.get("/predios/")
async def get_info(predio: Annotated[list[str] | None, Query()] = None):
    query = "SELECT * FROM lots"

    if (predio):
        query += f""" WHERE ID IN ({', '.join(predio)})"""

    df = get_all(query)
    
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

            "mean_slope": "first"
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
    
    df["car_ratio"] = df["VPH_AUTOM"] / df["VIVPAR_HAB"]
    df = df.replace([np.inf, -np.inf], np.nan).fillna(0)
    print("Datos después de calcular car_ratio y reemplazar inf:", df)
    print(df["car_ratio"])
    
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
            # "park_ratio": "mean",
            "wasteful_ratio": "mean",
            "underutilized_ratio": "mean",
            "amenity_ratio": "mean",
            "building_area": "sum",
            "unused_area": "sum", 
            "green_area": "sum",
            "parking_area": "sum",
            # "park_area": "sum",
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

            "mean_slope": "mean"
        }
    )
    
    df = df.fillna(0)
    
    print(df)
    
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
    areaFrame = centroid.to_crs("EPSG:32613").buffer(
        radius).to_crs("EPSG:4326")

    bounding_box = box(*areaFrame.total_bounds)

    united_gdf = gpd.GeoDataFrame()

    if (metrics):
        for metric in metrics:
            file = get_file(get_blob_url(f"{metric}.fgb"))
            # file = get_file(f"{BLOB_URL}/{FOLDER}/{metric}.fgb")
            gdf = pyogrio.read_dataframe(file, bbox=bounding_box.bounds)
            gdf = gdf[gdf.within(areaFrame.unary_union)]
            gdf["metric"] = metric
            united_gdf = pd.concat([united_gdf, gdf], ignore_index=True)

    # lofsFile = get_file(f"{BLOB_URL}/{FOLDER}/lots.fgb")
    lofsFile = get_file(get_blob_url(f"lots.fgb"))
    print(lofsFile)
    gdf = pyogrio.read_dataframe(lofsFile, bbox=bounding_box.bounds)
    gdf = gdf[gdf.within(areaFrame.unary_union)]
    gdf["metric"] = "lots"
    united_gdf = pd.concat([united_gdf, gdf], ignore_index=True)

    filePath = f"./data/lots.fgb"
    if not os.path.exists(filePath):
        f = open(filePath, "x")
        f.close()

    pyogrio.write_dataframe(united_gdf, filePath, driver="FlatGeobuf")

    return FileResponse(
        filePath
    )


def create_bbox(points):
    min_x, min_y = np.min(points, axis=0)
    max_x, max_y = np.max(points, axis=0)
    return (min_x, min_y, max_x, max_y)


@app.post("/poligon")
async def poligon_layer(payload: Dict[Any, Any]):
    coordinates = payload.get("coordinates")
    layer = payload.get("layer")


    lofsFile = get_file(f"data/_primavera/final/{layer}.fgb")

    if( coordinates ):
        polygon = Polygon(coordinates)
        polygon_gdf = gpd.GeoDataFrame(geometry=[polygon], crs="EPSG:4326")

        bbox = create_bbox(coordinates)

        gdf = pyogrio.read_dataframe(lofsFile, bbox=bbox)
        gdf = gdf[gdf.within(polygon_gdf.unary_union)]
    
    else:
        gdf = pyogrio.read_dataframe(lofsFile)

    filePath = f"./data/{layer}.fgb"

    if not os.path.exists(filePath):
        f = open(filePath, "x")
        f.close()

    pyogrio.write_dataframe(gdf, filePath, driver="FlatGeobuf")

    return FileResponse(
        filePath
    )


@app.get("/points")
async def getPoints():

    file = get_file(f"{BLOB_URL}/{FOLDER}/points.fgb")
    gdf = pyogrio.read_dataframe(file)

    return gdf.to_json()


@app.get("/test")
async def dbTest():
    query = "SELECT TOP 1 * FROM lots"

    server = os.getenv("SQL_SERVER")
    database = os.getenv("SQL_DATABASE")
    username = os.getenv("SQL_USERNAME")
    password = os.getenv("SQL_PASSWORD")
    driver = os.getenv("SQL_DRIVER")

    connection_string = f"DRIVER={driver};SERVER=tcp:{server},1433;DATABASE={database};UID={
        username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={connection_string}")

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

    file = get_file(get_blob_url("accessibility_points.fgb"))

    gdf_aggregate = pyogrio.read_dataframe(
        file
    )

    df_accessibility = get_all_info(
        pedestrian_network, gdf_aggregate, proximity_mapping
    )
    df_lots = df_lots.merge(
        df_accessibility, left_on="node_ids", right_index=True, how="left"
    )
    df_lots = df_lots[["ID", "minutes", "num_floors", "potential_new_units",
                       "max_height"]].rename(columns={"minutes": "value"})
    return df_lots.to_dict(orient="records")
    df_lots = df_lots[["ID", "minutes","num_floors","potential_new_units","max_height"]].rename(columns={"minutes": "value"})
    return df_lots.to_dict(orient="records")

@app.get("/layers")
async def get_layers(
    lat: float | None = None,
    lon: float | None = None,
    radius: float | None = None,
    layers: List[str] = Query(None)
):
    
    united_gdf = gpd.GeoDataFrame()

    has_bounding = False

    if( lat and lon ):

        centroid = gpd.GeoDataFrame(
            geometry=gpd.points_from_xy([lon], [lat]), crs="EPSG:4326"
        )
        areaFrame = centroid.to_crs("EPSG:32613").buffer(radius).to_crs("EPSG:4326")

        bounding_box = box(*areaFrame.total_bounds)
        
        has_bounding= True
    
    if( layers ):
        for layer in layers:
            file = get_file(f"{BLOB_URL}/{FOLDER}/{layer}.fgb")
            
            if( not has_bounding ):
                gdf = pyogrio.read_dataframe(file)
            else:
                gdf = pyogrio.read_dataframe(file, bbox=bounding_box.bounds)
                gdf = gdf[gdf.within(areaFrame.unary_union)]

            gdf["layer"] = layer
            united_gdf = pd.concat([united_gdf, gdf], ignore_index=True)
    
    filePath = f"./data/lots.fgb"

    if not os.path.exists(filePath):
        f = open( filePath, "x")
        f.close()

    pyogrio.write_dataframe(united_gdf, filePath , driver="FlatGeobuf")

    return FileResponse(
       filePath
    )
