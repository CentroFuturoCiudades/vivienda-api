from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, Response
from fastapi.middleware.cors import CORSMiddleware
from chat import chat_response, messages
from pydantic import BaseModel
import geopandas as gpd
import json
import sqlite3
from typing import Annotated
import pandas as pd
from utils.utils import normalize, calculate_walking_distance
import matplotlib.pyplot as plt
from osmnx.distance import nearest_nodes
import osmnx as ox
import numpy as np
import json
from typing import Dict, Any
from scripts.accessibility import get_all_info, load_network

app = FastAPI()
FOLDER = "data/la_primavera"
PROJECTS_MAPPING = {
    "DT": "data/distritotec",
    "primavera": "data/la_primavera"
}
WALK_RADIUS = 1609.34

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
  response = chat_response(item['message'])
  print(response)
  return {"history": messages, **response}


@app.get("/project/{project_name}")
async def change_project(project_name: str):
  global FOLDER
  FOLDER = PROJECTS_MAPPING.get(project_name)


@app.get("/coords")
async def get_coordinates():
  gdf_bounds = gpd.read_file(f'{FOLDER}/bounds.geojson', crs='EPSG:4326')
  geom = gdf_bounds.unary_union
  return {"latitud": geom.centroid.y, "longitud": geom.centroid.x}


@app.get("/geojson/{clave}")
async def get_geojson(clave: str):
  return FileResponse(f"{FOLDER}/{clave}.geojson")


@app.post("/query")
async def custom_query(payload: Dict[Any, Any]):
  metric = payload.get('metric')
  condition = payload.get('condition')
  proximity_mapping = payload.get('accessibility_info')
  conn = sqlite3.connect(f'{FOLDER}/predios.db')
  cursor = conn.cursor()
  if condition:
    data = get_all(cursor, f'''SELECT ID, ({metric}) As value, latitud, longitud FROM predios WHERE {condition}''')
  else:
    data = get_all(cursor, f'''SELECT ID, ({metric}) As value, latitud, longitud FROM predios''')
  df_lots = pd.DataFrame(data)
  df_lots['ID'] = df_lots['ID'].astype(int).astype(str)
  df_lots['value'] = df_lots['value'].fillna(0)

  if metric == 'minutes':
    gdf_bounds = gpd.read_file(f'{FOLDER}/bounds.geojson', crs='EPSG:4326')
    pedestrian_network = load_network('data/pedestrian_network.hd5', gdf_bounds, WALK_RADIUS)
    pedestrian_network.precompute(WALK_RADIUS)
    df_lots['node_ids'] = pedestrian_network.get_node_ids(df_lots.longitud, df_lots.latitud)
    gdf_aggregate = gpd.read_file("data/lots.gpkg", layer='points_accessibility', crs='EPSG:4326')

    df_accessibility = get_all_info(pedestrian_network, gdf_aggregate, proximity_mapping)
    df_lots = df_lots.merge(df_accessibility, left_on='node_ids', right_index=True, how='left')
    df_lots = df_lots[['ID', 'minutes']].rename(columns={'minutes': 'value'})
  return df_lots[['ID', 'value']].to_dict(orient='records')


@app.get("/predios/")
async def get_info(predio: Annotated[list[str] | None, Query()] = None):
  conn = sqlite3.connect(f'{FOLDER}/predios.db')
  cursor = conn.cursor()
  data = get_all(cursor, f'''SELECT * FROM predios WHERE ID IN ({', '.join(predio)})''')
  df = pd.DataFrame(data)
  df = df.drop(columns=['ID', 'num_properties']).agg({
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
  })
  return df.to_dict()


def get_all(cursor, query):
  print(query)
  output_obj = cursor.execute(query)
  results = output_obj.fetchall()
  data = [{output_obj.description[i][0]: row[i] for i in range(len(row))} for row in results]
  return data
