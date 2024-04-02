from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from chat import chat_response, messages
from pydantic import BaseModel
import geopandas as gpd
import json
import sqlite3
from typing import Annotated
import pandas as pd
from utils.utils import normalize, calculate_walking_distance, COLUMN_ID
import matplotlib.pyplot as plt
from osmnx.distance import nearest_nodes
import osmnx as ox
import numpy as np

app = FastAPI()
FOLDER = "data/processed_primavera"

# add rout for new chat request
origins = [
    "*",
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def dict_factory(cursor, row):
  d = {}
  for idx, col in enumerate(cursor.description):
    d[col[0]] = row[idx]
  return d


class Item(BaseModel):
  message: str


conn = sqlite3.connect('data/predios.db')
cursor = conn.cursor()


@app.post("/chat")
async def chat_request(item: Item):
  response = chat_response(item.message)
  print(response)
  return {"history": messages, **response}


@app.get("/coords/")
async def get_coordinates():
  gdf = gpd.read_file(f"{FOLDER}/predios.geojson", crs="EPSG:4326")
  geom = gdf.unary_union
  return { "latitud": geom.centroid.y, "longitud": geom.centroid.x }


@app.get("/geojson/{clave}")
async def get_geojson(clave: str):
  print(f"{FOLDER}/{clave}.geojson")
  return FileResponse(f"{FOLDER}/{clave}.geojson")


@app.get("/query")
async def custom_query(metric: str, condition: str = None):
  print(condition, metric)
  if condition:
    data = get_all(cursor, f'''SELECT {COLUMN_ID} as clave, ({metric}) As value FROM predios WHERE {condition}''')
  else:
    data = get_all(cursor, f'''SELECT {COLUMN_ID} as clave, ({metric}) As value FROM predios''')
  # print(data)
  print(len(data))
  return data


WALK_RADIUS = 1609.34 / 4
SECTORS = ['comercio', 'servicios', 'salud', 'educacion']


@app.get("/predios/")
async def get_info(predio: Annotated[list[str] | None, Query()] = None):
  data = get_all(cursor, f'''SELECT * FROM predios WHERE {COLUMN_ID} IN ({', '.join(predio)})''')
  print(data)
  # reduce data to only one predio, sum all the values
  df = pd.DataFrame(data)
  df = df.drop(columns=[COLUMN_ID, 'num_properties']).agg({
      "building_ratio": "mean",
      "unused_ratio": "mean",
      "green_ratio": "mean",
      "parking_ratio": "mean",
      "park_ratio": "mean",
      "wasteful_ratio": "mean",
      "underutilized_ratio": "mean",
      "equipment_ratio": "mean",
      "services_nearby": "mean",
      "num_establishments": "sum",
      "num_workers": "sum",
      "POBTOT": "mean",
      "VIVTOT": "mean",
      "VIVPAR_DES": "mean",
      "comercio": "mean",
      "servicios": "mean",
      "salud": "mean",
      "educacion": "mean",
      "accessibility_score": "mean",
      "adj_comercio": "mean",
      "adj_servicios": "mean",
      "adj_salud": "mean",
      "adj_educacion": "mean",
  })
  print(predio)
  gdf_lots = gpd.read_file(f"{FOLDER}/predios.geojson", crs="EPSG:4326")
  gdf_lots = gdf_lots[gdf_lots[COLUMN_ID].isin(predio)]
  gdf_denue = gpd.read_file(f"{FOLDER}/denue.geojson", crs="EPSG:4326")
  gdf_lots['latitud'] = gdf_lots['geometry'].centroid.y
  gdf_lots['longitud'] = gdf_lots['geometry'].centroid.x
  first = gdf_lots.drop(columns=['geometry']).iloc[0]
  print(df.to_dict())
  return {**df.to_dict(), **first.to_dict()}


def get_all(cursor, query):
  print(query)
  output_obj = cursor.execute(query)
  results = output_obj.fetchall()
  data = [{output_obj.description[i][0]: row[i] for i in range(len(row))} for row in results]
  return data


if __name__ == "__main__":
  gdf = gpd.read_file("data/datos.geojson", crs="EPSG:4326")
  gdf = gdf[['POBTOT',
             'VIVPAR_DES',
             'VIVTOT',
             'building_ratio',
             'green_ratio',
             'equipment_ratio',
             'parking_ratio',
             'unused_ratio',
             'wasteful_ratio',
             'num_workers',
             'num_establishments',
             'educacion',
             'salud',
             'comercio',
             'servicios',
             'services_nearby',
             'accessibility_score',
             'combined_score']]
  pd.options.display.float_format = "{:,.2f}".format
  print(gdf.describe().transpose()[['mean', 'min', '25%', '50%', '75%', 'max']])
  # data = get_all(cursor, '''SELECT COLUMN_ID, underutilized_ratio FROM predios WHERE POBTOT > 200''')
  # print(data)
