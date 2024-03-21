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
from utils.utils import normalize, calculate_walking_distance
import matplotlib.pyplot as plt
from osmnx.distance import nearest_nodes
import osmnx as ox
import numpy as np

app = FastAPI()

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


@app.get("/geojson/{clave}")
async def get_geojson(clave: str):
  return FileResponse(f"data/processed/{clave}.geojson")


@app.get("/query")
async def custom_query(metric: str, condition: str = None):
  print(condition, metric)
  if condition:
    data = get_all(cursor, f'''SELECT CLAVE_LOTE as clave, ({metric}) As value FROM predios WHERE {condition}''')
  else:
    data = get_all(cursor, f'''SELECT CLAVE_LOTE as clave, ({metric}) As value FROM predios''')
  print(len(data))
  return data


WALK_RADIUS = 1609.34 / 4
SECTORS = ['comercio', 'servicios', 'salud', 'educacion']


def get_nearby_services(gdf_lots, gdf_denue, radius=WALK_RADIUS, sectors=SECTORS):
  gdf_lots2 = gdf_lots.to_crs('EPSG:3043').set_index('CLAVE_LOTE')
  gdf_lots2['geometry'] = gdf_lots2.geometry.buffer(radius)
  gdf_denue['services_nearby'] = 1
  joined_gdf = gpd.sjoin(gdf_lots2, gdf_denue.to_crs('EPSG:3043'), how='inner', predicate='intersects')
  joined_gdf = joined_gdf.groupby(joined_gdf.index).agg({
      'services_nearby': 'sum',
      'sector': lambda x: dict(x.value_counts())
  })
  joined_gdf['sector'] = joined_gdf['sector'].fillna({})
  for sector in sectors:
    adj = f'adj_{sector}'
    joined_gdf[adj] = joined_gdf['sector'].apply(lambda x: x.get(sector, 0) if isinstance(x, dict) else 0)
  joined_gdf['total_score'] = joined_gdf[[f'adj_{x}' for x in sectors]].sum(axis=1)
  joined_gdf = gdf_lots.set_index('CLAVE_LOTE').merge(joined_gdf, how='left', left_index=True, right_index=True)
  return joined_gdf


def calculate_walkability(
        gdf_lots,
        gdf_denue,
        walk_radius,
        decay_rate=0.5):
  gdf_lots_buffered = gdf_lots.to_crs('EPSG:3043').set_index('CLAVE_LOTE')
  gdf_lots_buffered['geometry'] = gdf_lots_buffered.geometry.buffer(walk_radius)
  gdf_denue = gdf_denue[gdf_denue.intersects(gdf_lots_buffered.to_crs('EPSG:4326').unary_union)]

  # Get pedestrian network
  north, south, east, west = gdf_denue.total_bounds
  G = ox.graph_from_bbox(north, south, east, west, network_type='walk', simplify=True)
  centroid = gdf_lots.geometry.centroid
  start_node = nearest_nodes(G, X=centroid.x.mean(), Y=centroid.y.mean())

  # Calculate nearest node and walking time for amenities
  def calculate_nearest_node(row):
    return nearest_nodes(G, X=row.geometry.centroid.x, Y=row.geometry.centroid.y)

  def calculate_walking_time(row):
    return calculate_walking_distance(G, start_node, row)

  gdf_denue['nearest_node'] = gdf_denue.apply(calculate_nearest_node, axis=1)
  gdf_denue['walking_time'] = gdf_denue.apply(calculate_walking_time, axis=1)

  # Filter amenities within a 15-minute walk and calculate scores
  within_15_min = gdf_denue[gdf_denue['walking_time'] <= 15]
  within_15_min['proximity_score'] = np.exp(-decay_rate * within_15_min['walking_time'])
  within_15_min['weighted_score'] = 1  # Assuming a placeholder for actual weighted scoring

  # Calculate diversity and proximity scores
  diversity_scores = within_15_min.groupby('sector')['weighted_score'].sum()
  total_diversity_score = diversity_scores.sum()
  total_proximity_score = within_15_min['proximity_score'].sum()
  final_walkability_score = total_proximity_score + total_diversity_score

  print(total_proximity_score, total_diversity_score, final_walkability_score)

  # Count accessible amenities by sector
  sector_counts = within_15_min['sector'].value_counts()
  print(sector_counts)

  # Update gdf_lots with adjacency counts
  gdf_lots['services_nearby'] = 0
  for sector, count in sector_counts.items():
    gdf_lots[f"adj_{sector.lower()}"] = count
    gdf_lots['services_nearby'] += count
  print(gdf_lots)

  # Return updated GeoDataFrame
  return gdf_lots, final_walkability_score


@app.get("/predios/")
async def get_info(predio: Annotated[list[str] | None, Query()] = None):
  data = get_all(cursor, f'''SELECT * FROM predios WHERE CLAVE_LOTE IN ({', '.join(predio)})''')
  # reduce data to only one predio, sum all the values
  df = pd.DataFrame(data)
  df = df.drop(columns=['CLAVE_LOTE', 'num_properties']).agg({
      "building_ratio": "mean",
      "unused_ratio": "mean",
      "green_ratio": "mean",
      "parking_ratio": "mean",
      "park_ratio": "mean",
      "wasteful_ratio": "mean",
      "utilization_ratio": "mean",
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
      "total_score": "mean",
      "adj_comercio": "mean",
      "adj_servicios": "mean",
      "adj_salud": "mean",
      "adj_educacion": "mean",
  })
  print(predio)
  gdf_lots = gpd.read_file("data/processed/predios.geojson", crs="EPSG:4326")
  gdf_lots = gdf_lots[gdf_lots['CLAVE_LOTE'].isin(predio)]
  gdf_denue = gpd.read_file("data/processed/denue.geojson", crs="EPSG:4326")
  gdf_lots['latitud'] = gdf_lots['geometry'].centroid.y
  gdf_lots['longitud'] = gdf_lots['geometry'].centroid.x
  # gdf_lots, final_walkability_score = calculate_walkability(gdf_lots, gdf_denue, WALK_RADIUS)
  # first = gdf_lots.drop(columns=['geometry', 'sector']).iloc[0]
  first = gdf_lots.drop(columns=['geometry']).iloc[0]
  print(df.to_dict())
  return {**df.to_dict(), **first.to_dict()}  # , "accessibility_score": final_walkability_score}


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
             'total_score',
             'combined_score']]
  pd.options.display.float_format = "{:,.2f}".format
  print(gdf.describe().transpose()[['mean', 'min', '25%', '50%', '75%', 'max']])
  # data = get_all(cursor, '''SELECT CLAVE_LOTE, utilization_ratio FROM predios WHERE POBTOT > 200''')
  # print(data)
