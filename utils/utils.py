import os
from functools import lru_cache

import ee
import geopandas as gpd
import numpy as np
import osmnx as ox
import pandas as pd
from azure.storage.blob import BlobClient
from dotenv import load_dotenv
from geopandas import GeoDataFrame
from shapely.geometry import box
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sqlalchemy import create_engine

APP_ENV = os.getenv('APP_ENV', 'local')
load_dotenv(f'.env.{APP_ENV}')

RASTERS_DIR = "data/rasters"
GEOJSONS_DIR = "data/geojsons"
CITIES_CSV = "data/municipios.csv"
DEFAULT_YEAR_START = 1975
DEFAULT_YEAR_END = 2020
DEFAULT_YEAR_INTERVAL = 5
YEARS = np.arange(
    DEFAULT_YEAR_START, DEFAULT_YEAR_END + DEFAULT_YEAR_INTERVAL, DEFAULT_YEAR_INTERVAL
).astype(int)
HEX_RESOLUTION = 9
HEX_ID = f"hex_id_{HEX_RESOLUTION}"
COLUMN_ID = "ID"
BLOB_URL = "https://reimaginaurbanostorage.blob.core.windows.net"

@lru_cache()
def get_engine():
    if APP_ENV == "local":
        db_path = os.getenv("LOCAL_DB_PATH")
        print(db_path)
        return create_engine(db_path)
    else:
        driver = os.getenv("SQL_DRIVER")
        server = os.getenv("SQL_SERVER")
        database = os.getenv("SQL_DATABASE")
        username = os.getenv("SQL_USERNAME")
        password = os.getenv("SQL_PASSWORD")
        connection_string = f"DRIVER={driver};SERVER=tcp:{server},1433;DATABASE={database};UID={username};PWD={password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
        return create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}")

def get_all(query):
    engine = get_engine()
    df = pd.read_sql(query, engine)
    print(df)
    return df


def gdf_to_ee_polygon(gdf: gpd.GeoDataFrame):
    polygon_geojson = gdf.geometry.iloc[0].__geo_interface__
    geom_type = polygon_geojson["type"]

    if geom_type == "Polygon":
        coords = polygon_geojson["coordinates"][0]
    elif geom_type == "MultiPolygon":
        # This flattens the list of lists for multipolygon to fit into ee.Geometry.Polygon
        coords = [coord for part in polygon_geojson["coordinates"] for coord in part[0]]
    else:
        raise ValueError(f"Unsupported geometry type: {geom_type}")

    ee_polygon = ee.Geometry.Polygon(coords)
    return ee_polygon


@lru_cache()
def load_file(folder: str, file_name: str):
    endpoint = f"{BLOB_URL}/{folder}/{file_name}"
    file = f"data/{folder}/{file_name}"
    if not os.path.exists(f"data/{folder}"):
        os.makedirs(f"data/{folder}")
    if os.path.exists(file_name):
        return file
    blob_client = BlobClient.from_blob_url(endpoint)
    with open(file, "wb") as tmp_file:
        tmp_file.write(blob_client.download_blob().readall())
        return file


def normalized_limit(x, min, max):
    if x < min:
        return 0
    elif x > max:
        return 1
    return (x - min) / (max - min) if max - min != 0 else 0


def normalize(column_data, min_value=min, max_value=max):
    if callable(min_value):
        min_value = min_value(column_data)
    if callable(max_value):
        max_value = max_value(column_data)
    return column_data.apply(lambda x: normalized_limit(x, min_value, max_value))


def combine_gdfs(gdf1: GeoDataFrame, gdf2: GeoDataFrame, on: str = "hex_id_9"):
    gdf = gdf1.merge(gdf2, on=on)
    gdf = gdf.rename(columns={"geometry_x": "geometry"})
    gdf = gdf.drop(columns=["geometry_y"])
    gdf = GeoDataFrame(gdf, geometry=gdf["geometry"], crs=4326).set_index(on)
    return gdf


def remove_outliers(item, lower, upper):
    Q1 = item.quantile(lower)
    Q3 = item.quantile(upper)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return item.clip(lower_bound, upper_bound)


def autocluster(items, range_clusters=(2, 11), sort_column=None):
    best_n_clusters = None
    best_score = -1

    for n_clusters in range(range_clusters[0], range_clusters[1]):
        kmeans = KMeans(n_clusters=n_clusters, random_state=0)
        cluster_labels = kmeans.fit_predict(items)
        score = silhouette_score(items, cluster_labels)
        if score > best_score:
            best_score = score
            best_n_clusters = n_clusters

    kmeans = KMeans(n_clusters=best_n_clusters, random_state=0)
    clustering = kmeans.fit_predict(items)

    df = pd.DataFrame(items)
    df["cluster"] = clustering
    if sort_column:
        cluster_means = df.groupby("cluster")[sort_column].mean()
        sorted_cluster_labels = cluster_means.sort_values().index
        label_map = {
            old_label: new_label
            for new_label, old_label in enumerate(sorted_cluster_labels)
        }
        df["cluster"] = df["cluster"].map(label_map)
    return df["cluster"]


fills = [2, 3, 4, 4, 3]
column_names = ["ENT", "MUN", "LOC", "AGEB", "MZA"]


def fill(df, columns, aggs=None):
    df["CVE_GEO"] = ""
    for column, amount in zip(columns, fills):
        if pd.api.types.is_numeric_dtype(df[column]):
            df[column] = df[column].astype(int)
        df["CVE_GEO"] += df[column].astype(str).str.zfill(amount)
        # if column != new_name:
        #   df = df.drop(column, axis=1)
    # new_names = [column_names[i] for i in range(len(columns))]
    # df = df.drop(columns=columns)
    # df = df.reset_index(drop=True).set_index('id')
    if "geometry" in df.columns:
        df = gpd.GeoDataFrame(df, geometry="geometry", crs=df.crs)
    return df


coordinates = 25.643375, -100.287677
centroid = gpd.points_from_xy([coordinates[1]], [coordinates[0]])[0]
distance = 0.03
bbox = [
    centroid.x - distance,
    centroid.y - distance,
    centroid.x + distance,
    centroid.y + distance,
]
boundaries = box(*bbox)


def fit_to_boundaries(gdf, gdf_bounds):
    return gdf.loc[gdf.geometry.intersects(gdf_bounds)]


def join(df1, df2, aggs):
    gdf_joined = df1.sjoin(df2, how="left", predicate="intersects")

    aggregated_data = gdf_joined.groupby(gdf_joined.index_right).agg(aggs)
    gdf_final = df2.merge(
        aggregated_data, how="left", left_index=True, right_on="index_right"
    )
    gdf_final = gdf_final.drop(columns=["index_right"])
    return gdf_final


def join_near(df1, df2, aggs, max_distance=0.00005):
    gdf_joined = df1.sjoin_nearest(df2, how="left", max_distance=max_distance)

    aggregated_data = gdf_joined.groupby(gdf_joined.index_right).agg(aggs)
    gdf_final = df2.merge(
        aggregated_data, how="left", left_index=True, right_on="index_right"
    )
    gdf_final = gdf_final.drop(columns=["index_right"])
    return gdf_final


def map_sector_to_sector(codigo_act):
    for sector in SECTORS_MAP:
        for low, high in sector["range"]:
            if low <= codigo_act <= high:
                return sector["sector"]


SECTORS_MAP = [
    {"range": [(11, 11)], "sector": "primario"},
    {"range": [(21, 33)], "sector": "industria"},
    {"range": [(43, 46)], "sector": "comercio"},
    {"range": [(51, 56)], "sector": "oficina"},
    {"range": [(48, 49), (81, 81)], "sector": "servicios"},
    {"range": [(62, 62)], "sector": "salud"},
    {"range": [(61, 61)], "sector": "educacion"},
    {"range": [(72, 72)], "sector": "comercio"},
    {"range": [(73, 100)], "sector": "gubernamental"},
]


def calculate_walking_distance(G, start_node, row):
    try:
        shortest_path_nodes = ox.distance.shortest_path(
            G, start_node, row["nearest_node"], weight="length"
        )
        length = sum(
            ox.utils_graph.get_route_edge_attributes(G, shortest_path_nodes, "length")
        )
        # Convert length to time if needed, assuming average walking speed
        walking_speed_m_per_s = 1.4  # Roughly 5 km/h
        walking_time_s = length / walking_speed_m_per_s
        return walking_time_s / 60  # Convert to minutes
    except BaseException:
        return np.nan  # Return NaN if no path found
