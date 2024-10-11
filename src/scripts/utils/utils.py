import os
from functools import lru_cache

import ee
import geopandas as gpd
import numpy as np
import osmnx as ox
import pandas as pd
from dotenv import load_dotenv
from geopandas import GeoDataFrame
from shapely.geometry import box
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sqlalchemy import create_engine
import urllib.parse
from src.utils.files import get_file, get_blob_url

APP_ENV = os.getenv('APP_ENV', 'local')
load_dotenv(f'.env.{APP_ENV}')


def row2cell(row, res_xy):
    """Takes a row from the table and the resolution of the surface, and returns its geometry"""
    res_x, res_y = res_xy
    # XY Coordinates are centered on the pixel
    minX = row["x"] - (res_x / 2)
    maxX = row["x"] + (res_x / 2)
    minY = row["y"] + (res_y / 2)
    maxY = row["y"] - (res_y / 2)
    # Build squared polygon
    polygon = geometry.box(minX, minY, maxX, maxY)
    return polygon


def row2point(row, res_xy):
    """Convert raster cell row to a point (centroid of the cell)"""
    x, y = row["x"], row["y"]
    dx, dy = res_xy
    return gpd.points_from_xy([x + dx / 2], [y - dy / 2])[0]


def to_gdf(raster):
    """Transform a raster to a GeoDataFrame"""
    raster_geotable = raster.to_series().reset_index().rename(columns={0: "Value"})
    # Keep only cells with information
    raster_geotable = raster_geotable.query("Value > 0")
    # Build polygons for selected cells
    polygons = raster_geotable.apply(row2cell, res_xy=raster.rio.resolution(), axis=1)
    # Convert to GeoSeries
    features = polygons.pipe(gpd.GeoSeries, crs=raster.rio.crs)
    gdf = GeoDataFrame.from_features(features, crs=raster.rio.crs)
    # gdf = gdf.dissolve()
    return gdf


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


def fill(df, columns, aggs=None):
    fills = [2, 3, 4, 4, 3]
    column_names = ["ENT", "MUN", "LOC", "AGEB", "MZA"]
    df["CVE_GEO"] = ""
    for column, amount in zip(columns, fills):
        if pd.api.types.is_numeric_dtype(df[column]):
            df[column] = df[column].astype(int)
        df["CVE_GEO"] += df[column].astype(str).str.zfill(amount)
    if "geometry" in df.columns:
        df = gpd.GeoDataFrame(df, geometry="geometry", crs=df.crs)
    return df


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


def map_sector_to_sector(codigo_act: int) -> str:
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
