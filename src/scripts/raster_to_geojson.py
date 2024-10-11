import argparse
import os
from collections import Counter

import geopandas as gpd
import h3
import numpy as np
import pandas as pd
import rioxarray
from geopandas import GeoDataFrame
from shapely import geometry
from shapely.geometry import Polygon

from src.scripts.utils.utils import GEOJSONS_DIR, HEX_ID, HEX_RESOLUTION, RASTERS_DIR, YEARS


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

