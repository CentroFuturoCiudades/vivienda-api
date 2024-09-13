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

from utils.utils import GEOJSONS_DIR, HEX_ID, HEX_RESOLUTION, RASTERS_DIR, YEARS

REDUCE_DISTANCE = 10
BUFFER_DISTANCE = 0
THRESHOLD = 0.2


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


def disjunct_rasters(paths_rasters):
    # Get rasters data
    rasters = [rioxarray.open_rasterio(x) for x in paths_rasters]
    rasters = [x.copy(data=np.where(x.values > THRESHOLD, 1, 0)) for x in rasters]
    disjuncted_rasters = [rioxarray.open_rasterio(x) for x in paths_rasters]
    disjuncted_rasters = [
        x.copy(data=np.where(x.values > THRESHOLD, 1, 0)) for x in disjuncted_rasters
    ]

    # Remove urbanized area from previous years
    for i in range(1, len(rasters)):
        disjuncted_rasters[i] = disjuncted_rasters[i] - rasters[i - 1]

    return disjuncted_rasters


def convert_to_geojson(paths_rasters: list[str], years: list[str]):
    disjuncted_rasters = disjunct_rasters(paths_rasters)

    # Transform rasters to GeoDataFrames
    gdfs = [to_gdf(x) for x in disjuncted_rasters]

    # Join all geodataframes into one with a year feature
    for gdf, year in zip(gdfs, years):
        gdf["year"] = year
    gdf = pd.concat(gdfs)

    gdf = gdf.set_crs("ESRI:54009", allow_override=True)
    gdf = gdf.to_crs("EPSG:4326")

    # gdf['geometry'] = gdf['geometry'].make_valid().simplify(REDUCE_DISTANCE).buffer(BUFFER_DISTANCE)
    return gdf


def add_geometry(row) -> Polygon:
    points = h3.h3_to_geo_boundary(row[HEX_ID], True)
    return Polygon(points)


def as_h3(gdf: GeoDataFrame) -> GeoDataFrame:
    gdf[HEX_ID] = gdf.apply(
        lambda row: h3.geo_to_h3(row.geometry.y, row.geometry.x, HEX_RESOLUTION), axis=1
    )
    # h3_df = gdf.groupby([HEX_ID])['year'].median().reset_index()
    h3_df = gdf.groupby([HEX_ID])["year"].agg(Counter).reset_index()
    all_years = pd.concat([pd.Series(c) for c in h3_df["year"]], axis=1)
    max_counts = all_years.fillna(0).max(axis=1).to_dict()

    h3_df["year"] = h3_df["year"].apply(
        lambda row: {k: v / max_counts[k] for k, v in row.items()}
    )
    h3_df["year"] = h3_df["year"].apply(lambda x: max(x, key=x.get))

    h3_geoms = h3_df.apply(add_geometry, axis=1)
    h3_df = GeoDataFrame(h3_df, geometry=h3_geoms, crs=4326)

    return h3_df


years_tif = tuple([f"{x}.tif" for x in YEARS])

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert rasters of provided cities (or all cities) to geojsons."
    )
    parser.add_argument("-c", "--cities", nargs="*")
    args = parser.parse_args()

    if not args.cities:
        args.cities = os.listdir(RASTERS_DIR)

    for city in args.cities:
        raster_files = sorted(
            [os.path.join(RASTERS_DIR, city, f"{x}.tif") for x in YEARS]
        )
        print("Rasters to be converted")
        for x in raster_files:
            print(f" - {x}")
        gdf = convert_to_geojson(raster_files, YEARS)
        # gdf = gdf.dissolve()
        # gdf = gdf.simplify(10)
        # gdf = as_h3(gdf)

        # Save to file
        os.makedirs(os.path.join(GEOJSONS_DIR, city), exist_ok=True)
        output_file = os.path.join(GEOJSONS_DIR, city, "expansion.geojson")
        gdf.to_file(output_file, driver="GeoJSON")
        print(f"Converted {city} raster to geojson in {output_file}")
