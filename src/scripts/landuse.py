import argparse

import geopandas as gpd
import matplotlib.pyplot as plt
import osmnx as ox
import pandas as pd
from shapely.geometry import Polygon

from src.scripts.utils.constants import (
    BUFFER_PARKING,
    EQUIPMENT_TAGS,
    GDFS_MAPPING,
    PARK_TAGS,
    PARKING_FILTER,
    PARKING_TAGS,
    WALK_RADIUS,
    AMENITIES_FILE_MAPPING,
    BOUNDS_FILE,
    ESTABLISHMENTS_LOTS_FILE,
    LANDUSE_LOTS_FILE,
    ASSIGN_ESTABLISHMENTS_FILE,
    BUILDING_FILE,
    VEGETATION_FILE,
)
from src.scripts.utils.utils import load_gdf


def cut_out_inner_polygons(gdf):
    # Create an empty list to store the resulting geometries
    result_geometries = []

    # Iterate over each polygon in the GeoDataFrame
    for i, outer_polygon in gdf.iterrows():
        # Start with the outer polygon
        cut_polygon = outer_polygon.geometry

        # Iterate again to find inner polygons
        for j, inner_polygon in gdf.iterrows():
            if i != j:
                # Subtract the inner polygon from the outer polygon
                if cut_polygon.contains(inner_polygon.geometry):
                    cut_polygon = cut_polygon.difference(
                        inner_polygon.geometry)

        # Add the resulting geometry to the list
        result_geometries.append(cut_polygon)

    # Create a new GeoDataFrame with the resulting geometries
    gdf["geometry"] = result_geometries

    return gdf


def overlay_multiple(
    gdf_initial: gpd.GeoDataFrame, gdfs: list[gpd.GeoDataFrame]
) -> gpd.GeoDataFrame:
    previous_gdfs = []
    gdf_residual = gdf_initial.reset_index()
    cumulative_overlay = gpd.GeoDataFrame(geometry=[])

    for gdf in gdfs:
        gdf = gdf.explode()
        gdf = gdf[gdf.geom_type.isin(["Polygon", "MultiPolygon"])]
        gdf_residual = gdf_residual.overlay(gdf, how="difference")

        gdf_filtered = gdf[gdf.geom_type.isin(["Polygon", "MultiPolygon"])]
        gdf_intersect = gdf_initial.reset_index().overlay(
            gdf_filtered, how="intersection", keep_geom_type=False
        )
        if not cumulative_overlay.empty:
            gdf_intersect = gdf_intersect[gdf_intersect.geom_type.isin(
                ["Polygon", "MultiPolygon"])]
            gdf_intersect = gdf_intersect.overlay(
                cumulative_overlay, how="difference")

        cumulative_overlay = gpd.GeoDataFrame(
            pd.concat([cumulative_overlay, gdf], ignore_index=True)
        )
        previous_gdfs.append(gdf_intersect)

    previous_gdfs.append(gdf_residual)
    return previous_gdfs[::-1]


def get_args():
    parser = argparse.ArgumentParser(
        description="Join establishments with lots")
    parser.add_argument("input_dir", type=str,
                        help="The folder all the original data")
    parser.add_argument("output_dir", type=str,
                        help="The folder to save the output data")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()

LANDUSE_FILE = "landuse_{}.fgb"
if __name__ == "__main__":
    args = get_args()

    gdf_bounds = gpd.read_file(f"{args.input_dir}/{BOUNDS_FILE}", crs="EPSG:4326")
    # gdf_bounds = gdf_bounds.to_crs("EPSG:32614").buffer(WALK_RADIUS).to_crs("EPSG:4326")
    gdf_denue = load_gdf(f"{args.output_dir}/{ASSIGN_ESTABLISHMENTS_FILE}", gdf_bounds)
    gdf_lots = load_gdf(f"{args.output_dir}/{ESTABLISHMENTS_LOTS_FILE}", gdf_bounds)
    gdf_lots["lot_area"] = gdf_lots.to_crs("EPSG:6933").area / 10_000

    # Load the polygons for parking lots
    G_service_highways = ox.graph_from_polygon(
        gdf_bounds.unary_union,
        custom_filter=PARKING_FILTER,
        network_type="all",
        retain_all=True,
    )
    gdf_service_highways = ox.graph_to_gdfs(
        G_service_highways, nodes=False
    ).reset_index()
    gdf_parking_amenities = ox.geometries_from_polygon(
        gdf_bounds.unary_union, tags=PARKING_TAGS
    ).reset_index()
    gdf_parking_amenities = gdf_parking_amenities[
        (gdf_parking_amenities["element_type"] != "node")
    ]
    gdf_parking_amenities["geometry"] = gdf_parking_amenities["geometry"].intersection(
        gdf_bounds.unary_union
    )
    gdf_combined = gpd.GeoDataFrame(
        pd.concat([gdf_service_highways, gdf_parking_amenities],
                  ignore_index=True),
        crs="EPSG:4326",
    )
    unified_geometry = gdf_combined.dissolve().buffer(BUFFER_PARKING).unary_union
    external_polygons = [Polygon(poly.exterior)
                         for poly in unified_geometry.geoms]
    gdf_parking = gpd.GeoDataFrame(geometry=external_polygons, crs="EPSG:4326")

    gdf_buildings = load_gdf(f"{args.output_dir}/{BUILDING_FILE}")
    gdf_vegetation = load_gdf(f"{args.output_dir}/{VEGETATION_FILE}").reset_index(drop=True)
    gdf_buildings = gdf_buildings[["geometry"]]

    list_gdfs = [gdf_buildings, gdf_parking, gdf_vegetation]
    gdfs = overlay_multiple(gdf_lots, list_gdfs)

    gdfs[0] = gdfs[0][["lot_id", "geometry", "lot_area"]]
    gdfs[1] = gdfs[1][["lot_id", "geometry", "lot_area"]]
    gdfs[2] = gdfs[2][["lot_id", "geometry", "lot_area"]]
    gdfs[3] = gdfs[3][["lot_id", "geometry", "lot_area"]]

    for gdf, item in zip(gdfs, GDFS_MAPPING):
        column_area = f'{item["name"]}_area'
        column_ratio = f'{item["name"]}_ratio'
        gdf = gdf.dissolve(by="lot_id").reset_index()
        gdf[column_area] = gdf.to_crs("EPSG:6933").area / 10_000
        gdf[column_ratio] = gdf[column_area] / gdf["lot_area"]
        gdf.to_file(f"{args.output_dir}/{LANDUSE_FILE.format(item['name'])}", engine="pyogrio")
        gdf_lots = gdf_lots.merge(
            gdf[["lot_id", column_area, column_ratio]],
            on="lot_id",
            how="left",
        )
        gdf_lots[[column_area, column_ratio]] = gdf_lots[
            [column_area, column_ratio]
        ].fillna(0)

    gdf_lots.to_file(f"{args.output_dir}/{LANDUSE_LOTS_FILE}", engine="pyogrio")

    if args.view:
        fig, ax = plt.subplots()
        for gdf, item in zip(gdfs, GDFS_MAPPING):
            gdf.plot(ax=ax, color=item["color"], alpha=0.5)
        plt.show()
