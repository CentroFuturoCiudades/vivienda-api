import argparse

import geopandas as gpd
import matplotlib.pyplot as plt
import osmnx as ox
import pandas as pd
from shapely.geometry import Polygon

from utils.constants import (
    BUFFER_PARKING,
    EQUIPMENT_TAGS,
    GDFS_MAPPING,
    PARK_TAGS,
    PARKING_FILTER,
    PARKING_TAGS,
)


def overlay_multiple(
    gdf_initial: gpd.GeoDataFrame, gdfs: list[gpd.GeoDataFrame]
) -> gpd.GeoDataFrame:
    previous_gdfs = []
    gdf_residual = gdf_initial.reset_index()
    cumulative_overlay = gpd.GeoDataFrame(geometry=[])

    for gdf in gdfs:
        gdf = gdf.explode()
        gdf = gdf[gdf.geometry.type.isin(["Polygon", "MultiPolygon"])][["geometry"]]
        gdf_residual = gdf_residual.overlay(gdf, how="difference")

        gdf_filtered = gdf[gdf.geometry.type.isin(["Polygon", "MultiPolygon"])]
        gdf_intersect = gdf_initial.reset_index().overlay(
            gdf_filtered, how="intersection", keep_geom_type=False
        )
        if not cumulative_overlay.empty:
            gdf_intersect = gdf_intersect.overlay(cumulative_overlay, how="difference")

        cumulative_overlay = gpd.GeoDataFrame(
            pd.concat([cumulative_overlay, gdf], ignore_index=True)
        )
        previous_gdfs.append(gdf_intersect)

    previous_gdfs.append(gdf_residual)
    return previous_gdfs[::-1]


def get_args():
    parser = argparse.ArgumentParser(description="Join establishments with lots")
    parser.add_argument("bounds_file", type=str, help="The file with all the data")
    parser.add_argument(
        "lots_establishments_file", type=str, help="The file with all the data"
    )
    parser.add_argument("buildings_file", type=str, help="The file with all the data")
    parser.add_argument("vegetation_file", type=str, help="The file with all the data")
    parser.add_argument("output_folder", type=str, help="The file with all the data")
    parser.add_argument("output_file", type=str, help="The file with all the data")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    gdf_bounds = gpd.read_file(args.bounds_file, crs="EPSG:4326").unary_union
    gdf_lots = gpd.read_file(args.lots_establishments_file, crs="EPSG:4326")
    gdf_lots["lot_area"] = gdf_lots.to_crs("EPSG:6933").area / 10_000

    # Load the polygons for parking lots
    G_service_highways = ox.graph_from_polygon(
        gdf_bounds,
        custom_filter=PARKING_FILTER,
        network_type="all",
        retain_all=True,
    )
    gdf_service_highways = ox.graph_to_gdfs(
        G_service_highways, nodes=False
    ).reset_index()
    gdf_parking_amenities = ox.geometries_from_polygon(
        gdf_bounds, tags=PARKING_TAGS
    ).reset_index()
    gdf_parking_amenities = gdf_parking_amenities[
        (gdf_parking_amenities["element_type"] != "node")
    ]
    gdf_parking_amenities["geometry"] = gdf_parking_amenities["geometry"].intersection(
        gdf_bounds
    )
    gdf_combined = gpd.GeoDataFrame(
        pd.concat([gdf_service_highways, gdf_parking_amenities], ignore_index=True),
        crs="EPSG:4326",
    )
    unified_geometry = gdf_combined.dissolve().buffer(BUFFER_PARKING).unary_union
    external_polygons = [Polygon(poly.exterior) for poly in unified_geometry.geoms]
    gdf_parking = gpd.GeoDataFrame(geometry=external_polygons, crs="EPSG:4326")

    # Load the polygons for parks
    gdf_parks = ox.geometries_from_polygon(gdf_bounds, tags=PARK_TAGS).reset_index()
    gdf_parks = gdf_parks[gdf_parks["element_type"] != "node"]
    gdf_parks["geometry"] = gdf_parks["geometry"].intersection(gdf_bounds)

    # Load the polygons for equipment equipments (schools, universities and places of worship)
    gdf_equipment = ox.geometries_from_polygon(
        gdf_bounds, tags=EQUIPMENT_TAGS
    ).reset_index()
    gdf_equipment = gdf_equipment[gdf_equipment["element_type"] != "node"]
    gdf_equipment["geometry"] = gdf_equipment["geometry"].intersection(gdf_bounds)

    gdf_buildings = gpd.read_file(args.buildings_file, crs="EPSG:4326")
    gdf_vegetation = gpd.read_file(args.vegetation_file, crs="EPSG:4326").reset_index(
        drop=True
    )

    gdfs = overlay_multiple(
        gdf_lots, [gdf_buildings, gdf_equipment, gdf_parks, gdf_parking, gdf_vegetation]
    )
    for gdf, item in zip(gdfs, GDFS_MAPPING):
        gdf = gdf.set_index("ID").dissolve(by="ID")
        column_area = f'{item["name"]}_area'
        column_ratio = f'{item["name"]}_ratio'
        gdf[column_area] = gdf.to_crs("EPSG:6933").area / 10_000
        gdf[column_ratio] = gdf[column_area] / gdf["lot_area"]
        gdf = gdf.reset_index()[["ID", column_area, column_ratio, "geometry"]]
        gdf.to_file(args.output_folder.format(item["name"]))
        gdf_lots = gdf_lots.merge(gdf.drop(columns="geometry"), on="ID", how="left")
        gdf_lots[[column_area, column_ratio]] = gdf_lots[
            [column_area, column_ratio]
        ].fillna(0)

    gdf_lots.to_file(args.output_file)

    if args.view:
        fig, ax = plt.subplots()
        for gdf, item in zip(gdfs, GDFS_MAPPING):
            gdf.plot(ax=ax, color=item["color"], alpha=0.5)
        plt.show()
