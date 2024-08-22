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
    WALK_RADIUS,
)


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
            gdf_intersect = gdf_intersect[gdf_intersect.geom_type.isin(["Polygon", "MultiPolygon"])]
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
    parser.add_argument("bounds_file", type=str,
                        help="The file with all the data")
    parser.add_argument(
        "lots_establishments_file", type=str, help="The file with all the data"
    )
    parser.add_argument("buildings_file", type=str,
                        help="The file with all the data")
    parser.add_argument(
        "establishments_file", type=str, help="The file with all the data"
    )
    parser.add_argument(
        "amenities_file", type=str, help="The file with all the data"
    )
    parser.add_argument("vegetation_file", type=str,
                        help="The file with all the data")
    parser.add_argument("output_folder", type=str,
                        help="The file with all the data")
    parser.add_argument("output_file", type=str,
                        help="The file with all the data")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    gdf_bounds = gpd.read_file(args.bounds_file, crs="EPSG:4326")
    gdf_bounds = (
        gdf_bounds.to_crs("EPSG:32614")
        .buffer(WALK_RADIUS)
        .to_crs("EPSG:4326")
        .unary_union
    )
    # gdf_bounds = gpd.read_file(args.bounds_file, crs="EPSG:4326").unary_union
    gdf_establishments = gpd.read_file(
        args.establishments_file, engine="pyogrio").to_crs("EPSG:4326")
    gdf_lots = gpd.read_file(
        args.lots_establishments_file, engine="pyogrio").to_crs("EPSG:4326")
    gdf_lots["lot_area"] = gdf_lots.to_crs("EPSG:6933").area / 10_000
    gdf_amenities_extra = gpd.read_file(
        args.amenities_file, engine="pyogrio").to_crs("EPSG:4326")
    gdf_amenities_extra = gdf_amenities_extra.rename({"NOM2_2_ACT": "amenity", "DESCRIP": "name"}, axis=1)[
        ["amenity", "name", "geometry"]]
    gdf_amenities_extra = gdf_amenities_extra[gdf_amenities_extra["amenity"].notnull(
    )]
    rename_mapping = {
        'PARQUE': 'Parques recreativos',
        'EDUCACION': 'Educación',
        'EDUCACION BASICA': 'Educación Primaria',
        'BASICO': 'Educación Primaria',
        'INTERMEDIA': 'DIF',
        'INTERMEDIO': 'Educación Secundaria',
        'NIVEL MEDIO SUPERIOR': 'Educación Media Superior',
        'NIVEL SUPERIOR': 'Educación Superior',
        'EDUCACION ESPECIAL': 'Educación Especial',
        'EDUCACION EXTRAESCOLAR': 'Educación Extraescolar',
        'ASISTENCIA SOCIAL': 'Asistencia Social',
        'DEPORTE': 'Clubs deportivos y de acondicionamiento físico',
        'ABASTO': 'Abasto',
        'PRIMER NIVEL': 'Hospital general',
        'SEGUNDO NIVEL': 'Hospital general',
        'TERCER NIVEL': 'Hospital general',
        'SALUD': 'Hospital general',
        'CULTURA Y RECREACION': 'Otros Servicios recreativos',
        'PLAZA': 'Parques recreativos',
        # 'ADMINISTRACION PUBLICA': 'Administración Pública',
        # 'CAMELLONES': 'park',
        # 'JARDIN': 'park',
        # 'SIN USO': 'Sin Uso',
        # 'INVADIDO': 'Invadido',
        # 'ESPACIOS PUBLICOS DE TRANSICION': 'park',
        # 'ANP': 'Área Natural Protegida',
    }
    # rename the amenities and remove the ones that are not in the mapping
    gdf_amenities_extra["amenity"] = gdf_amenities_extra["amenity"].replace(
        rename_mapping)
    gdf_amenities_extra = gdf_amenities_extra[gdf_amenities_extra["amenity"].isin(
        rename_mapping.values())]
    gdf_amenities_extra = gdf_amenities_extra[gdf_amenities_extra.within(
        gdf_bounds)]

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
        pd.concat([gdf_service_highways, gdf_parking_amenities],
                  ignore_index=True),
        crs="EPSG:4326",
    )
    unified_geometry = gdf_combined.dissolve().buffer(BUFFER_PARKING).unary_union
    external_polygons = [Polygon(poly.exterior)
                         for poly in unified_geometry.geoms]
    gdf_parking = gpd.GeoDataFrame(geometry=external_polygons, crs="EPSG:4326")

    # Load the polygons for parks
    gdf_parks = ox.geometries_from_polygon(
        gdf_bounds, tags=PARK_TAGS).reset_index()
    gdf_parks = gdf_parks[gdf_parks["element_type"] != "node"]
    gdf_parks["geometry"] = gdf_parks["geometry"].intersection(gdf_bounds)

    # Load the polygons for equipment equipments (schools, universities and places of worship)
    gdf_equipment = ox.geometries_from_polygon(
        gdf_bounds, tags=EQUIPMENT_TAGS
    ).reset_index()
    gdf_equipment = gdf_equipment[gdf_equipment["element_type"] != "node"]
    gdf_equipment["geometry"] = gdf_equipment["geometry"].intersection(
        gdf_bounds)

    gdf_buildings = gpd.read_file(
        args.buildings_file, engine="pyogrio").to_crs("EPSG:4326")
    gdf_vegetation = gpd.read_file(args.vegetation_file, engine="pyogrio").to_crs("EPSG:4326").reset_index(
        drop=True
    )
    gdf_parks["amenity"] = "park"
    gdf_equipment["amenity"] = (
        gdf_equipment["amenity"]
        .fillna(gdf_equipment["leisure"])
        .fillna(gdf_equipment["building"])
        .fillna(gdf_equipment["landuse"])
    )
    gdf_amenities = gpd.GeoDataFrame(
        pd.concat([gdf_parks, gdf_equipment], ignore_index=True), crs="EPSG:4326"
    )
    # remove amenities from gdf_amenities that are inside gdf_amenities_extra
    gdf_amenities = gdf_amenities[~gdf_amenities.intersects(
        gdf_amenities_extra.unary_union)]
    gdf_amenities = pd.concat(
        [gdf_amenities, gdf_amenities_extra], ignore_index=True)
    gdf_amenities = cut_out_inner_polygons(gdf_amenities)
    gdf_amenities = gdf_amenities[["geometry", "amenity", "name"]]
    # consider only those larger than 0.1 quantile
    gdf_amenities = gdf_amenities.loc[gdf_amenities.geometry.area > gdf_amenities.geometry.area.quantile(0.01)]
    gdf_buildings = gdf_buildings[["geometry"]]

    print(gdf_amenities)
    print(gdf_amenities.geometry.area.describe())
    gdf_amenities.plot()
    plt.show()

    list_gdfs = [gdf_buildings, gdf_parking, gdf_amenities, gdf_vegetation]
    gdfs = overlay_multiple(gdf_lots, list_gdfs)

    establishments_amenities = gdf_establishments[gdf_establishments["amenity"].notnull()].rename({
        "ID_lot": "ID", "nom_estab": "name"}, axis=1)
    gdfs[2] = gpd.GeoDataFrame(
        pd.concat([gdfs[2], establishments_amenities],
                  ignore_index=True),
        crs="EPSG:4326",
    )
    gdfs[0] = gdfs[0][["ID", "geometry", "lot_area"]]
    gdfs[1] = gdfs[1][["ID", "geometry", "lot_area"]]
    gdfs[2] = gdfs[2][["ID", "amenity", "name", "geometry", "lot_area"]]
    gdfs[3] = gdfs[3][["ID", "geometry", "lot_area"]]
    gdfs[4] = gdfs[4][["ID", "geometry", "lot_area"]]

    for gdf, item in zip(gdfs, GDFS_MAPPING):
        column_area = f'{item["name"]}_area'
        column_ratio = f'{item["name"]}_ratio'
        gdf = gdf.dissolve(by="ID")
        gdf[column_area] = gdf.to_crs("EPSG:6933").area / 10_000
        gdf[column_ratio] = gdf[column_area] / gdf["lot_area"]
        gdf = gdf.reset_index()
        gdf.to_file(args.output_folder.format(item["name"]), engine="pyogrio")
        gdf_lots = gdf_lots.merge(
            gdf[["ID", column_area, column_ratio]],
            on="ID",
            how="left",
        )
        gdf_lots[[column_area, column_ratio]] = gdf_lots[
            [column_area, column_ratio]
        ].fillna(0)

    gdf_lots.to_file(args.output_file, engine="pyogrio")

    if args.view:
        fig, ax = plt.subplots()
        for gdf, item in zip(gdfs, GDFS_MAPPING):
            gdf.plot(ax=ax, color=item["color"], alpha=0.5)
        plt.show()
