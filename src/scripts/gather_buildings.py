import argparse

import ee
import geopandas as gpd
import matplotlib.pyplot as plt
import requests
from geopandas import GeoDataFrame

from src.scripts.utils.constants import BUILDING_CONFIDENCE
from src.scripts.utils.utils import gdf_to_ee_polygon


def process_buildings(gdf_bounds: GeoDataFrame) -> GeoDataFrame:
    ee_polygon = gdf_to_ee_polygon(gdf_bounds)
    image_collection = (
        ee.FeatureCollection("GOOGLE/Research/open-buildings/v3/polygons")
        .filterBounds(ee_polygon)
        .filter(f"confidence >= {BUILDING_CONFIDENCE}")
    )
    download_url = image_collection.getDownloadURL(
        filetype="geojson", filename="open_buildings"
    )
    response = requests.get(download_url)
    return gpd.read_file(
        response.content.decode("utf-8"), driver="GeoJSON", crs="EPSG:4326"
    )


def get_args():
    parser = argparse.ArgumentParser(description="Join establishments with lots")
    parser.add_argument("bounds_file", type=str, help="The file with all the data")
    parser.add_argument("output_file", type=str, help="The file with all the data")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    ee.Initialize()
    args = get_args()
    gdf_bounds = gpd.read_file(args.bounds_file, crs="EPSG:4326")
    gdf_buildings = process_buildings(gdf_bounds)
    gdf_buildings.to_file(args.output_file)
    if args.view:
        gdf_buildings.plot()
        plt.show()
