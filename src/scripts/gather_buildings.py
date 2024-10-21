import argparse

import ee
import geopandas as gpd
import matplotlib.pyplot as plt
import requests
from geopandas import GeoDataFrame
import osmnx as ox
import pandas as pd

from src.scripts.utils.constants import BOUNDS_FILE, BUILDING_FILE, BUILDING_CONFIDENCE
from src.scripts.utils.utils import gdf_to_ee_polygon


def process_buildings_in_chunks(gdf_bounds: GeoDataFrame, chunk_size: int = 100000) -> GeoDataFrame:
    ee_polygon = gdf_to_ee_polygon(gdf_bounds)
    image_collection = (
        ee.FeatureCollection("GOOGLE/Research/open-buildings/v3/polygons")
        .filterBounds(ee_polygon)
        .filter(f"confidence >= {BUILDING_CONFIDENCE}")
    )

    # Split into chunks to avoid memory issues
    feature_list = image_collection.toList(image_collection.size())
    total_features = feature_list.size().getInfo()
    print(len(total_features))
    
    all_chunks = []
    for i in range(0, total_features, chunk_size):
        print(f"Processing chunk {i//chunk_size + 1}/{total_features//chunk_size + 1}")
        chunk = feature_list.slice(i, i + chunk_size)
        chunk_collection = ee.FeatureCollection(chunk)
        
        # Get download URL for the current chunk
        download_url = chunk_collection.getDownloadURL(
            filetype="geojson", filename=f"open_buildings_chunk_{i//chunk_size}"
        )
        
        # Download and read in chunks
        response = requests.get(download_url)
        gdf_chunk = gpd.read_file(
            response.content.decode("utf-8"), driver="GeoJSON", crs="EPSG:4326"
        )
        all_chunks.append(gdf_chunk)
    
    # Combine all chunks into one GeoDataFrame
    gdf_buildings = gpd.GeoDataFrame(pd.concat(all_chunks, ignore_index=True))
    
    return gdf_buildings


def get_args():
    parser = argparse.ArgumentParser(description="Join establishments with lots")
    parser.add_argument("input_dir", type=str,
                        help="The folder all the original data")
    parser.add_argument("output_dir", type=str,
                        help="The folder to save the output data")
    parser.add_argument("city_name", type=str,
                        help="The name of the city to process")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    ee.Initialize()
    args = get_args()
    # gdf_bounds = gpd.read_file(f"{args.input_dir}/{BOUNDS_FILE}", crs="EPSG:4326")
    gdf_bounds = gpd.read_file(f"{args.output_dir}/blocks.fgb")
    gdf_bounds['geometry'] = gdf_bounds.buffer(0.005).simplify(0.1)
    gdf_bounds = gdf_bounds[['geometry']].dissolve()
    gdf_bounds.to_file(f"{args.output_dir}/bounds_{args.city_name}.fgb")
    gdf_buildings = process_buildings_in_chunks(gdf_bounds)
    gdf_buildings.to_file(f"{args.output_dir}/{BUILDING_FILE}")
    if args.view:
        gdf_buildings.plot()
        plt.show()
