import argparse

import os
import ee
import geopandas as gpd
import matplotlib.pyplot as plt
import requests
from geopandas import GeoDataFrame
import osmnx as ox
import pandas as pd

from src.scripts.utils.constants import BOUNDS_FILE, BUILDING_FILE, BUILDING_CONFIDENCE
from src.scripts.utils.utils import gdf_to_ee_polygon

import aiohttp
import asyncio
import geopandas as gpd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

async def fetch_geojson(session, url, i, output_dir):
    filename = os.path.join(output_dir, f"buildings_chunk_{i}.geojson")
    
    # Check if file already exists
    if os.path.exists(filename):
        print(f"Chunk {i} already exists, skipping.")
        return None

    start_time = time.time()
    print(f"Start downloading chunk {i}")
    
    async with session.get(url) as response:
        if response.status == 429:
            print(f"Rate limit reached on chunk {i}. Retrying later.")
            return None
        data = await response.text()
        
        # Save to file after downloading
        with open(filename, 'w') as f:
            f.write(data)
        download_time = time.time() - start_time
        print(f"Finished downloading chunk {i}. Time taken: {download_time:.2f} seconds")
    
    return filename

async def download_in_parallel(urls, output_dir):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_geojson(session, url, i, output_dir) for i, url in enumerate(urls)]
        return await asyncio.gather(*tasks)

def prepare_chunk_url(i, feature_list, chunk_size):
    start_time = time.time()
    chunk = feature_list.slice(i, i + chunk_size)
    chunk_collection = ee.FeatureCollection(chunk)
    download_url = chunk_collection.getDownloadURL(
        filetype="geojson", filename=f"open_buildings_chunk_{i//chunk_size}"
    )
    prepare_time = time.time() - start_time
    print(f"Chunk {i//chunk_size + 1} prepared. Time taken: {prepare_time:.2f} seconds")
    return download_url

def process_buildings_in_chunks_parallel(gdf_bounds: GeoDataFrame, chunk_size: int = 10000, output_dir="chunks") -> GeoDataFrame:

    os.makedirs(output_dir, exist_ok=True)
    total_start_time = time.time()
    
    print("Start processing...")
    ee_polygon = gdf_to_ee_polygon(gdf_bounds)
    
    # Measure the time to get features
    feature_start_time = time.time()
    image_collection = (
        ee.FeatureCollection("GOOGLE/Research/open-buildings/v3/polygons")
        .filterBounds(ee_polygon)
        .filter(f"confidence >= {BUILDING_CONFIDENCE}")
    )

    feature_list = image_collection.toList(image_collection.size())
    total_features = feature_list.size().getInfo()
    feature_time = time.time() - feature_start_time
    print(f"Total features: {total_features}. Time to get features: {feature_time:.2f} seconds")

    # Prepare download URLs
    prepare_start_time = time.time()
    urls = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(prepare_chunk_url, i, feature_list, chunk_size) for i in range(0, 200000, chunk_size)]
        for future in as_completed(futures):
            urls.append(future.result())
    prepare_time = time.time() - prepare_start_time
    print(f"URLs prepared in parallel. Time taken: {prepare_time:.2f} seconds")

    print("Finished preparing all URLs")
    # print all urls
    for url in urls:
        print(url)

    # Download in parallel
    download_start_time = time.time()
    print("Start downloading...")
    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_in_parallel(urls, output_dir))
    download_time = time.time() - download_start_time
    print(f"Finished downloading all chunks. Time taken: {download_time:.2f} seconds")

    # Concatenate chunks
    concat_start_time = time.time()
    gdfs = []
    for filename in os.listdir(output_dir):
        if filename.endswith(".geojson"):
            gdf_chunk = gpd.read_file(os.path.join(output_dir, filename))
            gdfs.append(gdf_chunk)
    gdf_buildings = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
    concat_time = time.time() - concat_start_time
    print(f"Finished concatenating chunks. Time taken: {concat_time:.2f} seconds")

    total_time = time.time() - total_start_time
    print(f"Total time for process: {total_time:.2f} seconds")
    
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
    gdf_bounds = gpd.read_file(f"{args.input_dir}/bounds.fgb")
    gdf_buildings = process_buildings_in_chunks_parallel(gdf_bounds, output_dir=args.output_dir + "/chunks")
    gdf_buildings.to_file(f"{args.output_dir}/{BUILDING_FILE}")
    if args.view:
        gdf_buildings.plot()
        plt.show()
