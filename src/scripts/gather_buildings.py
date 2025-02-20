from shapely.ops import unary_union
from shapely.geometry import shape
from rasterio.features import shapes
from rasterio.merge import merge
import rasterio
import zipfile
import argparse

import os
import ee
import geopandas as gpd
import matplotlib.pyplot as plt
import requests
from geopandas import GeoDataFrame
import osmnx as ox
import pandas as pd
from shapely.geometry import box

from src.scripts.utils.constants import BOUNDS_FILE, BUILDING_FILE, BUILDING_CONFIDENCE
from src.scripts.utils.utils import gdf_to_ee_polygon

import aiohttp
import asyncio
import geopandas as gpd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from scipy.ndimage import gaussian_filter


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
        print(f"Finished downloading chunk {
              i}. Time taken: {download_time:.2f} seconds")

    return filename


async def download_in_parallel(urls, output_dir):
    async with aiohttp.ClientSession() as session:
        tasks = [fetch_geojson(session, url, i, output_dir)
                 for i, url in enumerate(urls)]
        return await asyncio.gather(*tasks)


def prepare_chunk_url(i, feature_list, chunk_size):
    start_time = time.time()
    chunk = feature_list.slice(i, i + chunk_size)
    chunk_collection = ee.FeatureCollection(chunk)
    download_url = chunk_collection.getDownloadURL(
        filetype="geojson", filename=f"open_buildings_chunk_{i//chunk_size}"
    )
    prepare_time = time.time() - start_time
    print(f"Chunk {i//chunk_size +
          1} prepared. Time taken: {prepare_time:.2f} seconds")
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
    print(f"Total features: {total_features}. Time to get features: {
          feature_time:.2f} seconds")

    # Prepare download URLs
    prepare_start_time = time.time()
    urls = []
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(prepare_chunk_url, i, feature_list, chunk_size)
                   for i in range(0, 200000, chunk_size)]
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
    print(f"Finished downloading all chunks. Time taken: {
          download_time:.2f} seconds")

    # Concatenate chunks
    concat_start_time = time.time()
    gdfs = []
    for filename in os.listdir(output_dir):
        if filename.endswith(".geojson"):
            gdf_chunk = gpd.read_file(os.path.join(output_dir, filename))
            gdfs.append(gdf_chunk)
    gdf_buildings = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True))
    concat_time = time.time() - concat_start_time
    print(f"Finished concatenating chunks. Time taken: {
          concat_time:.2f} seconds")

    total_time = time.time() - total_start_time
    print(f"Total time for process: {total_time:.2f} seconds")

    return gdf_buildings


def download_chunk(url, chunk_id, output_dir):
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        zip_path = os.path.join(output_dir, f"chunk_{chunk_id}.zip")
        with open(zip_path, "wb") as zip_file:
            zip_file.write(response.content)
        print(f"Chunk {chunk_id} downloaded.")
        return zip_path
    else:
        raise RuntimeError(f"Failed to download chunk {
                           chunk_id}. HTTP Status: {response.status_code}")


def extract_rasters(zip_path, chunk_id, output_dir):
    chunk_dir = os.path.join(output_dir, f"chunk_{chunk_id}")
    os.makedirs(chunk_dir, exist_ok=True)

    with zipfile.ZipFile(zip_path, "r") as z:
        z.extractall(chunk_dir)

    rasters = {}
    for filename in os.listdir(chunk_dir):
        if "building_presence" in filename:
            rasters["presence"] = os.path.join(chunk_dir, filename)
        elif "building_height" in filename:
            rasters["height"] = os.path.join(chunk_dir, filename)

    if len(rasters) != 2:
        raise ValueError(f"Expected 2 rasters (presence and height) in chunk {
                         chunk_id}, but found {len(rasters)}.")

    print(f"Chunk {chunk_id} extracted: {rasters}")
    return rasters


def combine_rasters(raster_files, combined_output_file):
    presence_files = raster_files["presence"]
    height_files = raster_files["height"]

    # Merge presence rasters
    presence_sources = [rasterio.open(fp) for fp in presence_files]
    presence_merged, presence_transform = merge(presence_sources)
    for src in presence_sources:
        src.close()

    # Merge height rasters
    height_sources = [rasterio.open(fp) for fp in height_files]
    height_merged, height_transform = merge(height_sources)
    for src in height_sources:
        src.close()

    # Write the combined raster
    with rasterio.open(
        combined_output_file,
        "w",
        driver="GTiff",
        height=presence_merged.shape[1],
        width=presence_merged.shape[2],
        count=2,  # Two bands: presence and height
        dtype=presence_merged.dtype,
        crs=presence_sources[0].crs,
        transform=presence_transform,
    ) as dest:
        dest.write(presence_merged[0], 1)  # Band 1: presence
        dest.write(height_merged[0], 2)    # Band 2: height

    print(f"Combined raster saved as {combined_output_file}")


def process_image_collection_to_raster(
    gdf_bounds, combined_output_file, output_dir="chunks", chunk_size=0.05
):
    os.makedirs(output_dir, exist_ok=True)

    # Divide bounds into chunks
    bounds = gdf_bounds.total_bounds  # [minx, miny, maxx, maxy]
    xmin, ymin, xmax, ymax = bounds
    x_steps = int((xmax - xmin) / chunk_size) + 1
    y_steps = int((ymax - ymin) / chunk_size) + 1

    raster_files = {"presence": [], "height": []}

    for i in range(x_steps):
        for j in range(y_steps):
            chunk_id = f"{i}_{j}"
            chunk_xmin = xmin + i * chunk_size
            chunk_xmax = min(chunk_xmin + chunk_size, xmax)
            chunk_ymin = ymin + j * chunk_size
            chunk_ymax = min(chunk_ymin + chunk_size, ymax)

            # Define chunk geometry
            chunk_geom = box(chunk_xmin, chunk_ymin, chunk_xmax, chunk_ymax)
            chunk_gdf = gpd.GeoDataFrame(
                {"geometry": [chunk_geom]}, crs=gdf_bounds.crs)
            ee_polygon = gdf_to_ee_polygon(chunk_gdf)

            # Generate download URL
            try:
                image = (
                    ee.ImageCollection(
                        "GOOGLE/Research/open-buildings-temporal/v1")
                    .filterBounds(ee_polygon)
                    .mean()
                )
                url = image.getDownloadURL(
                    {"scale": 0.3, "region": ee_polygon, "fileFormat": "GeoTIFF"})
                zip_path = download_chunk(url, chunk_id, output_dir)
                rasters = extract_rasters(zip_path, chunk_id, output_dir)
                raster_files["presence"].append(rasters["presence"])
                raster_files["height"].append(rasters["height"])
            except Exception as e:
                print(f"Error processing chunk {chunk_id}: {e}")

    # Combine all rasters into a single file
    combine_rasters(raster_files, combined_output_file)


def raster_to_gdf(raster, band_index=1, threshold=0.3):
    band_data = raster.read(band_index)

    # Apply threshold (mask everything below the threshold)
    mask = band_data > threshold

    # Transform raster values to shapes
    print("Transforming raster to polygons...")
    transform = raster.transform
    shapes_gen = shapes(band_data, mask=mask, transform=transform)

    # Convert shapes to polygons
    polygons = []
    for geom, value in shapes_gen:
        if value > threshold:  # Ensure the value matches the threshold
            polygons.append(shape(geom))

    # Combine all polygons into a single geometry
    combined_geometry = unary_union(polygons)
    print(f"Generated combined geometry with {len(polygons)} parts.")

    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(
        {"geometry": [combined_geometry]},
        crs=raster.crs  # Use the CRS from the raster file
    )

    return gdf


def get_args():
    parser = argparse.ArgumentParser(
        description="Join establishments with lots")
    parser.add_argument("input_dir", type=str,
                        help="The folder all the original data")
    parser.add_argument("output_dir", type=str,
                        help="The folder to save the output data")
    parser.add_argument("city_name", type=str,
                        help="The name of the city to process")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()

import numpy as np
from scipy.ndimage import zoom

if __name__ == "__main__":
    ee.Initialize()
    args = get_args()
    # gdf_bounds = gpd.read_file(f"data/_primavera/final/culiacan_centro_bounds.fgb")
    # process_image_collection_to_raster(gdf_bounds, f"{args.output_dir}/buildings.tif", output_dir=args.output_dir + "/chunks", chunk_size=0.002)
    raster = rasterio.open(f"{args.output_dir}/chunk.tif")
    print(raster)
    # smooth_raster = zoom(raster.read(), 1)
    # smooth_raster[smooth_raster < 0.6] = np.nan
    plt.imshow(raster.read(1), cmap='pink')
    plt.show()

    # gdf.to_file(f"{args.output_dir}/buildings2.fgb")
    # bbox = gdf_bounds.total_bounds.tolist()
    # gdf = gpd.read_file(f"{args.output_dir}/buildings2.fgb", bbox=bbox)
    # gdf["geometry"] = gdf["geometry"].simplify(0.00002, preserve_topology=True)
    gdf = raster_to_gdf(raster, threshold=0.6)
    gdf.plot()
    plt.show()

    # gdf_bounds = gpd.read_file(f"{args.input_dir}/{BOUNDS_FILE}", crs="EPSG:4326")
    # gdf_bounds.plot()
    # plt.show()
    # # gdf_bounds = gpd.read_file(f"data/_primavera/final/culiacan_centro_bounds.fgb")
    # gdf_buildings = process_buildings_in_chunks_parallel(gdf_bounds, output_dir=args.output_dir + "/chunks")
    # gdf_buildings.to_file(f"{args.output_dir}/{BUILDING_FILE}")
    # if args.view:
    #     gdf_buildings.plot()
    #     plt.show()
