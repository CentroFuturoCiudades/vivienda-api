import os
import geopandas as gpd
import ee
from zipfile import ZipFile
from rasterio.merge import merge
import rasterio

ee.Initialize(project="primavera-438222")


def generate_chunks(bounds, resolution=4, max_pixels_per_side=10000):
    """
    Generate chunk geometries based on the bounding box and desired resolution.

    Parameters:
    - bounds: A list [minx, miny, maxx, maxy]
    - resolution: The pixel size in meters (e.g., 4 m)
    - max_pixels_per_side: Maximum number of pixels along one side of the chunk.
      The chunk size in meters will be max_pixels_per_side * resolution.

    Returns:
    - A list of (chunk_id, geometry) tuples.
    """
    minx, miny, maxx, maxy = bounds

    # Determine chunk size in meters
    chunk_size_m = max_pixels_per_side * resolution

    # Determine how many chunks we need in x and y directions
    width_m = maxx - minx
    height_m = maxy - miny

    x_steps = int((width_m // chunk_size_m) +
                  (1 if width_m % chunk_size_m != 0 else 0))
    y_steps = int((height_m // chunk_size_m) +
                  (1 if height_m % chunk_size_m != 0 else 0))

    chunks = []
    for i in range(x_steps):
        for j in range(y_steps):
            chunk_id = f"{i}_{j}"
            chunk_xmin = minx + i * chunk_size_m
            chunk_xmax = min(chunk_xmin + chunk_size_m, maxx)
            chunk_ymin = miny + j * chunk_size_m
            chunk_ymax = min(chunk_ymin + chunk_size_m, maxy)

            chunk_geom = [[
                [chunk_xmin, chunk_ymin],
                [chunk_xmin, chunk_ymax],
                [chunk_xmax, chunk_ymax],
                [chunk_xmax, chunk_ymin],
                [chunk_xmin, chunk_ymin]
            ]]
            chunks.append((chunk_id, chunk_geom))
    return chunks


def export_chunk_to_gcs(image, chunk_id, region, bucket_name, scale=4):
    """
    Exports a single chunk of the image to GCS as a GeoTIFF.
    """
    task = ee.batch.Export.image.toCloudStorage(
        image=image,
        description=f"chunk_{chunk_id}",
        bucket=bucket_name,
        fileNamePrefix=f"chunk_{chunk_id}",
        region=region,
        scale=scale,
        fileFormat="GeoTIFF"
    )
    task.start()
    print(f"Started export task for chunk {chunk_id}")
    return task


def wait_for_tasks(tasks, check_interval=30):
    """
    Wait until all tasks are complete or failed.
    This periodically checks task statuses until none are running.
    """
    import time
    while True:
        statuses = [t.status()['state'] for t in tasks]
        running = [s for s in statuses if s in ['READY', 'RUNNING']]
        if not running:
            print("All tasks have completed.")
            break
        else:
            print(f"Waiting... {len(running)} tasks still running.")
        time.sleep(check_interval)


def export_area_in_chunks(gdf_bounds, bucket_name, resolution, year, max_pixels_per_side=10000):
    # Initialize Earth Engine if not done globally
    # ee.Initialize()  # Uncomment if needed

    # Get total bounds from the GeoDataFrame
    bounds = gdf_bounds.total_bounds  # [minx, miny, maxx, maxy]
    minx, miny, maxx, maxy = bounds

    # Convert bounds to an Earth Engine geometry
    geom = ee.Geometry.Rectangle([minx, miny, maxx, maxy])

    # Define the start and end date for the given year
    start_date = f"{year}-01-01"
    end_date = f"{year+1}-01-01"

    # Load the image collection
    image_collection = (ee.ImageCollection("GOOGLE/Research/open-buildings-temporal/v1")
                        .filterBounds(geom)
                        .filterDate(start_date, end_date))

    # Mosaic all images for that year
    mosaic_image = image_collection.mean()

    # Generate chunks based on resolution and max_pixels_per_side
    chunks = generate_chunks(bounds, resolution, max_pixels_per_side)
    print(f"Total chunks to export: {len(chunks)}")

    # Export each chunk
    tasks = []
    for chunk_id, chunk_geom in chunks:
        task = export_chunk_to_gcs(
            mosaic_image, chunk_id, chunk_geom, bucket_name, scale=resolution)
        tasks.append(task)

    return tasks


if __name__ == "__main__":
    gdf_bounds = gpd.read_file(
        "data/_primavera/final/culiacan_centro_bounds.fgb")

    export_area_in_chunks(gdf_bounds, "reimagina-buildings", 0.1, 2023)
