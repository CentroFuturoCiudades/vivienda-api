import argparse
import tempfile

import ee
import geopandas as gpd
import matplotlib.pyplot as plt
import requests
import rioxarray
from geopandas import GeoDataFrame

from src.scripts.utils.utils import gdf_to_ee_polygon, to_gdf
from src.scripts.utils.constants import BOUNDS_FILE, VEGETATION_FILE


def process_green_area(gdf_bounds: GeoDataFrame) -> GeoDataFrame:
    ee.Initialize()
    ee_polygon = gdf_to_ee_polygon(gdf_bounds)
    image = ee.Image("JRC/GHSL/P2023A/GHS_BUILT_C/2018")
    image = image.clip(ee_polygon)
    image = image.eq(2).Or(image.eq(3))
    download_url = image.getDownloadURL(
        {
            "scale": 10,
            "region": image.geometry().getInfo(),
            "format": "GeoTIFF",
            "crs": "EPSG:4326",
        }
    )
    response = requests.get(download_url)
    with tempfile.TemporaryFile() as fp:
        fp.write(response.content)
        fp.seek(0)
        raster = rioxarray.open_rasterio(fp)
        gdf_builtup = to_gdf(raster).dissolve()
        return gdf_builtup


def get_args():
    parser = argparse.ArgumentParser(
        description="Join establishments with lots")
    parser.add_argument("input_dir", type=str,
                        help="The folder all the original data")
    parser.add_argument("output_dir", type=str,
                        help="The folder to save the output data")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()
    gdf_bounds = gpd.read_file(
        f"{args.input_dir}/{BOUNDS_FILE}", crs="EPSG:4326")
    gdf_builtup = process_green_area(gdf_bounds)
    gdf_builtup.to_file(f"{args.output_dir}/{VEGETATION_FILE}")
    if args.view:
        gdf_builtup.plot()
        plt.show()
