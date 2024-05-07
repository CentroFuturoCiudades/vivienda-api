from geopandas import GeoDataFrame
from scripts.raster_to_geojson import to_gdf
import rioxarray
import requests
import ee
import argparse
from utils.utils import gdf_to_ee_polygon
import tempfile
import geopandas as gpd
import matplotlib.pyplot as plt


def process_green_area(gdf_bounds: GeoDataFrame) -> GeoDataFrame:
  ee.Initialize()
  # ee.Authenticate()
  # Get the medium and high vegetation
  ee_polygon = gdf_to_ee_polygon(gdf_bounds)
  image = ee.Image("JRC/GHSL/P2023A/GHS_BUILT_C/2018")
  image = image.clip(ee_polygon)
  image = image.eq(2).Or(image.eq(3))
  download_url = image.getDownloadURL({
      'scale': 10,
      'region': image.geometry().getInfo(),
      'format': 'GeoTIFF',
      'crs': 'EPSG:4326'
  })
  response = requests.get(download_url)
  with tempfile.TemporaryFile() as fp:
    fp.write(response.content)
    fp.seek(0)
    raster = rioxarray.open_rasterio(fp)
    gdf_builtup = to_gdf(raster).dissolve()
    return gdf_builtup


def get_args():
  parser = argparse.ArgumentParser(description='Join establishments with lots')
  parser.add_argument('gpkg_file', type=str, help='The file with all the data')
  parser.add_argument('-v', '--view', action='store_true')
  return parser.parse_args()


if __name__ == '__main__':
  args = get_args()
  gdf_bounds = gpd.read_file(args.gpkg_file, layer='bounds', crs='EPSG:4326')
  gdf_builtup = process_green_area(gdf_bounds)
  gdf_builtup.to_file(args.gpkg_file, layer='vegetation', driver='GPKG')
  if args.view:
    gdf_builtup.plot()
    plt.show()
