import geopandas as gpd
import matplotlib.pyplot as plt
import requests
import ee
import argparse
from geopandas import GeoDataFrame

from utils.utils import gdf_to_ee_polygon
from utils.constants import BUILDING_CONFIDENCE


def process_buildings(gdf_bounds: GeoDataFrame) -> GeoDataFrame:
  ee_polygon = gdf_to_ee_polygon(gdf_bounds)
  image_collection = ee.FeatureCollection('GOOGLE/Research/open-buildings/v3/polygons') \
      .filterBounds(ee_polygon) \
      .filter(f'confidence >= {BUILDING_CONFIDENCE}')
  download_url = image_collection.getDownloadURL(filetype='geojson', filename='open_buildings')
  response = requests.get(download_url)
  return gpd.read_file(response.content.decode('utf-8'), driver='GeoJSON', crs='EPSG:4326')


def get_args():
  parser = argparse.ArgumentParser(description='Join establishments with lots')
  parser.add_argument('gpkg_file', type=str, help='The file with all the data')
  parser.add_argument('-v', '--view', action='store_true')
  return parser.parse_args()


if __name__ == '__main__':
  ee.Initialize()
  args = get_args()
  gdf_bounds = gpd.read_file(args.gpkg_file, layer='bounds', crs='EPSG:4326')
  gdf_buildings = process_buildings(gdf_bounds)
  gdf_buildings.to_file(args.gpkg_file, layer='buildings', driver='GPKG')
  if args.view:
    gdf_buildings.plot()
    plt.show()
