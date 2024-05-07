import argparse
import geopandas as gpd
import fiona


def get_args():
  parser = argparse.ArgumentParser(description='Join establishments with lots')
  parser.add_argument('folder', type=str, help='The file with all the data')
  return parser.parse_args()


if __name__ == '__main__':
  args = get_args()
  # layers = fiona.listlayers(f"{args.folder}/final.gpkg")

  # for layer in layers:
  #   gdf = gpd.read_file(f"{args.folder}/final.gpkg", layer=layer, crs='EPSG:4326')
  #   gdf.to_file(f"{args.folder}/{layer}.geojson", driver='GeoJSON')

  layers = fiona.listlayers(f"{args.folder}/lots.gpkg")
  layers = [
      'lots',
      'green',
      'parking',
      'park',
      'equipment',
      'building']
  for layer in layers:
    gdf = gpd.read_file(f"{args.folder}/lots.gpkg", layer=layer, crs='EPSG:4326')
    gdf.to_file(f"{args.folder}/{layer}.geojson", driver='GeoJSON')

  gdf = gpd.read_file(f"{args.folder}/final.gpkg", layer='bounds', crs='EPSG:4326')
  gdf.to_file(f"{args.folder}/bounds.geojson", driver='GeoJSON')

  gdf_establishments = gpd.read_file(f"{args.folder}/final.gpkg", layer='establishments', crs='EPSG:4326')
  gdf_establishments.to_file(f"{args.folder}/establishments.geojson", driver='GeoJSON')
