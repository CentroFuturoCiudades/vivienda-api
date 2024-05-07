import geopandas as gpd
import numpy as np
from utils.utils import remove_outliers, normalize

if __name__ == '__main__':
  gdf_lots = gpd.read_file('data/la_primavera/lots.gpkg', layer='accessibility', crs='EPSG:4326')

  gdf_lots['num_properties'] = gdf_lots['TVIVHAB'] + gdf_lots['num_establishments']
  gdf_lots['wasteful_area'] = gdf_lots['unused_area'] + gdf_lots['parking_area'] + gdf_lots['green_area']
  gdf_lots['wasteful_ratio'] = gdf_lots['unused_ratio'] + gdf_lots['parking_ratio'] + gdf_lots['green_ratio']
  gdf_lots['occupancy'] = (gdf_lots['POBTOT'] + gdf_lots['num_workers'])
  gdf_lots['underutilized_area'] = gdf_lots['wasteful_area'] / gdf_lots['occupancy']
  gdf_lots['underutilized_area'] = remove_outliers(gdf_lots['underutilized_area'], 0, 0.9)
  gdf_lots['underutilized_ratio'] = gdf_lots.apply(
      lambda x: x['wasteful_ratio'] / (x['occupancy'] + 1) if x['wasteful_ratio'] > 0.25 else 0,
      axis=1)
  gdf_lots['underutilized_ratio'] = remove_outliers(gdf_lots['underutilized_ratio'], 0, 0.9)
  gdf_lots['underutilized_ratio'] = normalize(gdf_lots['underutilized_ratio'])

  gdf_lots['occupancy_density'] = gdf_lots.apply(
      lambda x: x['occupancy'] /
      x['building_area'] if x['building_area'] > 0 else 0,
      axis=1)
  gdf_lots['occupancy_density'] = remove_outliers(gdf_lots['occupancy_density'], 0, 0.9)

  gdf_lots['home_density'] = gdf_lots.apply(lambda x: (x['VIVTOT'] + x['num_workers']) /
                                            x['building_area'] if x['building_area'] > 0 else 0, axis=1)
  gdf_lots['home_density'] = remove_outliers(gdf_lots['home_density'], 0, 0.9)

  gdf_lots['combined_score'] = ((1 - normalize(gdf_lots['minutes'])) + normalize(gdf_lots['underutilized_area']))
  gdf_lots['combined_score'] = normalize(gdf_lots['combined_score'])

  gdf_lots['latitud'] = gdf_lots['geometry'].centroid.y
  gdf_lots['longitud'] = gdf_lots['geometry'].centroid.x

  gdf_lots.to_file('data/la_primavera/lots.gpkg', layer='final', driver='GPKG')

  gdf_lots = gpd.read_file('data/la_primavera/lots.gpkg', layer='final', crs='EPSG:4326')
  gdf_lots.to_file('data/la_primavera/lots.geojson', driver='GeoJSON')

  gdf_bounds = gpd.read_file('data/la_primavera/original.gpkg', layer='bounds', crs='EPSG:4326')
  gdf_bounds.to_file('data/la_primavera/bounds.geojson', driver='GeoJSON')

  gdf_building = gpd.read_file('data/la_primavera/lots.gpkg', layer='building', crs='EPSG:4326')
  print(gdf_building.columns.to_list())
  gdf_building[['ID', 'geometry', 'building_area', 'building_ratio']].to_file(
      'data/la_primavera/building.geojson', driver='GeoJSON')

  gdf_parking = gpd.read_file('data/la_primavera/lots.gpkg', layer='parking', crs='EPSG:4326')
  print(gdf_parking.columns.to_list())
  gdf_parking[['ID', 'geometry', 'parking_area', 'parking_ratio']].to_file(
      'data/la_primavera/parking.geojson', driver='GeoJSON')

  gdf_park = gpd.read_file('data/la_primavera/lots.gpkg', layer='park', crs='EPSG:4326')
  print(gdf_park.columns.to_list())
  gdf_park[['ID', 'geometry', 'park_area', 'park_ratio']].to_file('data/la_primavera/park.geojson', driver='GeoJSON')

  gdf_green = gpd.read_file('data/la_primavera/lots.gpkg', layer='green', crs='EPSG:4326')
  gdf_green[['ID', 'geometry', 'green_area', 'green_ratio']].to_file(
      'data/la_primavera/green.geojson', driver='GeoJSON')

  gdf_equipment = gpd.read_file('data/la_primavera/lots.gpkg', layer='equipment', crs='EPSG:4326')
  gdf_equipment[['ID', 'geometry', 'equipment_area', 'equipment_ratio']
                ].to_file('data/la_primavera/equipment.geojson', driver='GeoJSON')

  gdf_establishments = gpd.read_file('data/la_primavera/processed.gpkg', layer='establishments', crs='EPSG:4326')
  gdf_establishments.to_file('data/la_primavera/establishments.geojson', driver='GeoJSON')
