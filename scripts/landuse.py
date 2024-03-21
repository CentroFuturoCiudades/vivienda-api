import sys
sys.path.insert(0, '..')
import geopandas as gpd
import pandas as pd
import osmnx as ox
from shapely.geometry import box, Polygon, MultiPolygon
from utils.utils import normalize, remove_outliers, fill, bbox, boundaries, map_sector_to_sector, join
import matplotlib.pyplot as plt
from osmnx.distance import nearest_nodes
import numpy as np

FOLDER = "data/DT"
BUFFER_PARKING = 0.00002

# TODO: Get heights from OSM
# Load the polygon of the boundaries
gdf_bounds = gpd.read_file(f'{FOLDER}/poligono.geojson', crs='EPSG:4326').unary_union

# Load the polygons for the lots
gdf_predios_dt = gpd.read_file(f'{FOLDER}/predios.geojson').to_crs('EPSG:4326')
gdf_predios_dt = gdf_predios_dt.dissolve(by='CLAVE_LOTE').reset_index()
gdf_predios_dt['ALTURA'] = gdf_predios_dt['ALTURA'].apply(lambda x: x.split(
    ' ')[0] if x and str(x).find(' ') > 0 else x).fillna(1).astype(float)
gdf_predios_dt['predio_area'] = gdf_predios_dt['geometry'].to_crs(crs=6933).area
gdf_predios_dt = gdf_predios_dt[gdf_predios_dt['predio_area'] > 0]

# Load the polygons for the blocks
BLOCK_NUMERIC_COLUMNS = [
    'VIVTOT',
    'TVIVHAB',
    'VIVPAR_DES',
    'VPH_AUTOM',
    'POBTOT',
]
gdf_blocks = gpd.read_file(f'{FOLDER}/blocks.geojson').set_index('CVE_GEO')
gdf_blocks['block_area'] = gdf_blocks['geometry'].to_crs(crs=6933).area
gdf_blocks[BLOCK_NUMERIC_COLUMNS] = gdf_blocks[BLOCK_NUMERIC_COLUMNS].applymap(
    lambda x: 0 if x == '*' or x == 'N/D' else x).astype(float)

# Load the coordinates for the establishments
gdf_denue = gpd.read_file(f'{FOLDER}/denue.geojson', crs='EPSG:4326')
gdf_denue = gdf_denue.rename(columns={'id': 'DENUE_ID'})
# Get which sector each establishment belongs to
gdf_denue['sector'] = gdf_denue['codigo_act'].apply(lambda x: str(x)[:2]).astype(int)
gdf_denue['sector'] = gdf_denue['sector'].apply(map_sector_to_sector)
gdf_denue['date'] = pd.to_datetime(gdf_denue['fecha_alta'], format='%Y-%m')
gdf_denue['year'] = gdf_denue['date'].dt.year

# Load the polygons for parking lots
service_highways_filter = '["highway"~"service"]["highway"!~"footway|pedestrian|path|steps|track|sidewalk"]'
G_service_highways = ox.graph_from_polygon(
    gdf_bounds,
    custom_filter=service_highways_filter,
    network_type='all',
    retain_all=True
)
gdf_service_highways = ox.graph_to_gdfs(G_service_highways, nodes=False).reset_index()
gdf_parking_amenities = ox.geometries_from_polygon(
    gdf_bounds, tags={
        'amenity': 'parking'}).reset_index()
gdf_parking_amenities = gdf_parking_amenities[(gdf_parking_amenities['element_type'] != 'node')]
gdf_parking_amenities['geometry'] = gdf_parking_amenities['geometry'].intersection(gdf_bounds)
gdf_combined = gpd.GeoDataFrame(pd.concat([gdf_service_highways, gdf_parking_amenities], ignore_index=True))
unified_geometry = gdf_combined.dissolve().buffer(BUFFER_PARKING).unary_union
external_polygons = [Polygon(poly.exterior) for poly in unified_geometry.geoms]
gdf_parking = gpd.GeoDataFrame(geometry=external_polygons, crs='EPSG:4326')

# Load the polygons for parks
gdf_parks = ox.geometries_from_polygon(
    gdf_bounds,
    tags={
        'leisure': 'park',
        'landuse': 'recreation_ground'}).reset_index()
gdf_parks = gdf_parks[gdf_parks['element_type'] != 'node']
gdf_parks['geometry'] = gdf_parks['geometry'].intersection(gdf_bounds)

# Load the polygons for equipment equipments (schools, universities and places of worship)
gdf_equipment = ox.geometries_from_polygon(
    gdf_bounds,
    tags={
        'amenity': [
            'place_of_worship',
            'school',
            'university'],
        'leisure': [
            'sports_centre',
            'pitch',
            'stadium'],
        'building': ['school'],
        'landuse': ['cemetery']}).reset_index()
gdf_equipment = gdf_equipment[gdf_equipment['element_type'] != 'node']
gdf_equipment['geometry'] = gdf_equipment['geometry'].intersection(gdf_bounds)

# Load the polygons for the building footprints
gdf_buildings = gpd.read_file(f'{FOLDER}/buildings.geojson', crs='EPSG:4326')

# Load the polygons for the builtup areas
gdf_builtup = gpd.read_file(f'{FOLDER}/builtup.geojson', crs='EPSG:4326').reset_index(drop=True)

# Join demographic information with the lots
gdf_block_lots = join(gdf_blocks, gdf_predios_dt, {
    'block_area': 'first',
    'CVEGEO': 'first',
    **{column: 'first' for column in BLOCK_NUMERIC_COLUMNS}
}).rename(columns={'CVEGEO': 'CVE_GEO'})
# TODO: Find an algorithm to accuratetly distribute demographic information for each lot
gdf_block_lots['block_percentage'] = gdf_block_lots.apply(
    lambda row: row['predio_area'] /
    row['block_area'] if row['block_area'] > row['predio_area'] else 1,
    axis=1)

# Join the establishments with the lots by their proximity in each block
# TODO: Ensure establishments are distributed to the correct lots
# TODO: Explore using buffers to join establishments with lots
# TODO: Find a faster and more elegant way to join establishments considering their block
_gdf_denue = gdf_denue.to_crs('EPSG:32614')
_gdf_block_lots = gdf_block_lots.to_crs('EPSG:32614')
gdf_temp = gpd.GeoDataFrame(columns=gdf_denue.columns)
for block_id in gdf_block_lots['CVE_GEO'].unique():
  establishments_in_block = _gdf_denue[_gdf_denue['CVE_GEO'] == block_id]
  lots_in_block = _gdf_block_lots[_gdf_block_lots['CVE_GEO'] == block_id]
  if not establishments_in_block.empty:
    joined_in_block = gpd.sjoin_nearest(establishments_in_block, lots_in_block, how="left", max_distance=0.0001)
    joined_in_block = joined_in_block.drop(columns=['CVE_GEO_left', 'index_right'])
    joined_in_block = joined_in_block.rename(columns={'CVE_GEO_right': 'CVE_GEO'})
    gdf_temp = pd.concat([gdf_temp, joined_in_block])
gdf_temp = gdf_temp.reset_index(drop=True)
establishment_counts = gdf_temp.groupby(['CVE_GEO', 'CLAVE_LOTE']).agg({
    'num_establishments': 'count',
    'num_workers': 'sum',
    'sector': lambda x: dict(x.value_counts())
})
gdf_lots = gdf_block_lots.merge(establishment_counts, on=['CVE_GEO', 'CLAVE_LOTE'], how='left')

# walk radius of 1 mile
WALK_RADIUS = 1609.34
SECTORS = ['comercio', 'servicios', 'salud', 'educacion']


# def get_nearby_services(gdf_lots, gdf_denue, radius=WALK_RADIUS, sectors=SECTORS):
#   gdf_lots = gdf_lots.drop(columns=['sector'])
#   gdf_lots2 = gdf_lots.copy().to_crs('EPSG:3043').drop(columns=['num_establishments']).set_index('CLAVE_LOTE')
#   gdf_lots2['geometry'] = gdf_lots2.buffer(radius)
#   joined_gdf = gpd.sjoin(gdf_lots2, gdf_denue.to_crs('EPSG:3043'), how='inner', predicate='intersects')
#   joined_gdf['services_nearby'] = joined_gdf['num_establishments']
#   joined_gdf = joined_gdf.groupby(joined_gdf.index).agg({
#       'services_nearby': 'sum',
#       'sector': lambda x: dict(x.value_counts())
#   })
#   joined_gdf['sector'] = joined_gdf['sector'].fillna({})
#   for sector in sectors:
#     adj = f'adj_{sector}'
#     joined_gdf[adj] = joined_gdf['sector'].apply(lambda x: x.get(sector, 0) if isinstance(x, dict) else 0)
#     joined_gdf[adj] = normalize(joined_gdf[adj])
#   joined_gdf['total_score'] = joined_gdf[[f'adj_{x}' for x in sectors]].sum(axis=1)
#   joined_gdf = gdf_lots.set_index('CLAVE_LOTE').merge(joined_gdf, how='left', left_index=True, right_index=True)
#   return joined_gdf

def calculate_walking_time(G, row):
  length = sum(ox.utils_graph.get_route_edge_attributes(G, row['path'], 'length'))
  # Convert length to time if needed, assuming average walking speed
  walking_speed_m_per_s = 1.4  # Roughly 5 km/h
  walking_time_s = length / walking_speed_m_per_s
  return walking_time_s / 60  # Convert to minutes


def get_paths(G, start_node, row):
  try:
    shortest_path_nodes = ox.distance.shortest_path(G, start_node, row['nearest_node'], weight='length')
    return shortest_path_nodes
  except BaseException:
    return np.nan  # Return NaN if no path found


from shapely.geometry import Point
from itertools import product
import networkx as nx


def get_paths_and_times(G, sources, targets):
  # Assuming an average walking speed to convert distance to time
  walking_speed_m_per_s = 1.4
  paths_lengths = {(source, target): nx.shortest_path_length(G, source=source, target=target, weight='length')
                   for source, target in product(sources, targets)}

  paths_times = {key: length / walking_speed_m_per_s / 60 for key, length in paths_lengths.items()}
  return paths_times


def calculate_batch_walking_times(G, sources, targets):
  walking_speed_m_per_s = 1.4  # Average walking speed in meters per second
  # Calculate shortest path lengths in meters for all source-target pairs
  lengths = dict(nx.all_pairs_dijkstra_path_length(G, cutoff=900, weight='length'))  # 900 seconds = 15 minutes

  # Filter lengths for our sources and targets, convert to time in minutes
  walking_times = {}
  for source in sources:
    for target in targets:
      if source in lengths and target in lengths[source]:
        # Convert length to walking time (in minutes)
        walking_times[(source, target)] = lengths[source][target] / walking_speed_m_per_s / 60

  return walking_times


def get_nearby_services(gdf_lots, gdf_denue, radius=WALK_RADIUS, sectors=SECTORS, decay_rate=0.01):
  # Ensure both GeoDataFrames use the same CRS for geographic calculations
  gdf_lots = gdf_lots.to_crs("EPSG:4326")
  gdf_denue = gdf_denue.to_crs("EPSG:4326")

  # Generate the pedestrian network graph around the combined area of interest
  unified_area = gdf_lots.unary_union.convex_hull | gdf_denue.unary_union.convex_hull
  G = ox.graph_from_polygon(unified_area, network_type='walk')

  # Find nearest network nodes to lot centroids and amenities
  gdf_lots['nearest_node'] = ox.distance.nearest_nodes(G, gdf_lots.geometry.centroid.x, gdf_lots.geometry.centroid.y)
  gdf_denue['nearest_node'] = ox.distance.nearest_nodes(G, gdf_denue.geometry.x, gdf_denue.geometry.y)

  # Get unique source and target nodes for batch processing
  source_nodes = gdf_lots['nearest_node'].unique()
  target_nodes = gdf_denue['nearest_node'].unique()

  # Calculate batch walking times between sources and targets
  walking_times = calculate_batch_walking_times(G, source_nodes, target_nodes)

  # Convert walking_times to DataFrame for easier processing
  times_df = pd.DataFrame([(s, t, time) for (s, t), time in walking_times.items()],
                          columns=['source', 'target', 'time'])

  # Merge walking times back to amenities based on 'nearest_node'
  gdf_denue_times = pd.merge(gdf_denue, times_df, left_on='nearest_node', right_on='target')

  # Filter amenities within walking radius (15 minutes)
  gdf_denue_within_radius = gdf_denue_times[gdf_denue_times['time'] <= radius / 60]

  # Calculate proximity scores
  gdf_denue_within_radius['proximity_score'] = np.exp(-decay_rate * gdf_denue_within_radius['time'])

  sector_weight_mapping = {
      'comercio': 0.5,
      'servicios': 0.5,
      'salud': 0.9,
      'educacion': 1
  }
  # Apply sector weights
  gdf_denue_within_radius['sector_weight'] = gdf_denue_within_radius['sector'].map(sector_weight_mapping)
  gdf_denue_within_radius['weighted_score'] = gdf_denue_within_radius['proximity_score'] * \
      gdf_denue_within_radius['sector_weight']
  # Merge back to get lot identifiers for each amenity
  gdf_denue_within_radius = gdf_denue_within_radius.drop(columns=['geometry']).merge(
      gdf_lots[['nearest_node', 'geometry']], left_on='source', right_on='nearest_node', how='left')
  # Use spatial join to associate amenities back to lots based on location
  # This step assumes each amenity now has a 'lot_id' to identify which lot it's associated with
  gdf_denue_within_radius = gpd.GeoDataFrame(gdf_denue_within_radius, geometry=gdf_denue_within_radius['geometry'])

  aggregated_scores = gdf_denue_within_radius.groupby(['source']).agg({
      'proximity_score': 'sum',
      'weighted_score': 'sum',
      'sector': lambda x: dict(x.value_counts())
  }).reset_index().set_index('source')

  for sector in sectors:
    aggregated_scores[f"adj_{sector}"] = aggregated_scores['sector'].apply(
        lambda x: x.get(sector, 0) if isinstance(x, dict) else 0)
  aggregated_scores['services_nearby'] = aggregated_scores[[f'adj_{x}' for x in sectors]].sum(axis=1)

  # Join with gdf_lots to add new columns
  gdf_lots = gdf_lots.drop(
      columns=['sector']).merge(
      aggregated_scores,
      left_on='nearest_node',
      right_index=True,
      how='left')
  gdf_lots['total_score'] = gdf_lots['weighted_score']
  print(gdf_lots)
  print(gdf_lots.columns)

  return gdf_lots.set_index('CLAVE_LOTE')


# def get_nearby_services(gdf_lots, gdf_denue, radius=WALK_RADIUS, sectors=SECTORS, decay_rate=0.1):
#   sector_weight_mapping = {
#       'comercio': 0.5,
#       'servicios': 0.5,
#       'salud': 0.9,
#       'educacion': 1
#   }

#   # Convert to appropriate CRS and prepare pedestrian network
#   gdf_lots = gdf_lots.to_crs('EPSG:4326')
#   gdf_denue = gdf_denue.to_crs('EPSG:4326')
#   G = ox.graph_from_polygon(gdf_lots.unary_union, network_type='walk')

#   # Initialize walkability score and sector counts columns
#   gdf_lots['walkability_score'] = 0.0
#   for sector in sectors:
#     gdf_lots[f"adj_{sector}"] = 0

#   # Iterate over lots and calculate scores
#   for index, lot in gdf_lots.iterrows():
#     score, sector_counts = calculate_walkability_for_lot(lot, gdf_denue, G, decay_rate, sector_weight_mapping)
#     gdf_lots.at[index, 'walkability_score'] = score
#     for sector, count in sector_counts.items():
#       gdf_lots.at[index, f"adj_{sector}"] += count

#   return gdf_lots


for sector in SECTORS:
  gdf_lots[sector] = gdf_lots['sector'].apply(lambda x: x.get(sector, 0) if isinstance(x, dict) else 0)
# gdf_lots = get_nearby_services(gdf_lots, gdf_denue)
gdf_lots = get_nearby_services(gdf_lots, gdf_denue)
print(gdf_lots)

fig, ax = plt.subplots(ncols=5)
gdf_lots.plot(ax=ax[0], column='total_score')
gdf_lots.plot(ax=ax[1], column='adj_comercio')
gdf_lots.plot(ax=ax[2], column='comercio')
gdf_lots.plot(ax=ax[3], column='adj_servicios')
gdf_lots.plot(ax=ax[4], column='servicios')
plt.show()

# Superimpose all uses of land to correctly identify the green cover
gdf_combined = gpd.GeoDataFrame(
    pd.concat([gdf_equipment, gdf_builtup, gdf_parks, gdf_parking, gdf_buildings], ignore_index=True))
gdf_combined = gpd.GeoDataFrame(geometry=[gdf_combined.unary_union], crs='EPSG:4326')
gdf_combined = gdf_combined.explode().reset_index(drop=True)
gdf_boundaries = gpd.GeoDataFrame(geometry=[gdf_bounds], crs='EPSG:4326')
greencover_geometry = gpd.overlay(gdf_boundaries, gdf_combined, how='difference', keep_geom_type=False)
gdf_greencover = gpd.GeoDataFrame(geometry=greencover_geometry.geometry, crs='EPSG:4326')
gdf_greencover = gpd.GeoDataFrame(geometry=[gdf_greencover.unary_union]).explode().reset_index(drop=True)
gdf_greencover = gpd.GeoDataFrame(geometry=gdf_greencover.geometry, crs='EPSG:4326')

# Separate buildings, parking, green cover and unused to intersect with lots
gdf_buildings2 = gdf_lots.reset_index().overlay(gdf_buildings, how='intersection', keep_geom_type=False)
gdf_buildings2 = gdf_buildings2.set_index('CLAVE_LOTE')
gdf_buildings2 = gdf_buildings2.dissolve(by='CLAVE_LOTE')
gdf_buildings2['building_area'] = gdf_buildings2['geometry'].to_crs(crs=6933).area

gdf_parking2 = gdf_lots.reset_index().overlay(gdf_parking, how='intersection', keep_geom_type=False)
gdf_parking2 = gdf_parking2.overlay(gdf_buildings, how='difference', keep_geom_type=False)
gdf_parking2 = gdf_parking2.set_index('CLAVE_LOTE')
gdf_parking2 = gdf_parking2.dissolve(by='CLAVE_LOTE')
gdf_parking2['parking_area'] = gdf_parking2['geometry'].to_crs(crs=6933).area

gdf_parks2 = gdf_lots.reset_index().overlay(gdf_parks, how='intersection', keep_geom_type=False)
gdf_parks2 = gdf_parks2.overlay(gdf_buildings, how='difference', keep_geom_type=False)
gdf_parks2 = gdf_parks2.overlay(gdf_parking, how='difference', keep_geom_type=False)
gdf_parks2 = gdf_parks2.set_index('CLAVE_LOTE')
gdf_parks2 = gdf_parks2.dissolve(by='CLAVE_LOTE')
gdf_parks2['park_area'] = gdf_parks2['geometry'].to_crs(crs=6933).area

gdf_equipment2 = gdf_lots.reset_index().overlay(gdf_equipment, how='intersection', keep_geom_type=False)
gdf_equipment2 = gdf_equipment2.overlay(gdf_buildings, how='difference', keep_geom_type=False)
gdf_equipment2 = gdf_equipment2.overlay(gdf_parking, how='difference', keep_geom_type=False)
gdf_equipment2 = gdf_equipment2.overlay(gdf_parks, how='difference', keep_geom_type=False)
gdf_equipment2 = gdf_equipment2.set_index('CLAVE_LOTE')
gdf_equipment2 = gdf_equipment2.dissolve(by='CLAVE_LOTE')
gdf_equipment2['equipment_area'] = gdf_equipment2['geometry'].to_crs(crs=6933).area

gdf_greencover_filtered = gdf_greencover[gdf_greencover.geometry.type.isin(['Polygon', 'MultiPolygon'])]
gdf_greencover2 = gdf_lots.reset_index().overlay(gdf_greencover_filtered, how='intersection')
gdf_greencover2 = gdf_greencover2.overlay(gdf_buildings, how='difference')
gdf_greencover2 = gdf_greencover2.overlay(gdf_parking, how='difference')
gdf_greencover2 = gdf_greencover2.overlay(gdf_parks, how='difference')
gdf_greencover2 = gdf_greencover2.overlay(gdf_equipment, how='difference')
gdf_greencover2 = gdf_greencover2.set_index('CLAVE_LOTE')
gdf_greencover2 = gdf_greencover2.dissolve(by='CLAVE_LOTE')
gdf_greencover2['green_area'] = gdf_greencover2['geometry'].to_crs(crs=6933).area

gdf_unused = gdf_lots.reset_index().overlay(gdf_greencover_filtered, how='difference', keep_geom_type=False)
gdf_unused = gdf_unused.overlay(gdf_buildings, how='difference', keep_geom_type=False)
gdf_unused = gdf_unused.overlay(gdf_parking, how='difference', keep_geom_type=False)
gdf_unused = gdf_unused.overlay(gdf_parks, how='difference', keep_geom_type=False)
gdf_unused = gdf_unused.overlay(gdf_equipment, how='difference', keep_geom_type=False)
gdf_unused = gdf_unused.set_index('CLAVE_LOTE')
gdf_unused = gdf_unused.dissolve(by='CLAVE_LOTE')
gdf_unused['unused_area'] = gdf_unused['geometry'].to_crs(crs=6933).area

gdf_blocks2 = gdf_lots.merge(gdf_buildings2[['building_area']], how='left', left_index=True, right_index=True)
gdf_blocks2 = gdf_blocks2.merge(gdf_greencover2[['green_area']], how='left', left_index=True, right_index=True)
gdf_blocks2 = gdf_blocks2.merge(gdf_parking2[['parking_area']], how='left', left_index=True, right_index=True)
gdf_blocks2 = gdf_blocks2.merge(gdf_parks2[['park_area']], how='left', left_index=True, right_index=True)
gdf_blocks2 = gdf_blocks2.merge(gdf_equipment2[['equipment_area']], how='left', left_index=True, right_index=True)
gdf_blocks2 = gdf_blocks2.merge(gdf_unused[['unused_area']], how='left', left_index=True, right_index=True)

print(gdf_lots)
print(gdf_buildings2['building_area'])
print(gdf_blocks2['building_area'])
gdf_blocks2['lot_area'] = gdf_blocks2['geometry'].to_crs(crs=6933).area
gdf_blocks2['building_ratio'] = gdf_blocks2['building_area'] / gdf_blocks2['lot_area']
gdf_blocks2['parking_ratio'] = gdf_blocks2['parking_area'] / gdf_blocks2['lot_area']
gdf_blocks2['park_ratio'] = gdf_blocks2['park_area'] / gdf_blocks2['lot_area']
gdf_blocks2['green_ratio'] = gdf_blocks2['green_area'] / gdf_blocks2['lot_area']
gdf_blocks2['unused_ratio'] = gdf_blocks2['unused_area'] / gdf_blocks2['lot_area']
gdf_blocks2['equipment_ratio'] = gdf_blocks2['equipment_area'] / gdf_blocks2['lot_area']

gdf_final = gdf_blocks2
columns = [
    'POBTOT',
    'VIVTOT',
    'num_workers',
    'num_establishments',
    'TVIVHAB',
    'VPH_AUTOM',
    'VIVPAR_DES',
    'lot_area',
    'building_area',
    'parking_area',
    'park_area',
    'green_area',
    'unused_area',
    'building_ratio',
    'parking_ratio',
    'park_ratio',
    'green_ratio',
    'unused_ratio',
    'equipment_ratio',
    'services_nearby',
    'comercio',
    'servicios',
    'salud',
    'educacion',
    'adj_comercio',
    'adj_servicios',
    'adj_salud',
    'adj_educacion',
    'total_score',]
gdf_final['num_properties'] = gdf_final['TVIVHAB'] + gdf_final['num_establishments']
gdf_final[columns] = gdf_final[columns].apply(pd.to_numeric, errors='coerce').fillna(0)
gdf_final['vacant_area'] = gdf_final.apply(
    lambda x: (
        x['VIVPAR_DES'] *
        x['building_area'] /
        x['num_properties']) if x['num_properties'] > 0 else 0,
    axis=1)
gdf_final['vacant_area'] = remove_outliers(gdf_final['vacant_area'], 0, 0.9)
gdf_final['vacant_ratio'] = gdf_final['vacant_area'] / gdf_final['lot_area']
gdf_final['vacant_ratio'] = remove_outliers(gdf_final['vacant_ratio'], 0, 0.9)
gdf_final['car_area'] = gdf_final['VPH_AUTOM'] * (gdf_final['building_area'] / gdf_final['num_properties']) * 0.05
gdf_final['wasteful_area'] = gdf_final['unused_area'] + \
    gdf_final['vacant_area'] + gdf_final['parking_area'] + gdf_final['green_area']
gdf_final['wasteful_area'] = remove_outliers(gdf_final['wasteful_area'], 0, 0.9)
gdf_final['wasteful_ratio'] = gdf_final['unused_ratio'] + \
    gdf_final['vacant_ratio'] + gdf_final['parking_ratio'] + gdf_final['green_ratio']
gdf_final['wasteful_ratio'] = remove_outliers(gdf_final['wasteful_ratio'], 0, 0.9)
gdf_final['used_area'] = gdf_final['lot_area'] - gdf_final['wasteful_area']
gdf_final['used_ratio'] = gdf_final['used_area'] / gdf_final['lot_area']

gdf_final['occupancy'] = (gdf_final['POBTOT'] + gdf_final['num_workers'])
gdf_final['utilization_area'] = gdf_final['wasteful_area'] / gdf_final['occupancy']
gdf_final['utilization_area'] = remove_outliers(gdf_final['utilization_area'], 0, 0.9)

gdf_final['occupancy_density'] = gdf_final.apply(
    lambda x: x['occupancy'] /
    x['building_area'] if x['building_area'] > 0 else 0,
    axis=1)
gdf_final['occupancy_density'] = remove_outliers(gdf_final['occupancy_density'], 0, 0.9)
gdf_final['home_density'] = gdf_final.apply(lambda x: x['VIVTOT'] /
                                            x['building_area'] if x['building_area'] > 0 else 0, axis=1)
gdf_final['lot_ratio'] = normalize(gdf_final['lot_area'])
gdf_final['utilization_ratio'] = gdf_final.apply(
    lambda x: (
        x['wasteful_ratio'] /
        x['occupancy']) if x['occupancy'] > 0 else (x['wasteful_ratio'] if x['wasteful_ratio'] > 0.5 else 0),
    axis=1)
gdf_final['occupancy_density'] = normalize(gdf_final['occupancy_density'])
gdf_final['utilization_ratio'] = remove_outliers(gdf_final['utilization_ratio'], 0, 0.9)
gdf_final['utilization_ratio'] = normalize(gdf_final['utilization_ratio'])

gdf_final['total_score'] = normalize(gdf_final['total_score'])
gdf_final['combined_score'] = gdf_final['total_score'] + gdf_final['utilization_ratio']
gdf_final['combined_score'] = remove_outliers(gdf_final['combined_score'], 0, 0.9)
gdf_final['combined_score'] = normalize(gdf_final['combined_score'])

fig = plt.figure(frameon=False, figsize=(20, 20))
ax = plt.Axes(fig, [0., 0., 1., 1.])
ax.xaxis.set_major_locator(plt.NullLocator())
ax.yaxis.set_major_locator(plt.NullLocator())
ax.margins(0, 0)
ax.set_axis_off()
fig.add_axes(ax)
# gdf_final.plot(ax=ax, column='wasteful_ratio', cmap='cool', linewidth=1, edgecolor='darkgray')
gdf_unused.plot(ax=ax, color='blue', alpha=1)
gdf_equipment2.plot(ax=ax, color='red', alpha=1)
gdf_buildings2.plot(ax=ax, color='yellow', alpha=1)
gdf_parking2.plot(ax=ax, color='lightgray', alpha=1)
gdf_greencover2.plot(ax=ax, color='lightgreen')
gdf_parks.plot(ax=ax, color='green')
plt.show()

fig, ax = plt.subplots(figsize=(20, 20), ncols=4)
ax[0].set_axis_off()
ax[0].set_title('Superficie desaprovechada')
gdf_final.plot(ax=ax[0], column='wasteful_ratio')
ax[1].set_axis_off()
ax[1].set_title('Utilización de la superficie')
gdf_final.plot(ax=ax[1], column='utilization_ratio')
ax[2].set_axis_off()
ax[2].set_title('Puntaje de accesibilidad')
gdf_final.plot(ax=ax[2], column='total_score')
ax[3].set_axis_off()
ax[3].set_title('Puntaje combinado')
gdf_final.plot(ax=ax[3], column='combined_score')
plt.show()

# convert to geojson files
FOLDER_FINAL = "data/processed"
COLUMNS = [
    'CLAVE_LOTE',
    'VIVTOT',
    'TVIVHAB',
    'VIVPAR_DES',
    'VPH_AUTOM',
    'POBTOT',
    'building_area',
    'green_area',
    'parking_area',
    'park_area',
    'equipment_area',
    'unused_area',
    'num_workers',
    'num_establishments',
    'services_nearby',
    'comercio',
    'servicios',
    'salud',
    'educacion',
    'adj_comercio',
    'adj_servicios',
    'adj_salud',
    'adj_educacion',
    'total_score',
    'lot_area',
    'building_ratio',
    'parking_ratio',
    'park_ratio',
    'green_ratio',
    'unused_ratio',
    'equipment_ratio',
    'num_properties',
    'vacant_area',
    'vacant_ratio',
    'wasteful_area',
    'wasteful_ratio',
    'used_area',
    'used_ratio',
    'occupancy',
    'utilization_area',
    'occupancy_density',
    'home_density',
    'lot_ratio',
    'utilization_ratio',
    'combined_score',
]
gdf_final[['geometry']].to_file(f"{FOLDER_FINAL}/predios.geojson", driver='GeoJSON')
gdf_final.reset_index()[COLUMNS].to_csv(f"{FOLDER_FINAL}/predios.csv", index=False)
gdf_buildings2[['ALTURA', 'geometry']].to_file(f"{FOLDER_FINAL}/buildings.geojson", driver='GeoJSON')
gdf_parking2[['parking_area', 'geometry']].to_file(f"{FOLDER_FINAL}/parking.geojson", driver='GeoJSON')
gdf_denue[['sector', 'num_workers', 'geometry', 'nom_estab', 'fecha_alta', 'codigo_act']].to_file(
    f"{FOLDER_FINAL}/denue.geojson", driver='GeoJSON')
gdf_greencover2[['green_area', 'geometry']].to_file(f"{FOLDER_FINAL}/greencover.geojson", driver='GeoJSON')
gdf_parks2[['park_area', 'geometry']].to_file(f"{FOLDER_FINAL}/parks.geojson", driver='GeoJSON')
gdf_equipment2[['equipment_area', 'geometry']].to_file(f"{FOLDER_FINAL}/equipment.geojson", driver='GeoJSON')
