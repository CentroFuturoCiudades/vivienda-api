import geopandas as gpd
import pandas as pd
import matplotlib.pyplot as plt
from utils.utils import remove_outliers, normalize, COLUMN_ID
import sys
import argparse

def get_args():
  parser = argparse.ArgumentParser(description='Join establishments with lots')
  parser.add_argument('output_folder', type=str, help='The folder to save the output data')
  parser.add_argument('column_id', type=str, help='The column to use as the identifier for the lots')
  return parser.parse_args()

if __name__ == '__main__':
    args = get_args()
    OUTPUT_FOLDER = args.output_folder
    COLUMN_ID = args.column_id

    gdf_lots = gpd.read_file(f'{OUTPUT_FOLDER}/predios.geojson', crs='EPSG:4326').set_index(COLUMN_ID)
    df_lots = pd.read_csv(f'{OUTPUT_FOLDER}/predios.csv').set_index(COLUMN_ID)
    df_lots.index = df_lots.index.map(str)
    gdf_lots['lot_area'] = gdf_lots.to_crs('EPSG:6933').area
    print(gdf_lots.columns.to_list())
    print(df_lots.columns.to_list())
    # combine gdf_lots and df_lots by CLAVE_LOTE
    df_final = gdf_lots.merge(df_lots, left_index=True, right_index=True, how='left')
    landuse_names = [
        'unused',
        'green',
        'parking',
        'park',
        'equipment',
        'building',
    ]
    gdfs_landuse = [
        gpd.read_file(f'{OUTPUT_FOLDER}/{name}.geojson', crs='EPSG:4326').set_index(COLUMN_ID).drop(columns='geometry')
        for name in landuse_names
    ]
    print(df_final.columns.to_list())
    for gdf in gdfs_landuse:
        df_final = df_final.merge(gdf, left_index=True, right_index=True, how='left')
    df_final = df_final.fillna(0)
    gdf_final = gpd.GeoDataFrame(df_final, geometry='geometry', crs='EPSG:4326')
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
        'accessibility_score',]
    gdf_final['TVIVHAB'] = gdf_final['TVIVHAB'].apply(lambda x: 0 if x == '*' else x).astype(int)
    gdf_final['num_properties'] = gdf_final['TVIVHAB'] + gdf_final['num_establishments']
    gdf_final['wasteful_area'] = gdf_final['unused_area'] + gdf_final['parking_area'] + gdf_final['green_area']
    gdf_final['wasteful_ratio'] = gdf_final['unused_ratio'] + gdf_final['parking_ratio'] + gdf_final['green_ratio']

    gdf_final['used_area'] = gdf_final['lot_area'] - gdf_final['wasteful_area']
    gdf_final['used_ratio'] = gdf_final['used_area'] / gdf_final['lot_area']

    gdf_final['occupancy'] = (gdf_final['POBTOT'] + gdf_final['num_workers'])
    gdf_final['underutilized_area'] = gdf_final['wasteful_area'] / gdf_final['occupancy']
    gdf_final['underutilized_area'] = remove_outliers(gdf_final['underutilized_area'], 0, 0.9)
    gdf_final['underutilized_ratio'] = gdf_final.apply(
        lambda x: x['wasteful_ratio'] / (x['occupancy'] + 1) if x['wasteful_ratio'] > 0.25 else 0,
        axis=1)
    gdf_final['underutilized_ratio'] = remove_outliers(gdf_final['underutilized_ratio'], 0, 0.9)
    gdf_final['underutilized_ratio'] = normalize(gdf_final['underutilized_ratio'])

    gdf_final['occupancy_density'] = gdf_final.apply(
        lambda x: x['occupancy'] /
        x['building_area'] if x['building_area'] > 0 else 0,
        axis=1)
    gdf_final['occupancy_density'] = remove_outliers(gdf_final['occupancy_density'], 0, 0.9)

    gdf_final['home_density'] = gdf_final.apply(lambda x: (x['VIVTOT'] + x['num_workers']) /
                                                x['building_area'] if x['building_area'] > 0 else 0, axis=1)
    gdf_final['home_density'] = remove_outliers(gdf_final['home_density'], 0, 0.9)

    gdf_final['combined_score'] = gdf_final['accessibility_score'] + gdf_final['underutilized_ratio']
    gdf_final['combined_score'] = remove_outliers(gdf_final['combined_score'], 0, 0.9)
    gdf_final['combined_score'] = normalize(gdf_final['combined_score'])

    fig, ax = plt.subplots(ncols=3, figsize=(20, 10))
    gdf_final.plot(ax=ax[0], column='wasteful_ratio')
    gdf_final.plot(ax=ax[1], column='underutilized_ratio')
    gdf_final.plot(ax=ax[2], column='combined_score')
    plt.show()

    gdf_final.reset_index()[[COLUMN_ID, 'geometry']].to_file(f'{OUTPUT_FOLDER}/predios.geojson', driver='GeoJSON')
    gdf_final.reset_index().drop(columns=['geometry']).to_csv(f"{OUTPUT_FOLDER}/predios.csv", index=False)

