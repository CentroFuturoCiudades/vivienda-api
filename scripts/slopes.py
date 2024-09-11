import argparse

import rasterio
import geopandas as gpd
import numpy as np
import json
import pyogrio
import sqlite3

from rasterio.mask import mask
from shapely.geometry import box

# Calcular pendiente
def calculate_slope(elevation_array, transform):
    dx, dy = np.gradient(elevation_array)  # Calcular gradiente en x e y
    slope = np.arctan(np.sqrt(dx**2 + dy**2)) * 180 / np.pi  # Convertir a grados
    return slope

def get_args():
    parser = argparse.ArgumentParser(
        description="Join establishments with lots")
    parser.add_argument("bounds_file", type=str,
                        help="Map limits file")
    parser.add_argument(
        "altitude_tif_file", type=str, 
        help="The file with altitude data")
    parser.add_argument(
        "lots_file", type=str, 
        help="Lots file")
    parser.add_argument(
        "db_file", type=str, 
        help="Database file")
    parser.add_argument("-v", "--view", action="store_true")
    return parser.parse_args()


if __name__ == "__main__":
    args = get_args()

    ##CONVERTIR TIFF DE ALTITUD A GEODATAFRAME
    tiff_path = args.altitude_tif_file
    geojson_path = args.bounds_file

    bounds_gdf = gpd.read_file(geojson_path)

    # Match CRS
    with rasterio.open(tiff_path) as src:
        bounds_gdf = bounds_gdf.to_crs(src.crs)

    # Obtener la geometría del poligono
    geometries = bounds_gdf.geometry.values

    # Recortar el tiff segun poligono
    with rasterio.open(tiff_path) as src:
        out_image, out_transform = mask(src, geometries, crop=True)
        out_meta = src.meta.copy()

    # Calcular la pendiente para punto
    elevation_array = out_image[0]
    slope_array = calculate_slope(elevation_array, out_transform)

    # Crear un GeoDataFrame con los polígonos de cada celda y la pendiente calculada
    rows, cols = np.nonzero(elevation_array)  # Obtener las filas y columnas de los píxeles no nulos

    # Calcular la geometría de cada píxel
    polygons = []
    for row, col in zip(rows, cols):
        x, y = rasterio.transform.xy(out_transform, row, col, offset='ul')  
        pixel_width, pixel_height = out_transform[0], -out_transform[4] 
        polygon = box(x, y - pixel_height, x + pixel_width, y)
        polygons.append(polygon)

    # Crear el GeoDataFrame con los polígonos y sus pendientes
    slope_gdf = gpd.GeoDataFrame({
        'geometry': polygons,
        'slope': slope_array[rows, cols]
    }, crs=bounds_gdf.crs)

    pyogrio.write_dataframe( slope_gdf, "./temp/slope_gdf.geojson")

    ##CALCULAR PENDIENTE DE CADA LOTE
    slope_gdf_path = './temp/slope_gdf.geojson' 
    lots_geojson_path = args.lots_file

    slope_gdf = gpd.read_file(slope_gdf_path)
    lots_gdf = gpd.read_file(lots_geojson_path)
    lots_gdf = lots_gdf.to_crs(slope_gdf.crs)

    # Crear una nueva columna en el GeoDataFrame de lotes para almacenar la pendiente
    lots_gdf['mean_slope'] = 0.0

    # Calcular la pendiente media para cada lote
    for idx, lot in lots_gdf.iterrows():
        # Obtener la pendiente para los píxeles que intersectan con el lote
        intersecting_slopes = slope_gdf[slope_gdf.geometry.intersects(lot.geometry)]
        
        # Calcular la pendiente media (u otra estadística) de los píxeles dentro del lote
        if not intersecting_slopes.empty:
            mean_slope = intersecting_slopes['slope'].mean()
        else:
            mean_slope = None  # O algún valor por defecto si no hay intersección
        
        # Asignar la pendiente media al lote
        lots_gdf.at[idx, 'mean_slope'] = mean_slope

    output_path = './temp/lots_with_slope.geojson'
    lots_gdf.to_file(output_path, driver='GeoJSON')

    ##UPDATE DATABASE
    db_path = args.db_file

    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    # Add a new column 'mean_slope' to the 'lots' table
    cursor.execute("ALTER TABLE lots ADD COLUMN mean_slope REAL")

    connection.commit()

    # Load the GeoJSON data
    with open(output_path, 'r') as f:
        geojson_data = json.load(f)

    # Prepare the list of tuples with lot_id and mean_slope
    update_values = [
        (feature['properties']['mean_slope'], feature['properties']['ID'])  # Adjust keys if needed
        for feature in geojson_data['features']
    ]

    # Construct the SQL query with CASE statements
    update_query = """
        UPDATE lots
        SET mean_slope = CASE
    """
    # Add each CASE condition for updating specific lot_ids
    index = 0
    for mean_slope, ID in update_values:

        if( mean_slope == None):
            mean_slope = 0

        update_query += f"WHEN ID = {ID} THEN { mean_slope } "
        index += 1

    # Close the CASE statement and add the WHERE clause
    update_query += "END WHERE ID IN ({})".format(
        ", ".join(str(ID) for _, ID in update_values)
    )

    print( update_query )

    # Execute the query
    cursor.execute(update_query)

    # Commit the changes
    connection.commit()

    # Close the connection
    connection.close()
