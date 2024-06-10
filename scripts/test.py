import time

import duckdb
import geopandas as gpd
import matplotlib.pyplot as plt

FOLDER = "data/la_primavera"

if __name__ == "__main__":
    gdf = gpd.read_file(
        "https://sium.blob.core.windows.net/sium/datos/denue_infancia.geojson",
        crs="EPSG:4326",
    )
    # remove null geometries
    gdf = gdf[gdf.geometry.notnull()]
    gdf.to_file("denue_infancia.fgb", driver="FlatGeobuf")

    exit()

    gdf_bounds = gpd.read_file(f"{FOLDER}/bounds.geojson", crs="EPSG:4326")
    bbox = gdf_bounds.total_bounds
    distance = 0.01  # distancia en grados
    centroid = gdf_bounds.centroid
    bbox = centroid.buffer(distance).envelope

    start = time.time()
    gdf = gpd.read_file(
        f"{FOLDER}/building.fgb", crs="EPSG:4326", bbox=bbox, driver="FlatGeobuf"
    )
    print(time.time() - start)
    print(gdf)
    gdf.plot()
    plt.show()

    # gdf_buildings = gpd.read_file(f"{FOLDER}/landuse_building.geojson", crs="EPSG:4326")
    # gdf_buildings["ID"] = gdf_buildings["ID"].astype(int).astype(str)
    # gdf_buildings.to_file(f"{FOLDER}/building.fgb", driver="FlatGeobuf")
    exit()
    # gdf = gpd.read_file(f'{FOLDER}/building.geojson', crs='EPSG:4326')
    # gdf['ID'] = gdf['ID'].astype(int).astype(str)
    # print(gdf)
    # gdf.to_parquet(f'{FOLDER}/building.parquet')
    # exit()

    gdf_bounds = gpd.read_file(f"{FOLDER}/bounds.geojson", crs="EPSG:4326")

    # Calcular el bbox basado en una distancia del centroide
    distance = 0.005  # distancia en grados
    centroid = gdf_bounds.centroid
    bbox = centroid.buffer(distance).envelope

    # Tomar los límites del bbox
    minx, miny, maxx, maxy = bbox.total_bounds

    start = time.time()
    duckdb.sql("INSTALL spatial")
    duckdb.sql("INSTALL httpfs")
    duckdb.sql(
        """
      LOAD spatial;
      LOAD httpfs;
    """
    )

    query = f"""
      SELECT *
      FROM read_parquet('https://reimaginaurbanostorage.blob.core.windows.net/primavera/building.parquet')
      WHERE ST_Intersects(
        ST_GeomFromWKB(geometry),
        ST_MakeEnvelope({minx}, {miny}, {maxx}, {maxy})
      );
    """
    query_data = duckdb.execute(query)
    # print(query_data)
    time_taken = time.time() - start
    start2 = time.time()
    df = query_data.df()
    time_taken2 = time.time() - start2
    df["geometry"] = df["geometry"].apply(
        lambda x: bytes(x) if isinstance(x, bytearray) else x
    )
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.GeoSeries.from_wkb(df["geometry"]), crs="EPSG:4326"
    )
    gdf.plot()
    plt.show()

    # gdf = gpd.read_file(f"{FOLDER}/building.geojson", crs="EPSG:4326")
    # gdf["ID"] = gdf["ID"].astype(int).astype(str)
    # get only the buildings that are within the bounds
    # gdf = gdf.cx[buffer.minx[0] : buffer.maxx[0], buffer.miny[0] : buffer.maxy[0]]

    print(time_taken, time_taken2)
