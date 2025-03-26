import asyncio
import io
import json
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response
from shapely import Polygon
from shapely.geometry import box

from src.chat import chat_completion_request
from src.utils.db import (
    MAPPING_REDUCE_FUNCS,
    get_geometry,
    get_metrics_info,
    query_metrics,
    select_accessibility_score,
    select_minutes,
)
from src.utils.files import get_blob_url, get_file

app = FastAPI()

allowed_origins_env = os.getenv("ALLOWED_ORIGINS", "")
allowed_origins = [origin.strip()
                   for origin in allowed_origins_env.split(",") if origin.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
pool = ThreadPoolExecutor()


def read_gdf_sync(filepath, bbox=None):
    return gpd.read_file(filepath, bbox=bbox, engine="pyogrio")


async def read_gdf_async(filepath, bbox=None):
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(pool, read_gdf_sync, filepath, bbox)
    return result


def gdf_from_coords(coords: List[List[float]]) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(geometry=[Polygon(x) for x in coords], crs="EPSG:4326")


async def load_gdf(layer: str, polygon_gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    bbox = box(*polygon_gdf.total_bounds)
    gdf = await read_gdf_async(get_file(get_blob_url(layer)), bbox)
    gdf = gdf[gdf.intersects(polygon_gdf.unary_union)]
    return gdf


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/coords")
async def get_coordinates(project: str = None):
    if not project:
        project = "primavera"

    gdf_bounds = await read_gdf_async(get_file(get_blob_url(f"{project}_bounds.fgb")), None)
    geom = gdf_bounds.unary_union
    return {"latitude": geom.centroid.y, "longitude": geom.centroid.x}


@app.post("/query")
async def custom_query(payload: Dict[Any, Any]):
    metrics = payload.get("metrics")
    condition = payload.get("condition")
    coordinates = payload.get("coordinates")
    proximity_mapping = payload.get("accessibility_info")
    payload["group_ages"] = [POB_AGES_METRICS_MAPPING[age]
                             for age in payload["group_ages"]]
    level = payload.get("level", "blocks")

    id = "block_id" if level == "blocks" else "lot_id"

    # TODO: Integrate so that it includes all selected metrics (including minutes and accessibility_score)
    if "minutes" in metrics:
        df = select_minutes(level, coordinates, proximity_mapping)
        df = df[[id, "minutes"]]
        df = df.rename(columns={"minutes": "value"})
    elif "accessibility_score" in metrics:
        df = select_accessibility_score(level, coordinates, proximity_mapping)
        df["accessibility_score"] = np.log(
            df["accessibility_score"] + 1) * 17
        df = df[[id, "accessibility_score"]]
        df = df.rename(columns={"accessibility_score": "value"})
    else:
        df = query_metrics(level, metrics, coordinates, payload)
    df = df.replace([np.inf, -np.inf], 0).fillna(0)
    print('--- Data ---')
    print(df)
    df_dict = df.to_dict(orient="records")
    quantiles = df["value"].quantile([0, 0.2, 0.4, 0.6, 0.8, 1])
    dict_quantiles = quantiles.to_dict()
    dict_quantiles = {str(k): v for k, v in dict_quantiles.items()}
    dict_quantiles["mean"] = df["value"].mean()
    dict_quantiles["std"] = df["value"].std()

    return {"stats_info": dict_quantiles, "data": df_dict}


POB_AGES_METRICS_MAPPING = {
    "0-2": "0a2",
    "3-5": "3a5",
    "6-11": "6a11",
    "12-14": "12a14",
    "15-17": "15a17",
    "18-24": "18a24",
    "25-59": "25a59",
    "60+": "60ymas",
}


@app.post("/predios")
async def get_info(payload: Dict[Any, Any]):
    coordinates = payload.get("coordinates", None)
    level = payload.get("type", "blocks")
    # proximity_mapping = payload.get("accessibility_info")
    payload["group_ages"] = [POB_AGES_METRICS_MAPPING[age]
                             for age in payload["group_ages"]]
    # TODO: Pass cols instead of hardcoded
    cols = [
        "poblacion",
        "viviendas_habitadas",
        "viviendas_habitadas_percent",
        "viviendas_deshabitadas",
        "viviendas_deshabitadas_percent",
        "grado_escuela",
        "area",
        "indice_bienestar",
        "viviendas_tinaco",
        "viviendas_pc",
        "viviendas_auto",
        "density",
        "cos",
        "max_cos",
        "cus",
        "max_cus",
        "max_density",
        "max_num_levels",
        "home_units",
        "max_home_units",
        "subutilizacion",
        "num_levels",
        "per_female_group_ages",
        "per_male_group_ages",
        "per_group_ages",
        "slope",
    ]
    try:
        df = reduce_data(cols, level, payload)
        df = df.replace([np.inf, -np.inf], 0).fillna(0)
        results = df.to_dict()
        print('--- Data Reduce ---')
        print(df)
    except Exception as e:
        print(e)
        results = {}
    return results


@app.get("/polygon/{layer}")
async def get_polygon(layer: str):
    return FileResponse(get_file(get_blob_url(f"{layer}.fgb")))


@app.post("/polygon")
async def get_polygon_segment(payload: Dict[Any, Any]):
    layer = payload.get("layer")
    coordinates = payload.get("coordinates")

    if layer == "blocks" or layer == "lots" or layer == "amenities" or layer == "accessibility_points":
        gdf = get_geometry(layer, coordinates)
        print('--- Polygon NO coordinates ---')
        print(gdf)
        with io.BytesIO() as output:
            pyogrio.write_dataframe(gdf, output, driver="FlatGeobuf")
            contents = output.getvalue()
            return Response(content=contents, media_type="application/octet-stream")
    else:
        if not coordinates or len(coordinates) == 0:
            return FileResponse(get_file(get_blob_url(f"{layer}.fgb")))

        layerFile = get_file(get_blob_url(f"{layer}.fgb"))
        polygon_gdf = gpd.GeoDataFrame(
            geometry=[Polygon(x) for x in coordinates], crs="EPSG:4326")
        bbox = box(*polygon_gdf.total_bounds)
        gdf = await read_gdf_async(layerFile, bbox)
        gdf = gdf[gdf.intersects(polygon_gdf.unary_union)]
        print('--- Polygon with coordinates ---')
        print(gdf)

        with io.BytesIO() as output:
            pyogrio.write_dataframe(gdf, output, driver="FlatGeobuf")
            contents = output.getvalue()
            return Response(content=contents, media_type="application/octet-stream")


def reduce_data(cols: List[str], level: str, payload: Dict[Any, Any]):
    coordinates = payload.get("coordinates", None)
    proximity_mapping = payload.get("accessibility_info")
    df = query_metrics(level, {col: col for col in cols}, coordinates, payload)
    new_cols = get_metrics_info(cols)
    new_cols = {k: v for k, v in zip(cols, new_cols)}
    if level == "lots":
        df = df.groupby("block_id").aggregate({
            k:
            "min"
            if v["level"] != "lots" else MAPPING_REDUCE_FUNCS[v["reduce"]]
            for k, v in new_cols.items()
        })
    df = df.replace([np.inf, -np.inf], 0).fillna(0)
    df = df.aggregate({
        k: MAPPING_REDUCE_FUNCS[v["reduce"]]
        for k, v in new_cols.items()
    })
    id = "block_id" if level == "blocks" else "lot_id"
    df_minutes = select_minutes(level, coordinates, proximity_mapping)
    df_minutes = df_minutes[[id, "minutes"]]
    df_minutes = df_minutes.aggregate({"minutes": "mean"})
    if not df_minutes.empty:
        df = pd.concat([df, df_minutes])
    print(df_minutes)

    # df_amenity = select_furthest_amenity(level, coordinates, proximity_mapping)
    # df_amenity = df_amenity[[id, "amenity"]]
    # df_amenity = df_amenity.aggregate(
    #     {"amenity": lambda x: x.value_counts().idxmax()})
    # if not df_amenity.empty:
    #     df = pd.concat([df, df_amenity])

    # df_accessibility = select_accessibility_score(level, coordinates,
    #                                               proximity_mapping)
    # df_accessibility['accessibility_score'] = np.log(
    #     df_accessibility['accessibility_score'] + 1) * 17
    # df_accessibility = df_accessibility[[id, "accessibility_score"]]
    # df_accessibility = df_accessibility.aggregate(
    #     {"accessibility_score": "mean"})
    # if not df_accessibility.empty:
    #     df = pd.concat([df, df_accessibility])
    return df


@app.post("/stats")
async def extract_insights(payload: Dict[Any, Any]):
    coordinates = payload.get("coordinates")
    coordinates_compare = payload.get("coordinates_compare")
    level = payload.get("type", "blocks")
    message = payload.get("message", None)

    if not coordinates or len(coordinates) == 0:
        return {"error": "Missing coordinates"}

    # cols = [k for k, v in METRIC_MAPPING.items() if v["reduce"] == "avg"]
    cols = [
        'viviendas_habitadas_percent',
        'viviendas_deshabitadas_percent',
        'grado_escuela',
        # 'indice_bienestar',
        'viviendas_tinaco',
        'viviendas_pc',
        'viviendas_auto',
        'density',
        'cos',
        'max_cos',
        'cus',
        'max_cus',
        'max_density',
        'max_num_levels',
        'subutilizacion',
        'per_p_0a2_m',
        'per_p_0a2_f',
        'per_p_3a5_m',
        'per_p_3a5_f',
        'per_p_6a11_m',
        'per_p_6a11_f',
        'per_p_12a14_m',
        'per_p_12a14_f',
        'per_p_15a17_m',
        'per_p_15a17_f',
        'per_p_18a24_m',
        'per_p_18a24_f',
        'per_p_25a59_m',
        'per_p_25a59_f',
        'per_p_60ymas_m',
        'per_p_60ymas_f',
        # 'num_levels',
        # 'slope',
    ]

    # extract all the data from the database for both coordinates
    df1 = query_metrics(
        level, {col: col for col in cols}, coordinates, payload)
    df1 = reduce_data(df1, cols, level)
    df2 = query_metrics(
        level, {col: col for col in cols}, coordinates_compare, payload)
    df2 = reduce_data(df2, cols, level)

    df_obs = df2[cols] - df1[cols]

    # new df with columns of df1, df2, and differences, with metrics as rows and columns as df1, df2, and differences
    new_df = pd.DataFrame(columns=["Zona de interés", "Zona metropolitana",
                          "Diferencia (Zona de interés - Zona metropolitana)"])
    for col in cols:
        new_df = pd.concat(
            [new_df, pd.DataFrame({
                "Zona de interés": df1[col],
                "Zona metropolitana": df2[col],
                "Diferencia (Zona de interés - Zona metropolitana)": df_obs[col]
            }, index=[col])
            ]
        )

    PROMPT = """
    Como analista de datos especializado en desarrollo urbano, vivienda y análisis demográfico, tu objetivo es generar un análisis conciso, relevante y basado en datos, destacando únicamente los aspectos más significativos.

    Tu tarea consiste en analizar la tabla de métricas proporcionada, comparando una zona de interés con su zona metropolitana de referencia. Para ello, sigue estas reglas:

    1. **Selecciona solo las métricas más relevantes**: Considera únicamente aquellas con una diferencia significativa entre ambas zonas, ya sea positiva o negativa. Ignora métricas que sean redundantes, tengan valores similares o no aporten un análisis claro.
    
    2. **Evita repetir información**: Si dos métricas expresan conceptos complementarios (ejemplo: porcentaje de viviendas habitadas y deshabitadas), elige solo una. Prioriza la que mejor represente el fenómeno.

    3. **Construye una narrativa unificada**: En lugar de listar métricas individualmente, integra los hallazgos en un solo párrafo fluido, conectando diferentes aspectos que puedan estar relacionados. Explica cómo ciertas diferencias pueden influenciar otras y qué implicaciones tienen.

    4. **Usa datos de forma estratégica**: Incluye cifras específicas solo cuando sean esenciales para reforzar un punto clave. Evita sobrecargar el texto con números innecesarios.

    5. **Sé claro y directo**: Redacta en un lenguaje accesible para una audiencia no técnica, manteniendo precisión y rigor analítico.

    Genera un solo párrafo con los insights clave de la comparación.
    """

    MESSAGES = [{"role": "system", "content": PROMPT}]
    if message:
        MESSAGES.append({"role": "user", "content": message})

    tools = [
        {
            "type": "function",
            "function": {
                "name": "chat_completion_request",
                "description": "Genera un análisis comparativo entre dos ubicaciones geográficas. Se enfoca en identificar solo las métricas más significativas y unirlas en un análisis cohesivo.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "metrics": {
                            "type": "array",
                            "description": "Lista de métricas seleccionadas por su relevancia. Solo aquellas con una diferencia significativa y sin redundancias.",
                            "items": {
                                "type": "string",
                                "enum": cols,
                            },
                        },
                        "description": {
                            "type": "string",
                            "description": "Narrativa clara y cohesionada de los insights generados, resaltando las diferencias más importantes y sus implicaciones.",
                        },
                    },
                    "required": ["metrics", "description"],
                },
            },
        }
    ]

    user_message = """
    Se presenta una tabla con métricas comparativas entre una zona de interés y la zona metropolitana de referencia. La tabla incluye los valores de cada métrica en ambas zonas y la diferencia entre ellas.

    Tabla:
    {table_columns}

    Analiza estos datos y genera un análisis comparativo en un solo párrafo, identificando únicamente las diferencias más relevantes y conectando los hallazgos en una narrativa clara y cohesionada.
    """

    MESSAGES.append({"role": "user", "content": user_message.format(
        table_columns=new_df.to_markdown())})
    chat_response = chat_completion_request(MESSAGES, tools=tools)

    json_data = chat_response.choices[0].message.tool_calls[0].function.arguments
    data = json.loads(json_data)

    description = data["description"]
    metrics = data["metrics"]

    print(metrics)
    print(description)

    return {"metrics": metrics, "description": description}
