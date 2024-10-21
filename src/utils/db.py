import os
from typing import List, Dict
from sqlalchemy import create_engine, func, Table, MetaData
from sqlalchemy.orm import Session, aliased
from functools import lru_cache
import pandas as pd
from dotenv import load_dotenv

MAPPING_REDUCE_FUNCS = {
    "sum": "sum",
    "avg": "mean",
    "min": "min",
    "max": "max",
}
METRIC_MAPPING = {
    "poblacion": {
        "query": lambda T: T.c.pobtot,
        "reduce": "sum",
        "level": "blocks",
    },
    "viviendas_habitadas": {
        "query": lambda T: T.c.vivpar_hab,
        "reduce": "sum",
        "level": "blocks",
    },
    "viviendas_deshabitadas": {
        "query": lambda T: func.greatest(T.c.vivpar_des * 1.0 / func.nullif(T.c.vivpar_hab, 0) * 100, 0),
        "reduce": "avg",
        "level": "blocks",
    },
    "grado_escuela": {
        "query": lambda T: T.c.graproes,
        "reduce": "avg",
        "level": "blocks",
    },
    "indice_bienestar": {
        "query": lambda T: T.c.puntuaje_hogar_digno,
        "reduce": "avg",
        "level": "blocks",
    },
    "viviendas_tinaco": {
        "query": lambda T: func.greatest(T.c.vph_tinaco * 1.0 / func.nullif(T.c.vivpar_hab, 0) * 100, 0),
        "reduce": "avg",
        "level": "blocks",
    },
    "viviendas_pc": {
        "query": lambda T: func.greatest(T.c.vph_pc * 1.0 / func.nullif(T.c.vivpar_hab, 0) * 100, 0),
        "reduce": "avg",
        "level": "blocks",
    },
    "viviendas_auto": {
        "query": lambda T: func.greatest(T.c.vph_autom * 1.0 / func.nullif(T.c.vivpar_hab, 0) * 100, 0),
        "reduce": "avg",
        "level": "blocks",
    },
    "accessibility_score": {
        "query": lambda T: T.c.accessibility_score * 100,
        "reduce": "avg",
        "level": "blocks",
    },
    "minutes": {
        "query": lambda T: T.c.minutes,
        "reduce": "avg",
        "level": "blocks",
    },
    "density": {
        "query": lambda T: T.c.home_density,
        "reduce": "avg",
        "level": "lots",
    },
    "max_height": {
        "query": lambda T: T.c.max_height,
        "reduce": "avg",
        "level": "lots",
    },
    "potencial": {
        "query": lambda T: T.c.potential_new_units,
        "reduce": "sum",
        "level": "lots",
    },
    "subutilizacion": {
        "query": lambda T: func.least(1 - (T.c.units_estimate * 100.0 / func.nullif(T.c.max_home_units, 0)), 100),
        "reduce": "sum",
        "level": "lots",
    },
    "subutilizacion_type": {
        "query": lambda T: T.c.max_height,
        "reduce": "sum",
        "level": "lots",
    },
}

APP_ENV = os.getenv('APP_ENV', 'local')
load_dotenv(f'.env.{APP_ENV}')


@lru_cache()
def get_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST")
    port = os.getenv("POSTGRES_PORT")
    db = os.getenv("POSTGRES_DB")

    connection_string = f"postgresql+psycopg2://{
        user}:{password}@{host}:{port}/{db}"
    return create_engine(connection_string)


def query_metrics(level: str, metrics: Dict[str, str], ids: List[str] = None):
    # TODO: Refactor code since it is too unnecessarily complex
    engine = get_engine()
    metadata = MetaData()
    Blocks = Table('blocks', metadata, autoload_with=engine)
    Lots = Table('lots', metadata, autoload_with=engine)

    with Session(engine) as session:
        # Aliases for easy access to both tables
        lots_alias = aliased(Lots)
        blocks_alias = aliased(Blocks)

        # Start with a base query that will be modified based on level and metrics
        base_query = session.query()

        # Determine the selected level
        if level == "blocks":
            base_query = base_query.add_columns(
                blocks_alias.c.cvegeo.label("cvegeo"))
            join_condition = lots_alias.c.cvegeo == blocks_alias.c.cvegeo
            base_query = base_query.select_from(
                blocks_alias).join(lots_alias, join_condition)

            for metric, new_metric in metrics.items():
                # Check if the metric belongs to Lots or Blocks
                if METRIC_MAPPING[metric]["level"] == "lots":
                    # Apply the appropriate aggregation function based on reduce_mappings
                    if metric in METRIC_MAPPING:
                        func_reduce = getattr(
                            func, METRIC_MAPPING[metric]["reduce"])
                        # agg_function = METRIC_MAPPING[metric]["reduce"]
                        _metric = METRIC_MAPPING[metric]["query"](lots_alias)
                        # base_query = base_query.add_columns(func.min(_metric).label(new_metric))
                        base_query = base_query.add_columns(
                            func_reduce(_metric).label(new_metric))
                    else:
                        raise ValueError(
                            f"Metric {metric} has no defined aggregation.")
                elif METRIC_MAPPING[metric]["level"] == "blocks":
                    _metric = METRIC_MAPPING[metric]["query"](blocks_alias)
                    # base_query = base_query.add_columns(
                    #     _metric.label(new_metric))
                    base_query = base_query.add_columns(
                        func.min(_metric).label(new_metric))
                else:
                    raise ValueError(
                        f"Metric {metric} not found in either Lots or Blocks.")
            # Group by the block to ensure the aggregation happens per block
            base_query = base_query.group_by(blocks_alias.c.cvegeo)

            if ids:
                base_query = base_query.filter(blocks_alias.c.cvegeo.in_(ids))

        elif level == "lots":
            base_query = base_query.add_columns(
                lots_alias.c.lot_id.label("lot_id"))
            base_query = base_query.add_columns(
                lots_alias.c.cvegeo.label("cvegeo"))

            for metric, new_metric in metrics.items():
                # Check if the metric belongs to Lots or Blocks
                if METRIC_MAPPING[metric]["level"] == "lots":
                    _metric = METRIC_MAPPING[metric]["query"](lots_alias)
                    base_query = base_query.add_columns(
                        _metric.label(new_metric))
                elif METRIC_MAPPING[metric]["level"] == "blocks":
                    # If we are at the "lots" level, and the metric comes from Blocks, simply return the value from Lots
                    _metric = METRIC_MAPPING[metric]["query"](blocks_alias)
                    base_query = base_query.add_columns(
                        _metric.label(new_metric))
                else:
                    raise ValueError(
                        f"Metric {metric} not found in either Lots or Blocks.")

            # Perform join if we need any metrics from Blocks
            if any(METRIC_MAPPING[metric]["level"] == "blocks" for metric in metrics):
                join_condition = lots_alias.c.cvegeo == blocks_alias.c.cvegeo
                base_query = base_query.join(blocks_alias, join_condition)
            if ids:
                base_query = base_query.filter(lots_alias.c.lot_id.in_(ids))
        else:
            raise ValueError(f"Unknown level: {level}")
        print(base_query)

        df = pd.read_sql(base_query.statement, session.bind)
        return df


def select_minutes(
    level: str, ids: List[str], amenities: List[str]
):
    engine = get_engine()
    metadata = MetaData()
    Blocks = Table('blocks', metadata, autoload_with=engine)
    Blocks = aliased(Blocks)
    Lots = Table('lots', metadata, autoload_with=engine)
    Lots = aliased(Lots)
    AccessibilityTrips = Table(
        'accessibility_trips', metadata, autoload_with=engine)
    with Session(engine) as session:
        query = session.query(
            func.min(AccessibilityTrips.c.amenity).label("amenity"),
            func.max(AccessibilityTrips.c.distance).label("distance"),
            func.max(AccessibilityTrips.c.minutes).label("minutes"),
        )
        column = Blocks.c.cvegeo if level == "blocks" else Lots.c.lot_id
        query = query.add_columns(func.min(Blocks.c.cvegeo).label("cvegeo"))
        query = query.select_from(AccessibilityTrips).join(
            Blocks, AccessibilityTrips.c.origin_id == Blocks.c.node_ids)
        if level == "lots":
            query = query.add_columns(func.min(Lots.c.lot_id).label("lot_id"))
            query = query.join(Lots, Blocks.c.cvegeo == Lots.c.cvegeo)
        if ids:
            query = query.filter(column.in_(ids))
        query = query.filter(AccessibilityTrips.c.num_amenity == 1)
        if amenities:
            query = query.filter(AccessibilityTrips.c.amenity.in_(amenities))
        query = query.group_by(column)
        df = pd.read_sql(query.statement, session.bind)
        return df
