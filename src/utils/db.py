import os
from typing import List, Dict
from sqlalchemy import create_engine, func, Table, MetaData, case, select, desc
from sqlalchemy.sql import literal_column
from sqlalchemy.orm import Session, aliased
from functools import lru_cache
import pandas as pd


def percent(numerator, denominator):
    return case(
        (denominator == 0, 0),
        else_=numerator * 100.0 / func.nullif(denominator, 0)
    )


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
    "viviendas_habitadas_percent": {
        "query": lambda T: percent(T.c.vivpar_hab, T.c.vivtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "viviendas_deshabitadas": {
        "query": lambda T: T.c.vivpar_des,
        "reduce": "sum",
        "level": "blocks",
    },
    "viviendas_deshabitadas_percent": {
        "query": lambda T: percent(T.c.vivpar_des, T.c.vivtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "grado_escuela": {
        "query": lambda T: T.c.graproes,
        "reduce": "avg",
        "level": "blocks",
    },
    "indice_bienestar": {
        "query": lambda T: T.c.puntuaje_hogar_digno * 100,
        "reduce": "avg",
        "level": "blocks",
    },
    "viviendas_tinaco": {
        "query": lambda T: percent(T.c.vph_tinaco, T.c.vivpar_hab),
        "reduce": "avg",
        "level": "blocks",
    },
    "viviendas_pc": {
        "query": lambda T: percent(T.c.vph_pc, T.c.vivpar_hab),
        "reduce": "avg",
        "level": "blocks",
    },
    "viviendas_auto": {
        "query": lambda T: percent(T.c.vph_autom, T.c.vivpar_hab),
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
        "query": lambda T: T.c.density,
        "reduce": "avg",
        "level": "lots",
    },
    "cos": {
        "query": lambda T: T.c.cos,
        "reduce": "avg",
        "level": "lots",
    },
    "max_cos": {
        "query": lambda T: T.c.max_cos,
        "reduce": "avg",
        "level": "lots",
    },
    "cus": {
        "query": lambda T: T.c.cus,
        "reduce": "avg",
        "level": "lots",
    },
    "max_cus": {
        "query": lambda T: T.c.max_cus,
        "reduce": "avg",
        "level": "lots",
    },
    "home_units": {
        "query": lambda T: T.c.home_units,
        "reduce": "sum",
        "level": "lots",
    },
    "max_density": {
        "query": lambda T: T.c.max_density,
        "reduce": "avg",
        "level": "lots",
    },
    "area": {
        "query": lambda T: T.c.lot_area / 10_000,
        "reduce": "sum",
        "level": "lots",
    },
    "max_num_levels": {
        "query": lambda T: T.c.max_num_levels,
        "reduce": "avg",
        "level": "lots",
    },
    "max_home_units": {
        "query": lambda T: T.c.max_home_units,
        "reduce": "sum",
        "level": "lots",
    },
    "subutilizacion": {
        "query": lambda T: func.greatest(func.least(1 - (T.c.home_units / func.nullif(T.c.max_home_units, 0)), 1), 0) * 100,
        "reduce": "avg",
        "level": "lots",
    },
    "potential_population": {
        "query": lambda T: T.c.potential_home_units * T.c.prom_ocup,
        "reduce": "sum",
        "level": "lots",
    },
    "subutilizacion_type": {
        "query": lambda T: T.c.num_levels,
        "reduce": "sum",
        "level": "lots",
    },
    "per_p_0a2_m": {
        "query": lambda T: percent(T.c.p_0a2_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_0a2_f": {
        "query": lambda T: percent(T.c.p_0a2_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_3a5_m": {
        "query": lambda T: percent(T.c.p_3a5_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_3a5_f": {
        "query": lambda T: percent(T.c.p_3a5_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_6a11_m": {
        "query": lambda T: percent(T.c.p_6a11_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_6a11_f": {
        "query": lambda T: percent(T.c.p_6a11_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_12a14_m": {
        "query": lambda T: percent(T.c.p_12a14_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_12a14_f": {
        "query": lambda T: percent(T.c.p_12a14_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_15a17_m": {
        "query": lambda T: percent(T.c.p_15a17_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_15a17_f": {
        "query": lambda T: percent(T.c.p_15a17_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_18a24_m": {
        "query": lambda T: percent(T.c.p_18a24_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_18a24_f": {
        "query": lambda T: percent(T.c.p_18a24_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_25a59_m": {
        "query": lambda T: percent(T.c.p_25a59_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_25a59_f": {
        "query": lambda T: percent(T.c.p_25a59_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_60ymas_m": {
        "query": lambda T: percent(T.c.p_60ymas_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_60ymas_f": {
        "query": lambda T: percent(T.c.p_60ymas_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "num_levels": {
        "query": lambda T: T.c.num_levels,
        "reduce": "avg",
        "level": "lots",
    },
    "max_num_levels": {
        "query": lambda T: T.c.max_num_levels,
        "reduce": "avg",
        "level": "lots",
    },
    "num_levels": {
        "query": lambda T: T.c.num_levels,
        "reduce": "avg",
        "level": "lots",
    },
    "slope": {
        "query": lambda T: T.c.num_levels,
        "reduce": "avg",
        "level": "lots",
    }
}


def get_metric(metric: str, Lots, Blocks):
    if metric in METRIC_MAPPING:
        return METRIC_MAPPING[metric]
    else:
        if hasattr(Lots.c, metric):
            return {
                "query": lambda T: getattr(T.c, metric),
                "reduce": "sum",
                "level": "lots",
            }
        elif hasattr(Blocks.c, metric):
            return {
                "query": lambda T: getattr(T.c, metric),
                "reduce": "sum",
                "level": "blocks",
            }


@lru_cache()
def get_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "reimaginaurbano")

    connection_string = f"postgresql+psycopg2://{
        user}:{password}@{host}:{port}/{db}"
    return create_engine(connection_string)


def get_metrics_info(metrics: List[str]):
    engine = get_engine()
    metadata = MetaData()
    Blocks = Table('blocks', metadata, autoload_with=engine)
    Lots = Table('lots', metadata, autoload_with=engine)
    return [get_metric(metric, Lots, Blocks) for metric in metrics]


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
                metric_info = get_metric(metric, Lots, Blocks)
                if metric_info["level"] == "lots":
                    func_reduce = getattr(
                        func, metric_info["reduce"])
                    _metric = metric_info["query"](lots_alias)
                    base_query = base_query.add_columns(
                        func_reduce(_metric).label(new_metric))
                elif metric_info["level"] == "blocks":
                    _metric = metric_info["query"](blocks_alias)
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
                metric_info = get_metric(metric, Lots, Blocks)
                if metric_info["level"] == "lots":
                    _metric = metric_info["query"](lots_alias)
                    base_query = base_query.add_columns(
                        _metric.label(new_metric))
                elif metric_info["level"] == "blocks":
                    # If we are at the "lots" level, and the metric comes from Blocks, simply return the value from Lots
                    _metric = metric_info["query"](blocks_alias)
                    base_query = base_query.add_columns(
                        _metric.label(new_metric))
                else:
                    raise ValueError(
                        f"Metric {metric} not found in either Lots or Blocks.")

            # Perform join if we need any metrics from Blocks
            if any(get_metric(metric, Lots, Blocks)["level"] == "blocks" for metric in metrics):
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


def select_furthest_amenity(level: str, ids: List[str], amenities: List[str]):
    engine = get_engine()
    metadata = MetaData()
    Blocks = Table('blocks', metadata, autoload_with=engine)
    Lots = Table('lots', metadata, autoload_with=engine)
    AccessibilityTrips = Table(
        'accessibility_trips', metadata, autoload_with=engine)

    with Session(engine) as session:
        # Aliased table for ranking rows within each origin_id
        ranked_trips = (
            session.query(
                AccessibilityTrips.c.origin_id,
                AccessibilityTrips.c.amenity,
                AccessibilityTrips.c.distance,
                AccessibilityTrips.c.minutes,
                func.row_number().over(
                    partition_by=AccessibilityTrips.c.origin_id,
                    order_by=desc(AccessibilityTrips.c.minutes)
                ).label("rn")
            )
            .filter(AccessibilityTrips.c.num_amenity == 1)
            .subquery()
        )

        # Define the column to filter based on the level
        column = Blocks.c.cvegeo if level == "blocks" else Lots.c.lot_id

        # Main query
        query = session.query(
            ranked_trips.c.amenity.label("amenity"),
            ranked_trips.c.distance.label("distance"),
            ranked_trips.c.minutes.label("minutes"),
            Blocks.c.cvegeo.label("cvegeo")
        )

        # Join with Blocks table
        query = query.join(
            Blocks, ranked_trips.c.origin_id == Blocks.c.node_ids)

        # Additional logic if level is "lots"
        if level == "lots":
            query = query.add_columns(Lots.c.lot_id.label("lot_id"))
            query = query.join(Lots, Blocks.c.cvegeo == Lots.c.cvegeo)

        # Apply filter for specific IDs if provided
        if ids:
            query = query.filter(column.in_(ids))

        # Additional filter for amenities if provided
        if amenities:
            query = query.filter(ranked_trips.c.amenity.in_(amenities))

        # Only keep rows where row number is 1 (furthest `amenity` per origin_id)
        query = query.filter(ranked_trips.c.rn == 1)

        # Execute and return the result as DataFrame
        df = pd.read_sql(query.statement, session.bind)
        return df


def select_accessibility_score(
    level: str, ids: List[str], amenities: List[str]
):
    engine = get_engine()  # Ensure this function is defined to get the engine
    metadata = MetaData()
    Blocks = Table('blocks', metadata, autoload_with=engine)
    Blocks = aliased(Blocks)
    Lots = Table('lots', metadata, autoload_with=engine)
    AccessibilityTrips = Table(
        'accessibility_trips', metadata, autoload_with=engine)

    with Session(engine) as session:
        # Step 1: Precompute Rj values for each destination_id as a subquery
        rj_subquery = (
            select(
                AccessibilityTrips.c.destination_id,
                (func.min(AccessibilityTrips.c.attraction) /
                 func.nullif(
                     func.sum(AccessibilityTrips.c.population * AccessibilityTrips.c.gravity), 0)
                 ).label('rj')
            )
            .group_by(AccessibilityTrips.c.destination_id)
            .subquery()
        )

        # Step 2: Calculate ai using the Rj subquery joined with AccessibilityTrips, grouped by origin_id and amenity
        intermediate_query = session.query(
            AccessibilityTrips.c.origin_id.label("origin_id"),
            AccessibilityTrips.c.amenity.label("amenity"),
            func.sum(rj_subquery.c.rj *
                     AccessibilityTrips.c.gravity).label('accessibility_score')
        )

        # Define column for the selected level
        id = "cvegeo" if level == "blocks" else "lot_id"
        column = Blocks.c.cvegeo if level == "blocks" else Lots.c.lot_id
        intermediate_query = intermediate_query.add_columns(column.label(id))

        # Join with Blocks, Lots, and the Rj subquery
        intermediate_query = intermediate_query.select_from(AccessibilityTrips).join(
            rj_subquery, AccessibilityTrips.c.destination_id == rj_subquery.c.destination_id
        ).join(
            Blocks, AccessibilityTrips.c.origin_id == Blocks.c.node_ids
        )

        if level == "lots":
            intermediate_query = intermediate_query.join(
                Lots, Blocks.c.cvegeo == Lots.c.cvegeo)
            # intermediate_query = intermediate_query.add_columns(func.min(Lots.c.lot_id).label("lot_id"))

        # Apply filters for ids and amenities if provided
        if ids:
            intermediate_query = intermediate_query.filter(column.in_(ids))
        # Apply filters for amenities if provided
        if amenities:
            intermediate_query = intermediate_query.filter(
                AccessibilityTrips.c.amenity.in_(amenities))

        # Group the intermediate result by level, amenity, and origin_id
        intermediate_query = intermediate_query.group_by(
            column, AccessibilityTrips.c.amenity, AccessibilityTrips.c.origin_id)

        # Step 3: Execute the intermediate query and store it as a DataFrame
        intermediate_df = pd.read_sql(
            intermediate_query.statement, session.bind)

        # Step 4: Aggregate the final accessibility score by origin_id only
        final_df = intermediate_df.groupby(id, as_index=False).agg({
            "accessibility_score": "sum"
        })
        if amenities:
            final_df["accessibility_score"] = final_df["accessibility_score"] / \
                len(amenities)
        else:
            final_df["accessibility_score"] = final_df["accessibility_score"] / \
                len(intermediate_df["amenity"].unique())

        return final_df
