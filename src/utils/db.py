import os
from functools import cache
from typing import Any, Dict, List

import geopandas as gpd
import pandas as pd
from geoalchemy2 import Geometry
from sqlalchemy import JSON, Column, MetaData, Table, Text, case, create_engine, desc, func, select, text
from sqlalchemy.orm import Session, aliased
from starlette.concurrency import run_in_threadpool


def percent(numerator, denominator):
    return case(
        (denominator == 0, 0),
        (numerator > denominator, 100),
        else_=numerator * 100.0 / func.nullif(denominator, 0)
    )


def get_per_group_ages(T, group_ages, metric):
    if not group_ages:
        return 0
    type_mapping = {
        "pobtot": "",
        "pobmas": "_m",
        "pobfem": "_f",
    }
    female_ages = [f"p_{age}{type_mapping[metric]}" for age in group_ages]
    total_female = sum([
        getattr(T.c, age)
        for age in female_ages
    ])
    return percent(total_female, getattr(T.c, metric))


MAPPING_REDUCE_FUNCS = {
    "sum": "sum",
    "avg": "mean",
    "min": "min",
    "max": "max",
}
METRIC_MAPPING = {
    "poblacion": {
        "query": lambda T, _: T.c.pobtot,
        "reduce": "sum",
        "level": "blocks",
    },
    "viviendas_habitadas": {
        "query": lambda T, _: T.c.tvivparhab,
        "reduce": "sum",
        "level": "blocks",
    },
    "viviendas_habitadas_percent": {
        "query": lambda T, _: percent(T.c.tvivparhab, T.c.vivtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "viviendas_deshabitadas": {
        "query": lambda T, _: T.c.vivpar_des,
        "reduce": "sum",
        "level": "blocks",
    },
    "viviendas_deshabitadas_percent": {
        "query": lambda T, _: percent(T.c.vivpar_des, T.c.vivtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "grado_escuela": {
        "query": lambda T, _: T.c.graproes,
        "reduce": "avg",
        "level": "blocks",
    },
    "indice_bienestar": {
        "query": lambda T, _: T.c.household_wellbeing * 100,
        "reduce": "avg",
        "level": "blocks",
    },
    "viviendas_tinaco": {
        "query": lambda T, _: percent(T.c.vph_tinaco, T.c.vivparh_cv),
        "reduce": "avg",
        "level": "blocks",
    },
    "viviendas_pc": {
        "query": lambda T, _: percent(T.c.vph_pc, T.c.vivparh_cv),
        "reduce": "avg",
        "level": "blocks",
    },
    "viviendas_auto": {
        "query": lambda T, _: percent(T.c.vph_autom, T.c.vivparh_cv),
        "reduce": "avg",
        "level": "blocks",
    },
    "density": {
        "query": lambda T, _: T.c.density * 10000,
        "reduce": "avg",
        "level": "lots",
    },
    "cos": {
        "query": lambda T, _: T.c.cos,
        "reduce": "avg",
        "level": "lots",
    },
    "max_cos": {
        "query": lambda T, _: T.c.max_cos,
        # "query": lambda T, _: T.c.lot_area,
        "reduce": "avg",
        "level": "lots",
    },
    "cus": {
        "query": lambda T, _: T.c.cus,
        "reduce": "avg",
        "level": "lots",
    },
    "max_cus": {
        "query": lambda T, _: T.c.max_cus,
        "reduce": "avg",
        "level": "lots",
    },
    "home_units": {
        "query": lambda T, _: T.c.home_units,
        "reduce": "sum",
        "level": "lots",
    },
    "max_density": {
        "query": lambda T, _: T.c.max_density,
        "reduce": "avg",
        "level": "lots",
    },
    "area": {
        "query": lambda T, _: T.c.lot_area / 10_000,
        "reduce": "sum",
        "level": "lots",
    },
    "max_num_levels": {
        "query": lambda T, _: T.c.max_num_levels,
        "reduce": "avg",
        "level": "lots",
    },
    "max_home_units": {
        "query": lambda T, _: T.c.max_home_units,
        "reduce": "sum",
        "level": "lots",
    },
    "subutilizacion": {
        "query": lambda T, _: func.greatest(func.least(1 - (T.c.home_units / func.nullif(T.c.max_home_units, 0)), 1), 0) * 100,
        "reduce": "avg",
        "level": "lots",
    },
    "potential_population": {
        "query": lambda T, _: T.c.potential_home_units * T.c.prom_ocup,
        "reduce": "sum",
        "level": "lots",
    },
    "per_p_0a2_m": {
        "query": lambda T, _: percent(T.c.p_0a2_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_0a2_f": {
        "query": lambda T, _: percent(T.c.p_0a2_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_3a5_m": {
        "query": lambda T, _: percent(T.c.p_3a5_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_3a5_f": {
        "query": lambda T, _: percent(T.c.p_3a5_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_6a11_m": {
        "query": lambda T, _: percent(T.c.p_6a11_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_6a11_f": {
        "query": lambda T, _: percent(T.c.p_6a11_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_12a14_m": {
        "query": lambda T, _: percent(T.c.p_12a14_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_12a14_f": {
        "query": lambda T, _: percent(T.c.p_12a14_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_15a17_m": {
        "query": lambda T, _: percent(T.c.p_15a17_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_15a17_f": {
        "query": lambda T, _: percent(T.c.p_15a17_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_18a24_m": {
        "query": lambda T, _: percent(T.c.p_18a24_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_18a24_f": {
        "query": lambda T, _: percent(T.c.p_18a24_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_25a59_m": {
        "query": lambda T, _: percent(T.c.p_25a59_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_25a59_f": {
        "query": lambda T, _: percent(T.c.p_25a59_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_60ymas_m": {
        "query": lambda T, _: percent(T.c.p_60ymas_m, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_p_60ymas_f": {
        "query": lambda T, _: percent(T.c.p_60ymas_f, T.c.pobtot),
        "reduce": "avg",
        "level": "blocks",
    },
    "num_levels": {
        "query": lambda T, _: T.c.num_levels,
        "reduce": "avg",
        "level": "lots",
    },
    "slope": {
        # "query": lambda T, _: T.c.mean_slope,
        "query": lambda T, _: T.c.lot_area,
        "reduce": "avg",
        "level": "lots",
    },
    "per_female_group_ages": {
        "query": lambda T, payload: get_per_group_ages(T, payload.get("group_ages"), "pobfem"),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_male_group_ages": {
        "query": lambda T, payload: get_per_group_ages(T, payload.get("group_ages"), "pobmas"),
        "reduce": "avg",
        "level": "blocks",
    },
    "per_group_ages": {
        "query": lambda T, payload: get_per_group_ages(T, payload.get("group_ages"), "pobtot"),
        "reduce": "avg",
        "level": "blocks",
    },
    # "minutes": {
    #     "query": lambda T, _: func.max(T.c.minutes),
    #     "reduce": "avg",
    #     "level": "blocks",
    # }
}
TABLES = {
    "blocks": {"id": "block_id"},
    "lots": {"id": "lot_id", "relationships": {"block_id": "blocks"}},
    "accessibility_trips": {"id": "id", "relationships": {"node_ids": "blocks"}},
}


class QueryBuilder:
    def __init__(self, session, metadata):
        self.session = session
        self.metadata = metadata
        self.tables = {}

    def get_table(self, table_name, schema="marts"):
        """Retrieve or load a table dynamically."""
        if table_name not in self.tables:
            self.tables[table_name] = Table(
                table_name, self.metadata, autoload_with=self.session.bind, schema=schema)
        return self.tables[table_name]

    def determine_joins(self, required_tables):
        """Determine which tables need to be joined based on required metrics."""
        joins = []
        tables = {name: self.get_table(name) for name in required_tables}

        # Dynamically infer necessary joins based on TABLES relationships
        for table_name, table_info in TABLES.items():
            if table_name in tables and "relationships" in table_info:
                for fk_col, parent_table in table_info["relationships"].items():
                    if parent_table in tables:
                        joins.append(
                            (tables[table_name].c[fk_col], tables[parent_table].c[TABLES[parent_table]["id"]]))

        return tables, joins

    def build_query(self, level: str, metrics: Dict[str, str], coordinates: List[List[float]] = None, filters: List[Any] = None, payload: Dict[str, str] = None):
        """Construct a dynamic SQL query based on the requested level, metrics, and filters."""
        required_tables = {
            METRIC_MAPPING[metric]["level"] for metric in metrics.keys()
        }
        required_tables.add(level)  # Ensure the primary table is included

        tables, joins = self.determine_joins(required_tables)
        base_query = self.session.query()

        # Select ID columns
        base_query = base_query.add_columns(
            tables[level].c[TABLES[level]["id"]].label(level + "_id"))

        if "relationships" in TABLES[level]:
            for fk_col in TABLES[level]["relationships"]:
                base_query = base_query.add_columns(
                    tables[level].c[fk_col].label(fk_col))

        # Process metrics dynamically with correct aggregation
        for metric_name, metric_info in metrics.items():
            func_reduce = getattr(
                func, METRIC_MAPPING[metric_name]["reduce"])
            metric_table = self.tables[METRIC_MAPPING[metric_name]["level"]]
            _metric = METRIC_MAPPING[metric_name]["query"](
                metric_table, payload)

            # If the metric comes from a lower level, aggregate it accordingly
            if METRIC_MAPPING[metric_name]["level"] != level:
                base_query = base_query.add_columns(
                    func.min(_metric).label(metric_info)
                )
            else:
                base_query = base_query.add_columns(
                    func_reduce(_metric).label(metric_info)
                )

        # Apply joins dynamically
        for left, right in joins:
            base_query = base_query.join(
                right.table, left == right, isouter=True)

        # Apply filters
        if filters:
            for condition in filters:
                base_query = base_query.filter(condition)

        # Apply spatial constraints if needed
        table = tables[level]
        base_query = fit_to_boundaries(table, base_query, coordinates)

        # Group by relevant column
        group_by_column = tables[level].c[TABLES[level]["id"]]
        base_query = base_query.group_by(group_by_column)

        return base_query


# def query_metrics(level: str, metrics: Dict[str, str], coordinates: List[List[float]] = None, filters: List[Any] = None, payload: Dict[str, str] = None):
#     """General function to query metrics dynamically for any level."""
#     engine = get_engine()
#     metadata = MetaData()
#     with Session(engine) as session:
#         builder = QueryBuilder(session, metadata)

#         # Ensure filters for specific cases
#         if "minutes" in metrics:
#             filters = []
#             filters.append(
#                 builder.get_table("accessibility_trips").c.num_amenity == 1)

#         query = builder.build_query(
#             level, metrics, coordinates, filters, payload)
#         return pd.read_sql(query.statement, session.bind)


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


@cache
def get_engine():
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "reimaginaurbano")

    connection_string = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    )
    # optional ping to avoid stale connections
    return create_engine(connection_string, pool_pre_ping=True)


async def safe_read_sql(query: str,
                        bind,
                        is_gdf=False) -> pd.DataFrame:
    # if await request.is_disconnected():
    #    print("Cancelled before DB call")
    #    return pd.DataFrame()

    if is_gdf:
        df = await run_in_threadpool(lambda: gpd.read_postgis(query, bind, geom_col="geometry"))
    else:
        df = await run_in_threadpool(lambda: pd.read_sql(query, bind))
    # if await request.is_disconnected():
    #     print("Cancelled after DB call")
    #     return pd.DataFrame()

    return df


# def gdf_query(query: str) -> gpd.GeoDataFrame:
#    engine = get_engine()
#    with Session(engine) as session:
#        gdf = gpd.read_postgis(query, session.bind, geom_col="geometry")
#        return gdf


# def df_query(query: str) -> gpd.GeoDataFrame:
#    engine = get_engine()
#    with Session(engine) as session:
#        df = pd.read_sql(query, session.bind)
#        return df


def get_metrics_info(metrics: List[str]):
    engine = get_engine()
    metadata = MetaData()
    Blocks = Table('blocks', metadata, autoload_with=engine, schema="marts")
    Lots = Table('lots', metadata, autoload_with=engine, schema="marts")
    return [get_metric(metric, Lots, Blocks) for metric in metrics]


async def query_metrics(level: str,
                        metrics: Dict[str, str],
                        coordinates: List[List[float]] = None,
                        payload: Dict[str, str] = None):
    # TODO: Refactor code since it is too unnecessarily complex
    engine = get_engine()
    metadata = MetaData()
    Blocks = Table("blocks", metadata, autoload_with=engine, schema="marts")
    Lots = Table("lots", metadata, autoload_with=engine, schema="marts")

    with Session(engine) as session:
        # Aliases for easy access to both tables
        lots_alias = aliased(Lots)
        blocks_alias = aliased(Blocks)

        # Start with a base query that will be modified based on level and metrics
        base_query = session.query()

        # Determine the selected level
        if level == "blocks":
            base_query = base_query.add_columns(
                blocks_alias.c.block_id.label("block_id"))
            join_condition = lots_alias.c.block_id == blocks_alias.c.block_id
            base_query = base_query.select_from(blocks_alias).join(
                lots_alias, join_condition, isouter=True)

            for metric, new_metric in metrics.items():
                # Check if the metric belongs to Lots or Blocks
                metric_info = get_metric(metric, Lots, Blocks)
                if metric_info["level"] == "lots":
                    func_reduce = getattr(func, metric_info["reduce"])
                    _metric = metric_info["query"](lots_alias, payload)
                    base_query = base_query.add_columns(
                        func_reduce(_metric).label(new_metric))
                elif metric_info["level"] == "blocks":
                    _metric = metric_info["query"](blocks_alias, payload)
                    base_query = base_query.add_columns(
                        func.min(_metric).label(new_metric))
                else:
                    raise ValueError(
                        f"Metric {metric} not found in either Lots or Blocks.")
            # Group by the block to ensure the aggregation happens per block
            base_query = base_query.group_by(blocks_alias.c.block_id)

            base_query = fit_to_boundaries(blocks_alias, base_query,
                                           coordinates)

        elif level == "lots":
            base_query = base_query.add_columns(
                lots_alias.c.lot_id.label("lot_id"))
            base_query = base_query.add_columns(
                lots_alias.c.block_id.label("block_id"))

            for metric, new_metric in metrics.items():
                # Check if the metric belongs to Lots or Blocks
                metric_info = get_metric(metric, Lots, Blocks)
                if metric_info["level"] == "lots":
                    _metric = metric_info["query"](lots_alias, payload)
                    base_query = base_query.add_columns(
                        _metric.label(new_metric))
                elif metric_info["level"] == "blocks":
                    # If we are at the "lots" level, and the metric comes from Blocks, simply return the value from Lots
                    _metric = metric_info["query"](blocks_alias, payload)
                    base_query = base_query.add_columns(
                        _metric.label(new_metric))
                else:
                    raise ValueError(
                        f"Metric {metric} not found in either Lots or Blocks.")

            # Perform join if we need any metrics from Blocks
            if any(
                    get_metric(metric, Lots, Blocks)["level"] == "blocks"
                    for metric in metrics):
                join_condition = lots_alias.c.block_id == blocks_alias.c.block_id
                base_query = base_query.join(blocks_alias,
                                             join_condition,
                                             isouter=True)

            base_query = fit_to_boundaries(lots_alias, base_query, coordinates)
        else:
            raise ValueError(f"Unknown level: {level}")
        print(base_query)

        # df = pd.read_sql(base_query.statement, session.bind)
        df = await safe_read_sql(
            base_query.statement, session.bind)
        return df


async def select_minutes(
    level: str, coordinates: List[List[float]], amenities: List[str]
):
    engine = get_engine()
    metadata = MetaData()
    Blocks = Table('blocks', metadata, autoload_with=engine, schema="marts")
    Blocks = aliased(Blocks)
    Lots = Table('lots', metadata, autoload_with=engine, schema="marts")
    Lots = aliased(Lots)
    AccessibilityTrips = Table(
        'accessibility_trips', metadata, autoload_with=engine, schema="marts")
    with Session(engine) as session:
        query = session.query(
            func.min(AccessibilityTrips.c.amenity).label("amenity"),
            func.max(AccessibilityTrips.c.distance).label("distance"),
            func.max(AccessibilityTrips.c.minutes).label("minutes"),
        )
        column = Blocks.c.block_id if level == "blocks" else Lots.c.lot_id
        query = query.add_columns(
            func.min(Blocks.c.block_id).label("block_id"))
        query = query.select_from(AccessibilityTrips).join(
            Blocks, AccessibilityTrips.c.origin_id == Blocks.c.node_ids, isouter=True)
        if level == "lots":
            query = query.add_columns(func.min(Lots.c.lot_id).label("lot_id"))
            query = query.join(Lots, Blocks.c.block_id ==
                               Lots.c.block_id, isouter=True)

        query = fit_to_boundaries(
            Blocks if level == "blocks" else Lots, query, coordinates)
        query = query.filter(AccessibilityTrips.c.num_amenity == 1)
        if amenities:
            query = query.filter(AccessibilityTrips.c.amenity.in_(amenities))
        query = query.group_by(column)
        df = await safe_read_sql(
            query.statement, session.bind)
        return df


def select_furthest_amenity(level: str, coordinates: List[List[float]], amenities: List[str]):
    engine = get_engine()
    metadata = MetaData()
    Blocks = Table('blocks', metadata, autoload_with=engine, schema="marts")
    Lots = Table('lots', metadata, autoload_with=engine, schema="marts")
    AccessibilityTrips = Table(
        'accessibility_trips', metadata, autoload_with=engine, schema="marts")

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
        table = Blocks if level == "blocks" else Lots

        # Main query
        query = session.query(
            ranked_trips.c.amenity.label("amenity"),
            ranked_trips.c.distance.label("distance"),
            ranked_trips.c.minutes.label("minutes"),
            Blocks.c.block_id.label("block_id")
        )

        # Join with Blocks table
        query = query.join(
            Blocks, ranked_trips.c.origin_id == Blocks.c.node_ids, isouter=True)

        # Additional logic if level is "lots"
        if level == "lots":
            query = query.add_columns(Lots.c.lot_id.label("lot_id"))
            query = query.join(Lots, Blocks.c.block_id ==
                               Lots.c.block_id, isouter=True)

        # Apply filter for specific IDs if provided
        query = fit_to_boundaries(table, query, coordinates)

        # Additional filter for amenities if provided
        if amenities:
            query = query.filter(ranked_trips.c.amenity.in_(amenities))

        # Only keep rows where row number is 1 (furthest `amenity` per origin_id)
        query = query.filter(ranked_trips.c.rn == 1)

        # Execute and return the result as DataFrame
        df = pd.read_sql(query.statement, session.bind)
        return df


async def select_accessibility_score(
    level: str, coordinates: List[List[float]], amenities: List[str]
):
    engine = get_engine()  # Ensure this function is defined to get the engine
    metadata = MetaData()
    Blocks = Table('blocks', metadata, autoload_with=engine, schema="marts")
    Blocks = aliased(Blocks)
    Lots = Table('lots', metadata, autoload_with=engine, schema="marts")
    AccessibilityTrips = Table(
        'accessibility_trips', metadata, autoload_with=engine, schema="marts")

    with Session(engine) as session:
        # Step 1: Precompute Rj values for each destination_id as a subquery
        rj_subquery = (
            select(
                AccessibilityTrips.c.destination_id,
                (func.min(AccessibilityTrips.c.capacity) /
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
        id = "block_id" if level == "blocks" else "lot_id"
        column = Blocks.c.block_id if level == "blocks" else Lots.c.lot_id
        intermediate_query = intermediate_query.add_columns(column.label(id))

        # Join with Blocks, Lots, and the Rj subquery
        intermediate_query = intermediate_query.select_from(AccessibilityTrips).join(
            rj_subquery, AccessibilityTrips.c.destination_id == rj_subquery.c.destination_id, isouter=True
        ).join(
            Blocks, AccessibilityTrips.c.origin_id == Blocks.c.node_ids, isouter=True
        )

        if level == "lots":
            intermediate_query = intermediate_query.join(
                Lots, Blocks.c.block_id == Lots.c.block_id, isouter=True)
            # intermediate_query = intermediate_query.add_columns(func.min(Lots.c.lot_id).label("lot_id"))

        # Apply filters for ids and amenities if provided
        intermediate_query = fit_to_boundaries(
            Blocks if level == "blocks" else Lots, intermediate_query, coordinates)
        # Apply filters for amenities if provided
        if amenities:
            intermediate_query = intermediate_query.filter(
                AccessibilityTrips.c.amenity.in_(amenities))

        # Group the intermediate result by level, amenity, and origin_id
        intermediate_query = intermediate_query.group_by(
            column, AccessibilityTrips.c.amenity, AccessibilityTrips.c.origin_id)

        # Step 3: Execute the intermediate query and store it as a DataFrame
        intermediate_df = await safe_read_sql(
            intermediate_query.statement, session.bind)

        # Step 4: Aggregate the final accessibility score by origin_id only
        final_df = intermediate_df.groupby(id, as_index=False).agg({
            "accessibility_score": "sum"
        })
        if amenities:
            final_df["accessibility_score"] = (
                final_df["accessibility_score"] / len(amenities))
        else:
            final_df["accessibility_score"] = (
                final_df["accessibility_score"] / len(intermediate_df["amenity"].unique()))

        return final_df


# def execute_query(query: str, geometry: bool = False):
#     engine = get_engine()
#     with Session(engine) as session:
#         df = pd.read_sql(query, session.bind)
#         if geometry:
#             gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
#             return gdf
#         return df


async def get_geometry(layer: str, coordinates: List[List[float]]):
    engine = get_engine()
    metadata = MetaData()

    with Session(engine) as session:
        if layer == "blocks":
            table = Table("blocks",
                          metadata,
                          Column("block_id", Text),
                          Column("geometry", Geometry("POLYGON", srid=4326)),
                          schema="marts")
            query = select(table.c.block_id, table.c.geometry)
        elif layer == "lots":
            table = Table("lots",
                          metadata,
                          Column("lot_id", Text),
                          Column("block_id", Text),
                          Column("geometry", Geometry("POLYGON", srid=4326)),
                          schema="marts")
            query = select(table.c.lot_id, table.c.geometry)
        elif layer == "amenities" or layer == "accessibility_points":
            print("Fetching amenities")
            table = Table("amenities",
                          metadata,
                          Column("id", Text),
                          Column("amenity", Text),
                          Column("name", Text),
                          Column("capacity", Text),
                          Column("control", Text),
                          Column("source", Text),
                          Column("num_visits", Text),
                          Column("visits_category", Text),
                          Column("extra_data", JSON),
                          Column("geometry", Geometry("POLYGON", srid=4326)),
                          schema="marts")
            query = select(
                table.c.id, table.c.name, table.c.amenity, table.c.capacity,
                table.c.control, table.c.source, table.c.num_visits,
                table.c.visits_category, table.c.extra_data, table.c.geometry,
                func.ST_Area(func.ST_Transform(table.c.geometry,
                                               3857)).label('area'))

        else:
            raise ValueError(f"Unknown layer: {layer}")

    query = fit_to_boundaries(table, query, coordinates)

    #    gdf = gpd.read_postgis(query, session.bind, geom_col="geometry")
    gdf = await safe_read_sql(
        query, session.bind, is_gdf=True)
    return gdf


def fit_to_boundaries(table, query, coordinates):
    if coordinates:
        polygons_wkt = [
            f"ST_GeomFromText('POLYGON(({', '.join([f'{x[0]} {x[1]}' for x in coord])}))', 4326)"
            for coord in coordinates
        ]
        subquery = text(f"""
            (SELECT ST_Union(ARRAY[{', '.join(polygons_wkt)}]) AS geom)
        """)
        query = query.where(
            func.ST_Intersects(table.c.geometry, subquery)
        )

    return query
