import logging
from typing import Any

import pandas as pd
import sqlalchemy as sa

logger = logging.getLogger(__name__)


def get_engine(
    host: str = "localhost",
    port: str | int = "15432",
    user: str = "postgres",
    password: str = "securepassword123",
    dbname: str = "datasets",
) -> sa.Engine:
    return sa.create_engine(
        f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    )


def get_engine_from_config(cfg: Any) -> sa.Engine:
    return get_engine(
        host=cfg.postgres_host,
        port=cfg.postgres_port,
        user=cfg.postgres_user,
        password=cfg.postgres_password,
        dbname=cfg.postgres_db,
    )


def ensure_schema(engine: sa.Engine, schema: str):
    with engine.begin() as conn:
        conn.execute(sa.text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))


def ensure_table(
    engine: sa.Engine,
    schema: str = "raw",
    table: str = "pv_roi_estimates",
):
    ensure_schema(engine, schema)
    with engine.begin() as conn:
        conn.execute(sa.text(f"""
            CREATE TABLE IF NOT EXISTS {schema}.{table} (
                building_id TEXT NOT NULL,
                kwp FLOAT,
                capex FLOAT,
                annual_production_kwh FLOAT,
                annual_consumption_kwh FLOAT,
                user_type TEXT,
                regime TEXT,
                npv FLOAT,
                irr FLOAT,
                payback_simple FLOAT,
                payback_discounted FLOAT,
                tasso_autoconsumo FLOAT,
                _sdc_extracted_at TIMESTAMP
            )
        """))


def load_estimates(
    engine: sa.Engine,
    df: pd.DataFrame,
    schema: str = "raw",
    table: str = "pv_roi_estimates",
) -> int:
    df = df.copy()
    df["_sdc_extracted_at"] = pd.Timestamp.now()

    ensure_schema(engine, schema)
    with engine.begin() as conn:
        df.to_sql(table, conn, schema=schema, if_exists="append", index=False)
    return len(df)


def already_processed(
    engine: sa.Engine,
    schema: str = "raw",
    table: str = "pv_roi_estimates",
) -> set[str]:
    query = sa.text(f"SELECT DISTINCT building_id FROM {schema}.{table}")
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return set(df["building_id"].tolist())
    except Exception:
        return set()


def truncate_estimates(
    engine: sa.Engine,
    schema: str = "raw",
    table: str = "pv_roi_estimates",
):
    ensure_schema(engine, schema)
    with engine.begin() as conn:
        conn.execute(sa.text(f"TRUNCATE TABLE {schema}.{table}"))


def load_eligible_buildings(
    engine: sa.Engine,
    suitability_schema: str,
    suitability_table: str,
    detections_schema: str,
    detections_table: str,
    require_detection: bool = True,
) -> pd.DataFrame:
    select_cols = """
            s.building_id,
            ST_X(ST_Centroid(s.geometry)) as lon,
            ST_Y(ST_Centroid(s.geometry)) as lat,
            s.footprint_area_m2,
            s.building_class,
            s.building_subtype,
            s.height,
            s.num_floors"""

    if require_detection:
        sql = f"""
        SELECT {select_cols}
        FROM {suitability_schema}.{suitability_table} s
        JOIN {detections_schema}.{detections_table} d
            ON d.building_id = s.building_id
        WHERE d.has_pv = false
        ORDER BY s.building_id
        """
    else:
        sql = f"""
        SELECT {select_cols}
        FROM {suitability_schema}.{suitability_table} s
        ORDER BY s.building_id
        """

    try:
        with engine.connect() as conn:
            return pd.read_sql(sa.text(sql), conn)
    except Exception as e:
        logger.warning("Could not load buildings (tables may not exist yet): %s", e)
        return pd.DataFrame()
