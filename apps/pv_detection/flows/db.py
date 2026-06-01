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


def load_predictions(
    engine: sa.Engine,
    df: pd.DataFrame,
    schema: str = "raw",
    table: str = "pv_predictions",
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
    table: str = "pv_predictions",
) -> set[str]:
    query = sa.text(f"SELECT DISTINCT building_id FROM {schema}.{table}")
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn)
        return set(df["building_id"].tolist())
    except Exception:
        return set()


def truncate_predictions(
    engine: sa.Engine,
    schema: str = "raw",
    table: str = "pv_predictions",
):
    ensure_schema(engine, schema)
    with engine.begin() as conn:
        conn.execute(sa.text(f"TRUNCATE TABLE {schema}.{table}"))
