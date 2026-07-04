"""Watermark store for incremental DAGs.

A watermark is the last successfully loaded boundary value (here: max event_time).
Two interchangeable backends are provided; the get/set signature is identical so a
DAG can switch backends without touching its logic.

- Airflow Variable  : simplest, good for single-writer scheduling. Default.
- PostgreSQL row    : audited, survives Variable purges, safer for multi-writer.
                      Requires this metadata table on the source/meta DB:
                          CREATE TABLE etl_watermark (
                              job_key    TEXT PRIMARY KEY,
                              watermark  TEXT NOT NULL,
                              updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
                          );
"""
from __future__ import annotations


# --- Airflow Variable backend (default) --------------------------------------
def get_watermark(key: str, default: str | None = None) -> str | None:
    from airflow.models import Variable

    # default_var makes the first run (no Variable yet) start from a floor, not crash.
    return Variable.get(key, default_var=default)


def set_watermark(key: str, value: str) -> None:
    from airflow.models import Variable

    Variable.set(key, value)


# --- PostgreSQL backend (audited alternative) --------------------------------
def get_watermark_pg(key: str, conn_id: str = "postgres_meta", default: str | None = None) -> str | None:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    row = PostgresHook(postgres_conn_id=conn_id).get_first(
        "SELECT watermark FROM etl_watermark WHERE job_key = %s",
        parameters=(key,),
    )
    return row[0] if row else default


def set_watermark_pg(key: str, value: str, conn_id: str = "postgres_meta") -> None:
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    # Upsert keeps exactly one row per job and makes re-runs idempotent.
    PostgresHook(postgres_conn_id=conn_id).run(
        """
        INSERT INTO etl_watermark (job_key, watermark, updated_at)
        VALUES (%s, %s, now())
        ON CONFLICT (job_key) DO UPDATE
            SET watermark = EXCLUDED.watermark, updated_at = now()
        """,
        parameters=(key, value),
    )
