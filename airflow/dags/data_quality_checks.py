"""Data-quality gates on StarRocks: fail the run, block downstream.

Three gates run in parallel (row-count, null-ratio, freshness). Any violation
RAISES, which fails that task; with the default all_success trigger rule the
downstream `publish` (e.g. a serving swap / mart refresh) never runs. This is a
gate, not a warning: bad data pages someone instead of flowing through silently.

Note the deliberate choice of FAIL over SKIP: `@task.short_circuit` would skip
downstream quietly. For a quality gate we want a red run and an alert.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from include.dq import assert_fresh, assert_null_ratio_below, assert_row_count_at_least

STARROCKS_MYSQL_CONN_ID = "starrocks_mysql"
TABLE = "example_db.raw_sensor_readings"
# Gate today's slice; keeps the check cheap and relevant to the freshest partition.
TODAY_PREDICATE = "event_time >= current_date()"


def _scalar(sql: str):
    """sql -> single scalar value, via StarRocks (MySQL wire protocol)."""
    from airflow.providers.mysql.hooks.mysql import MySqlHook

    row = MySqlHook(mysql_conn_id=STARROCKS_MYSQL_CONN_ID).get_first(sql)
    return row[0] if row else None


@dag(
    dag_id="data_quality_checks",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args={"retries": 0},  # a DQ failure is a real signal, not a flake to retry away
    tags=["data-quality", "starrocks", "gate"],
)
def data_quality_checks():
    @task
    def check_row_count() -> int:
        # At least some rows must have landed for today.
        return assert_row_count_at_least(_scalar, TABLE, min_rows=1, where=TODAY_PREDICATE)

    @task
    def check_null_ratio() -> float:
        # `value` going mostly-NULL usually means an upstream schema/parse break.
        return assert_null_ratio_below(_scalar, TABLE, column="value", max_ratio=0.05, where=TODAY_PREDICATE)

    @task
    def check_freshness() -> int:
        # Newest row must be within 60 minutes, else ingest is stalled.
        return assert_fresh(_scalar, TABLE, ts_column="event_time", max_lag_minutes=60)

    @task
    def publish() -> None:
        # Runs only if all three gates passed (all_success). Placeholder for the
        # real downstream action: mart refresh, view swap, or MV refresh trigger.
        pass

    gates = [check_row_count(), check_null_ratio(), check_freshness()]
    gates >> publish()


data_quality_checks()
