"""Partition-scoped backfill: re-load one day partition per run, never a full refresh.

`catchup=True` lets the scheduler materialize one run per daily interval across the
bounded window; each run rewrites ONLY its own partition. This keeps backfills
restartable and cheap: re-running a single day touches a single partition.

Idempotency per partition = truncate-that-partition then reload-that-partition.
Atomic alternative (brief-empty-window free): load into a TEMPORARY PARTITION then
`ALTER TABLE ... REPLACE PARTITION`. Kept the simpler form here; the temp-partition
swap is the production upgrade when readers must never see a half-empty day.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

SOURCE_CONN_ID = "postgres_source"
STARROCKS_FE_CONN_ID = "starrocks_fe"      # HTTP Stream Load endpoint
STARROCKS_MYSQL_CONN_ID = "starrocks_mysql"  # MySQL wire protocol (FE :9030) for DDL/DML
STARROCKS_DB = "example_db"
STARROCKS_TABLE = "raw_sensor_readings"


@dag(
    dag_id="backfill_partitioned",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2024, 2, 1, tz="UTC"),  # bounded backfill window
    catchup=True,                # each interval = one day partition; scheduler fills the range
    max_active_runs=3,           # cap parallel partitions so source/warehouse aren't swamped
    default_args={"retries": 1},
    tags=["backfill", "starrocks", "partitioned"],
)
def backfill_partitioned():
    @task
    def resolve_partition(data_interval_start=None) -> dict:
        # TaskFlow injects context keys by matching parameter name.
        day = data_interval_start.in_timezone("UTC")
        return {
            "partition": f"p{day.format('YYYYMMDD')}",           # matches daily expression-partition naming
            "day_start": day.format("YYYY-MM-DD 00:00:00"),
            "day_end": day.add(days=1).format("YYYY-MM-DD 00:00:00"),
        }

    @task
    def clear_partition(part: dict) -> dict:
        from airflow.providers.mysql.hooks.mysql import MySqlHook

        # StarRocks speaks the MySQL wire protocol on the FE (:9030).
        # Partition-scoped truncate: only this day is cleared, never the whole table.
        hook = MySqlHook(mysql_conn_id=STARROCKS_MYSQL_CONN_ID)
        hook.run(f"TRUNCATE TABLE {STARROCKS_DB}.{STARROCKS_TABLE} PARTITION ({part['partition']})")
        return part

    @task
    def reload_partition(part: dict) -> int:
        import json
        import re

        import requests
        from airflow.hooks.base import BaseHook
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        # Read exactly this day's rows from the source (half-open interval).
        rows = PostgresHook(postgres_conn_id=SOURCE_CONN_ID).get_records(
            """
            SELECT event_time, device_id, metric, value
            FROM sensor_readings
            WHERE event_time >= %s AND event_time < %s
            ORDER BY event_time
            """,
            parameters=(part["day_start"], part["day_end"]),
        )
        if not rows:
            return 0

        records = [
            {"event_time": str(r[0]), "device_id": r[1], "metric": r[2], "value": r[3]}
            for r in rows
        ]
        label = re.sub(r"[^A-Za-z0-9_-]", "_", f"backfill_{STARROCKS_TABLE}_{part['partition']}")[:128]

        conn = BaseHook.get_connection(STARROCKS_FE_CONN_ID)
        url = f"http://{conn.host}:{conn.port or 8030}/api/{STARROCKS_DB}/{STARROCKS_TABLE}/_stream_load"
        headers = {
            "label": label,
            "format": "json",
            "strip_outer_array": "true",
            "partitions": part["partition"],  # write only into the target partition
            "Expect": "100-continue",
        }
        payload = json.dumps(records).encode("utf-8")

        session = requests.Session()
        session.auth = (conn.login, conn.password)
        resp = session.put(url, data=payload, headers=headers, allow_redirects=False, timeout=600)
        if resp.status_code in (307, 308):  # follow FE->BE redirect, re-sending auth
            resp = session.put(resp.headers["Location"], data=payload, headers=headers, timeout=600)

        body = resp.json()
        if body.get("Status") not in ("Success", "Publish Timeout"):
            raise RuntimeError(f"Backfill Stream Load failed for {part['partition']}: {body}")
        return len(records)

    part = resolve_partition()
    reload_partition(clear_partition(part))


backfill_partitioned()
