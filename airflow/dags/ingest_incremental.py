"""Incremental ingest: PostgreSQL source -> StarRocks raw table.

Airflow 2.10, TaskFlow API. Idempotent by construction:
  - high-watermark (max loaded event_time) bounds each batch, and
  - a content-scoped Stream Load label makes a retried batch a no-op (StarRocks
    rejects duplicate labels), so a crash between load and watermark-advance
    self-heals on the next run.

Airflow 3.x caveat: `@dag`/`@task` and `schedule=`/`catchup=` are unchanged.
Only `airflow.datasets.Dataset` (unused here) was renamed to `airflow.sdk.Asset`.
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task

from include.watermark import get_watermark, set_watermark

SOURCE_CONN_ID = "postgres_source"   # Airflow Connection: the operational Postgres
STARROCKS_CONN_ID = "starrocks_fe"   # Airflow Connection: StarRocks FE (host/http-port/login/password)
STARROCKS_DB = "example_db"
STARROCKS_TABLE = "raw_sensor_readings"
WATERMARK_KEY = "ingest_incremental.raw_sensor_readings"
BATCH_LIMIT = 200_000                # bound the batch; XCom carries only the batch here for clarity


@dag(
    dag_id="ingest_incremental",
    schedule="*/15 * * * *",         # every 15 min; `schedule=` replaced schedule_interval
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,                   # incremental relies on the watermark, not on backfilled intervals
    max_active_runs=1,               # serialize runs so the watermark advances monotonically
    default_args={"retries": 2, "retry_delay": pendulum.duration(minutes=5)},
    tags=["ingest", "starrocks", "incremental"],
)
def ingest_incremental():
    @task
    def read_watermark() -> str:
        # First ever run has no watermark -> start from epoch floor.
        return get_watermark(WATERMARK_KEY, default="1970-01-01 00:00:00")

    @task
    def extract(low_watermark: str) -> list[dict]:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id=SOURCE_CONN_ID)
        # Strict `>` avoids re-reading the boundary row already loaded last run.
        rows = hook.get_records(
            """
            SELECT event_time, device_id, metric, value
            FROM sensor_readings
            WHERE event_time > %s
            ORDER BY event_time
            LIMIT %s
            """,
            parameters=(low_watermark, BATCH_LIMIT),
        )
        return [
            {"event_time": str(r[0]), "device_id": r[1], "metric": r[2], "value": r[3]}
            for r in rows
        ]

    @task
    def load(records: list[dict], low_watermark: str) -> str | None:
        if not records:
            return None  # nothing new; leave the watermark where it is

        import json
        import re

        import requests
        from airflow.hooks.base import BaseHook

        high_watermark = max(r["event_time"] for r in records)
        # Deterministic, content-scoped label => a retry of the same window dedups.
        raw_label = f"{WATERMARK_KEY}_{low_watermark}_{high_watermark}"
        label = re.sub(r"[^A-Za-z0-9_-]", "_", raw_label)[:128]

        conn = BaseHook.get_connection(STARROCKS_CONN_ID)  # no hardcoded creds
        base_url = f"http://{conn.host}:{conn.port or 8030}"
        url = f"{base_url}/api/{STARROCKS_DB}/{STARROCKS_TABLE}/_stream_load"
        headers = {
            "label": label,
            "format": "json",
            "strip_outer_array": "true",
            "Expect": "100-continue",
        }
        payload = json.dumps(records).encode("utf-8")

        # FE answers Stream Load with a 307 to a BE. `requests` drops auth across
        # hosts on redirect (like curl without --location-trusted), so follow it
        # manually and re-send credentials — this is the classic Stream Load gotcha.
        session = requests.Session()
        session.auth = (conn.login, conn.password)
        resp = session.put(url, data=payload, headers=headers, allow_redirects=False, timeout=300)
        if resp.status_code in (307, 308):
            resp = session.put(resp.headers["Location"], data=payload, headers=headers, timeout=300)

        body = resp.json()
        status = body.get("Status", "")
        if status in ("Success", "Publish Timeout"):
            return high_watermark
        if "Label Already Exists" in body.get("Message", ""):
            # Same batch already committed on a prior attempt — safe to advance.
            return high_watermark
        raise RuntimeError(f"Stream Load failed: {body}")

    @task
    def advance_watermark(new_watermark: str | None) -> None:
        if new_watermark:  # only advance when a batch actually loaded
            set_watermark(WATERMARK_KEY, new_watermark)

    low = read_watermark()
    records = extract(low)
    high = load(records, low)
    advance_watermark(high)


ingest_incremental()
