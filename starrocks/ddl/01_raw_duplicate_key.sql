-- Raw, append-only telemetry — DUPLICATE KEY model.
-- Why Duplicate Key: raw ingest keeps every row (no upsert, no pre-aggregation).
-- The key columns define the SORT order (prefix index), not uniqueness.
-- Assumes StarRocks 3.1+ for expression partitioning; on older versions use an
-- explicit RANGE(event_time) with dynamic partitioning instead.

CREATE TABLE IF NOT EXISTS example_db.raw_sensor_readings (
    event_time   DATETIME     NOT NULL  COMMENT "event timestamp (sort-key prefix -> time pruning)",
    device_id    BIGINT       NOT NULL  COMMENT "join key to dim_device; also bucket key",
    metric       VARCHAR(64)  NOT NULL  COMMENT "e.g. power_kw, irradiance, temp_c",
    value        DOUBLE       NULL      COMMENT "reading value; nullable to tolerate sensor gaps",
    ingested_at  DATETIME     NULL      COMMENT "load-time audit column"
)
ENGINE = OLAP
-- Sort key. Filters on the prefix (event_time, then device_id) prune via the prefix index.
DUPLICATE KEY(event_time, device_id, metric)
-- One partition per day => partition pruning on time-ranged queries; cheap to drop/backfill.
PARTITION BY date_trunc('day', event_time)
-- Bucket on the join key so this fact can COLOCATE with dim_device (same key, same bucket count).
DISTRIBUTED BY HASH(device_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "1",              -- single-replica for local/dev; use 3 in prod
    "colocate_with" = "cg_sensor_device", -- colocation group shared with dim_device
    -- Auto-drop partitions older than 90 days so raw storage stays bounded.
    "partition_live_number" = "90"
);
