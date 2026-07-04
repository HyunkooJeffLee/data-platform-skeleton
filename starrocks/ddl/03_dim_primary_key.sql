-- Device dimension — PRIMARY KEY model.
-- Why Primary Key: device metadata is updated by CDC (name/site/coordinates change).
-- The Primary Key model supports real upserts and deletes, so the latest row per
-- device_id wins without reindexing — unlike append-only or aggregate models.

CREATE TABLE IF NOT EXISTS example_db.dim_device (
    device_id    BIGINT        NOT NULL  COMMENT "primary key; also the join/bucket key",
    device_name  VARCHAR(128)  NULL,
    site_code    VARCHAR(32)   NULL      COMMENT "grouping attribute used by dashboards",
    latitude     DOUBLE        NULL,
    longitude    DOUBLE        NULL,
    is_active    BOOLEAN       NULL,
    updated_at   DATETIME      NULL      COMMENT "source change timestamp"
)
ENGINE = OLAP
PRIMARY KEY(device_id)
-- Bucket count + key MUST match raw_sensor_readings to share the colocation group,
-- so fact<->dim joins on device_id run node-local (no shuffle) at query time.
DISTRIBUTED BY HASH(device_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "1",
    "colocate_with" = "cg_sensor_device",
    -- Persist the primary-key index for large dims; drop for small ones to save memory.
    "enable_persistent_index" = "true"
);

-- Upsert pattern: Stream Load / Routine Load into a Primary Key table is upsert by
-- default. To also propagate deletes, add the __op column (0=upsert, 1=delete) via
-- the loader's columns/merge settings.
