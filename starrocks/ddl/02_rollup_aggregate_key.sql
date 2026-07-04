-- Hourly rollup — AGGREGATE KEY model.
-- Why Aggregate Key: dashboards read per-hour/per-device aggregates far more often
-- than raw rows. Pre-aggregating at load time shrinks scanned rows at query time.
-- Key columns are the GROUP-BY dimensions; value columns carry an aggregate function
-- (SUM/MAX/MIN/...) that StarRocks applies automatically as rows merge.

CREATE TABLE IF NOT EXISTS example_db.agg_sensor_hourly (
    event_hour   DATETIME     NOT NULL  COMMENT "truncated to the hour (dimension)",
    device_id    BIGINT       NOT NULL  COMMENT "dimension + bucket key",
    metric       VARCHAR(64)  NOT NULL  COMMENT "dimension",
    reading_cnt  BIGINT       SUM       COMMENT "number of raw readings in the hour",
    value_sum    DOUBLE       SUM       COMMENT "sum -> derive AVG as value_sum/reading_cnt",
    value_max    DOUBLE       MAX       COMMENT "hourly peak",
    value_min    DOUBLE       MIN       COMMENT "hourly trough"
)
ENGINE = OLAP
-- Aggregate key = the grouping dimensions; rows with the same key merge via the funcs above.
AGGREGATE KEY(event_hour, device_id, metric)
PARTITION BY date_trunc('day', event_hour)
DISTRIBUTED BY HASH(device_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "1"
);

-- Loading pattern: INSERT the hour-truncated projection of the raw table; the
-- aggregate model folds duplicates automatically, so repeated loads of the same
-- hour stay correct (SUM/MAX/MIN are order-independent on the merged key).
--   INSERT INTO example_db.agg_sensor_hourly
--   SELECT date_trunc('hour', event_time) AS event_hour, device_id, metric,
--          count(*), sum(value), max(value), min(value)
--   FROM   example_db.raw_sensor_readings
--   WHERE  event_time >= '2024-01-01' AND event_time < '2024-01-02'
--   GROUP  BY 1, 2, 3;
