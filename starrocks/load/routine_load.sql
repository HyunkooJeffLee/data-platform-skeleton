-- Routine Load: StarRocks continuously consumes JSON telemetry from Kafka into the
-- raw table. Unlike Stream Load (client push), Routine Load is a server-side job
-- that StarRocks schedules, tracks offsets for, and resumes automatically.

CREATE ROUTINE LOAD example_db.rl_sensor_readings ON raw_sensor_readings
COLUMNS(event_time, device_id, metric, value)
PROPERTIES (
    "desired_concurrent_number" = "3",   -- parallel consuming tasks (bounded by partitions)
    "max_batch_interval" = "10",         -- seconds per batch commit (latency vs. throughput)
    "max_batch_rows" = "300000",
    "max_error_number" = "100",          -- tolerate a few malformed rows before pausing
    "strict_mode" = "false",
    "format" = "json",
    "jsonpaths" = "[\"$.event_time\",\"$.device_id\",\"$.metric\",\"$.value\"]"
)
FROM KAFKA (
    "kafka_broker_list" = "localhost:9092",
    "kafka_topic" = "sensor_readings",
    -- Start from the earliest offset on first run; StarRocks persists offsets after.
    "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

-- Operate the job:
--   SHOW ROUTINE LOAD FOR example_db.rl_sensor_readings;   -- state, lag, error link
--   PAUSE  ROUTINE LOAD FOR example_db.rl_sensor_readings;
--   RESUME ROUTINE LOAD FOR example_db.rl_sensor_readings;
--   STOP   ROUTINE LOAD FOR example_db.rl_sensor_readings; -- irreversible
