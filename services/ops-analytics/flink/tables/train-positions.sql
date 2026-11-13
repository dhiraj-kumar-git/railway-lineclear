-- Position reports from Position Ingestion. Windows use recorded_at_ms, the time the
-- train took the reading, not the time the message reached Kafka.
CREATE TABLE train_positions (
  train_number   STRING,
  section        STRING,
  recorded_at_ms BIGINT,
  -- A report without a time is treated as very old, so it shows up as late instead of stopping the job
  event_time AS TO_TIMESTAMP_LTZ(COALESCE(recorded_at_ms, 0), 3),
  -- Wait 10 seconds for stragglers before a window closes
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'train.positions',
  'properties.bootstrap.servers' = '@KAFKA_BOOTSTRAP_SERVERS@',
  'properties.security.protocol' = '@KAFKA_SECURITY_PROTOCOL@',
  'properties.group.id' = 'ops-analytics',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);
