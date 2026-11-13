-- Election and diversion events: leader_elected from Leader Election and
-- transaction_decided from Schedule Manager. Event time is the Kafka record's timestamp.
CREATE TABLE coordination_events (
  event_type    STRING,
  leaderless_ms BIGINT,
  decision      STRING,
  event_time    TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'coordination.events',
  'properties.bootstrap.servers' = '@KAFKA_BOOTSTRAP_SERVERS@',
  'properties.security.protocol' = '@KAFKA_SECURITY_PROTOCOL@',
  'properties.group.id' = 'ops-analytics',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);
