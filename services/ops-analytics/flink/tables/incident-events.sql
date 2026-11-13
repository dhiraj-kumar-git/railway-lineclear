-- Incident events from Incident Manager. Each event is sent the moment it happens, so the
-- Kafka record's own timestamp is the event time.
CREATE TABLE incident_events (
  event_type  STRING,
  incident_id STRING,
  kind        STRING,
  section     STRING,
  event_time  TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'incident.events',
  'properties.bootstrap.servers' = '@KAFKA_BOOTSTRAP_SERVERS@',
  'properties.security.protocol' = '@KAFKA_SECURITY_PROTOCOL@',
  'properties.group.id' = 'ops-analytics',
  'scan.startup.mode' = 'group-offsets',
  'properties.auto.offset.reset' = 'earliest',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);
