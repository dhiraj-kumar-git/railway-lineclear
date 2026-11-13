-- Every result the job produces, one JSON message per window and section on ops.results.
-- result_key is also the Kafka key and the reader's DynamoDB key, so a result written
-- twice after a restart replaces itself instead of counting twice.
CREATE TABLE ops_results (
  result_key    STRING,
  metric        STRING,
  section       STRING,
  window_start  STRING,
  window_end    STRING,
  trains        BIGINT,
  updates       BIGINT,
  incidents     BIGINT,
  elections     BIGINT,
  leaderless_ms BIGINT,
  commits       BIGINT,
  aborts        BIGINT,
  train_number  STRING,
  recorded_at   STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'ops.results',
  'properties.bootstrap.servers' = '@KAFKA_BOOTSTRAP_SERVERS@',
  'properties.security.protocol' = '@KAFKA_SECURITY_PROTOCOL@',
  'key.format' = 'raw',
  'key.fields' = 'result_key',
  'value.format' = 'json',
  'value.fields-include' = 'ALL',
  'sink.delivery-guarantee' = 'at-least-once'
);
