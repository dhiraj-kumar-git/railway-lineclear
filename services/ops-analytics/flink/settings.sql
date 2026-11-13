-- Session settings for the operations analytics job
SET 'pipeline.name' = 'ops-analytics';
-- One task per operator: the volumes are small, and the late-report job then sees the
-- same watermark that closes the windows
SET 'parallelism.default' = '1';
-- Window boundaries and the times written to ops.results are in UTC
SET 'table.local-time-zone' = 'UTC';
-- A Kafka partition with no messages for 10 seconds stops holding the watermark back
SET 'table.exec.source.idle-timeout' = '10 s';
-- Save the job's state every 30 seconds, so a restart resumes instead of starting over
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
