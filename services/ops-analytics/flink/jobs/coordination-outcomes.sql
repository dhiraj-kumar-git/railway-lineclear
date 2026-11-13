-- Per minute: elections held, time spent without a duty controller, and diversions
-- committed or aborted
INSERT INTO ops_results
SELECT
  CONCAT('coordination|all|', DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss''Z''')),
  'coordination',
  'all',
  DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
  DATE_FORMAT(window_end, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  CAST(SUM(CASE WHEN event_type = 'leader_elected' THEN 1 ELSE 0 END) AS BIGINT),
  CAST(SUM(CASE WHEN event_type = 'leader_elected' THEN COALESCE(leaderless_ms, 0) ELSE 0 END) AS BIGINT),
  CAST(SUM(CASE WHEN event_type = 'transaction_decided' AND decision = 'COMMIT' THEN 1 ELSE 0 END) AS BIGINT),
  CAST(SUM(CASE WHEN event_type = 'transaction_decided' AND decision = 'ABORT' THEN 1 ELSE 0 END) AS BIGINT),
  CAST(NULL AS STRING),
  CAST(NULL AS STRING)
FROM TABLE(TUMBLE(TABLE coordination_events, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
WHERE event_type IN ('leader_elected', 'transaction_decided')
GROUP BY window_start, window_end;
