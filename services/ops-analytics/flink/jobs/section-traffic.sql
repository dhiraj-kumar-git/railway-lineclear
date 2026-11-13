-- Distinct trains and position updates per block section per minute
INSERT INTO ops_results
SELECT
  CONCAT('section_traffic|', section, '|', DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss''Z''')),
  'section_traffic',
  section,
  DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
  DATE_FORMAT(window_end, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
  COUNT(DISTINCT train_number),
  COUNT(*),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  CAST(NULL AS STRING),
  CAST(NULL AS STRING)
FROM TABLE(TUMBLE(TABLE train_positions, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY section, window_start, window_end;
