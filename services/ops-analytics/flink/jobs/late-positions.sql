-- Reports that arrive after their minute's window has closed are left out of the count
-- above. They are written here instead, one record each, so none is lost silently.
INSERT INTO ops_results
SELECT
  CONCAT('late_position|', train_number, '|', CAST(recorded_at_ms AS STRING)),
  'late_position',
  section,
  DATE_FORMAT(FLOOR(CAST(event_time AS TIMESTAMP(3)) TO MINUTE), 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
  DATE_FORMAT(FLOOR(CAST(event_time AS TIMESTAMP(3)) TO MINUTE) + INTERVAL '1' MINUTE, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  train_number,
  DATE_FORMAT(CAST(event_time AS TIMESTAMP(3)), 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z''')
FROM train_positions
WHERE CURRENT_WATERMARK(event_time) IS NOT NULL
  AND CAST(CURRENT_WATERMARK(event_time) AS TIMESTAMP(3))
      >= FLOOR(CAST(event_time AS TIMESTAMP(3)) TO MINUTE) + INTERVAL '1' MINUTE - INTERVAL '0.001' SECOND;
