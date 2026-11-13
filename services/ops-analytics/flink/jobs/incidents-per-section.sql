-- Incidents reported per block section in five-minute windows
INSERT INTO ops_results
SELECT
  CONCAT('section_incidents|', section, '|', DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss''Z''')),
  'section_incidents',
  section,
  DATE_FORMAT(window_start, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
  DATE_FORMAT(window_end, 'yyyy-MM-dd''T''HH:mm:ss''Z'''),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  COUNT(*),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  CAST(NULL AS BIGINT),
  CAST(NULL AS STRING),
  CAST(NULL AS STRING)
FROM TABLE(TUMBLE(TABLE incident_events, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
WHERE event_type = 'incident_reported'
GROUP BY section, window_start, window_end;
