-- S1 to S5 are relieved by loop line L1, S6 to S10 by L2
INSERT INTO loop_lines (blocked_section, loop_line)
SELECT 'S' || n, CASE WHEN n <= 5 THEN 'L1' ELSE 'L2' END
FROM generate_series(1, 10) AS n
ON CONFLICT DO NOTHING;

-- The fault switch starts unarmed
INSERT INTO fault_switch (id) VALUES (true)
ON CONFLICT DO NOTHING;
