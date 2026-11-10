INSERT INTO segments (code, kind, name)
SELECT 'S' || n, 'block_section', 'Block section ' || n
FROM generate_series(1, 10) AS n
ON CONFLICT DO NOTHING;

INSERT INTO segments (code, kind, name) VALUES
    ('L1', 'loop_line', 'Loop line 1'),
    ('L2', 'loop_line', 'Loop line 2')
ON CONFLICT DO NOTHING;

INSERT INTO term_fence (id, highest_term) VALUES (true, 0)
ON CONFLICT DO NOTHING;