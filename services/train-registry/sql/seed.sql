INSERT INTO stations (code, name) VALUES
    ('MMCT', 'Mumbai Central'),
    ('BVI',  'Borivali'),
    ('ST',   'Surat'),
    ('BRC',  'Vadodara Junction'),
    ('RTM',  'Ratlam Junction'),
    ('KOTA', 'Kota Junction'),
    ('NDLS', 'New Delhi'),
    ('MTJ',  'Mathura Junction'),
    ('AGC',  'Agra Cantt'),
    ('GWL',  'Gwalior Junction'),
    ('VGLJ', 'Virangana Lakshmibai Jhansi Junction'),
    ('RKMP', 'Rani Kamalapati'),
    ('SBC',  'KSR Bengaluru'),
    ('SC',   'Secunderabad Junction'),
    ('NGP',  'Nagpur Junction'),
    ('BPL',  'Bhopal Junction'),
    ('NZM',  'Hazrat Nizamuddin'),
    ('BZA',  'Vijayawada Junction'),
    ('ERS',  'Ernakulam Junction'),
    ('TVC',  'Thiruvananthapuram Central')
ON CONFLICT DO NOTHING;

INSERT INTO trains (number, name, category, origin, destination) VALUES
    ('12951', 'Mumbai Rajdhani Express',  'Rajdhani',  'MMCT', 'NDLS'),
    ('12002', 'Bhopal Shatabdi Express',  'Shatabdi',  'NDLS', 'RKMP'),
    ('22691', 'Bengaluru Rajdhani Express', 'Rajdhani', 'SBC',  'NZM'),
    ('12626', 'Kerala Express',           'Superfast', 'NDLS', 'TVC')
ON CONFLICT DO NOTHING;

INSERT INTO route_stops (train_number, stop_seq, station_code, arrives, departs, journey_day) VALUES
    ('12951', 1, 'MMCT', NULL,    '17:00', 1),
    ('12951', 2, 'BVI',  '17:20', '17:22', 1),
    ('12951', 3, 'ST',   '19:40', '19:45', 1),
    ('12951', 4, 'BRC',  '21:05', '21:15', 1),
    ('12951', 5, 'RTM',  '00:15', '00:20', 2),
    ('12951', 6, 'KOTA', '03:10', '03:20', 2),
    ('12951', 7, 'NDLS', '08:30', NULL,    2),
    ('12002', 1, 'NDLS', NULL,    '06:00', 1),
    ('12002', 2, 'MTJ',  '07:20', '07:22', 1),
    ('12002', 3, 'AGC',  '07:50', '07:55', 1),
    ('12002', 4, 'GWL',  '09:25', '09:28', 1),
    ('12002', 5, 'VGLJ', '10:45', '10:50', 1),
    ('12002', 6, 'RKMP', '14:30', NULL,    1),
    ('22691', 1, 'SBC',  NULL,    '20:00', 1),
    ('22691', 2, 'SC',   '07:10', '07:20', 2),
    ('22691', 3, 'NGP',  '15:00', '15:10', 2),
    ('22691', 4, 'BPL',  '21:00', '21:10', 2),
    ('22691', 5, 'NZM',  '05:30', NULL,    3),
    ('12626', 1, 'NDLS', NULL,    '20:10', 1),
    ('12626', 2, 'BPL',  '06:10', '06:20', 2),
    ('12626', 3, 'NGP',  '12:00', '12:10', 2),
    ('12626', 4, 'BZA',  '01:00', '01:15', 3),
    ('12626', 5, 'ERS',  '22:00', '22:05', 3),
    ('12626', 6, 'TVC',  '03:30', NULL,    4)
ON CONFLICT DO NOTHING;