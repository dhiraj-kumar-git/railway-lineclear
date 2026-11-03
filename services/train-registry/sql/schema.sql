CREATE TABLE IF NOT EXISTS stations (
    code text PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE IF NOT EXISTS trains (
    number      text PRIMARY KEY,
    name        text NOT NULL,
    category    text NOT NULL,
    origin      text NOT NULL REFERENCES stations (code),
    destination text NOT NULL REFERENCES stations (code)
);

CREATE TABLE IF NOT EXISTS route_stops (
    train_number text    NOT NULL REFERENCES trains (number),
    stop_seq     integer NOT NULL,
    station_code text    NOT NULL REFERENCES stations (code),
    arrives      time,
    departs      time,
    journey_day  integer NOT NULL DEFAULT 1,
    PRIMARY KEY (train_number, stop_seq)
);