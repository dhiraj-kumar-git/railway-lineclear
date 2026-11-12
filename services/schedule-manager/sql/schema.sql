-- The loop line that relieves each block section when it is closed
CREATE TABLE IF NOT EXISTS loop_lines (
    blocked_section text PRIMARY KEY,
    loop_line       text NOT NULL
);

-- Transaction numbers run T-101, T-102, ... so the first matches the worked example in the design document
CREATE SEQUENCE IF NOT EXISTS transaction_number START WITH 101;

-- The coordinator's log: one row per diversion. PREPARING is written before any Prepare is sent,
-- and COMMIT or ABORT before any Commit or Abort, so a new leader can always finish what was started.
CREATE TABLE IF NOT EXISTS transactions (
    txn_id          text PRIMARY KEY DEFAULT 'T-' || nextval('transaction_number'),
    incident_id     text   NOT NULL UNIQUE, -- one diversion per incident, so a redelivered event starts nothing new
    train_number    text   NOT NULL,
    blocked_section text   NOT NULL,
    loop_line       text   NOT NULL,
    term            bigint NOT NULL,
    coordinator_id  text   NOT NULL,
    state           text   NOT NULL CHECK (state IN ('PREPARING', 'COMMIT', 'ABORT')),
    reason          text,                       -- why it was aborted
    revised_stops   jsonb  NOT NULL DEFAULT '[]', -- written to the schedule only if the decision is COMMIT
    started_at      timestamptz NOT NULL DEFAULT now(),
    decided_at      timestamptz,
    completed_at    timestamptz                 -- every participant has acknowledged the decision
);

-- Each participant's vote and whether it has acknowledged the decision
CREATE TABLE IF NOT EXISTS transaction_participants (
    txn_id          text NOT NULL REFERENCES transactions (txn_id),
    participant     text NOT NULL,
    vote            text CHECK (vote IN ('YES', 'NO')),
    reason          text,
    acknowledged_at timestamptz,
    PRIMARY KEY (txn_id, participant)
);

-- Revised arrival times: the schedule's own part of a diversion, written in the same
-- database transaction as the COMMIT decision, so it exists if and only if the diversion committed
CREATE TABLE IF NOT EXISTS schedule_revisions (
    txn_id          text    NOT NULL REFERENCES transactions (txn_id),
    train_number    text    NOT NULL,
    stop_seq        integer NOT NULL,
    station_code    text    NOT NULL,
    station_name    text    NOT NULL,
    planned_arrival text    NOT NULL,
    revised_arrival text    NOT NULL,
    delay_minutes   integer NOT NULL,
    PRIMARY KEY (txn_id, stop_seq)
);

-- When a committed diversion was handed to the passenger notification queue.
-- A new leader sends any COMMIT that has none, so a notice is never lost to a crash.
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS notified_at timestamptz;

-- The demo fault switch, one row. When armed, the coordinator freezes right after it next logs
-- COMMIT, as if its process had stopped, so the hand-over to the other replica can be shown on demand.
CREATE TABLE IF NOT EXISTS fault_switch (
    id            boolean PRIMARY KEY DEFAULT true CHECK (id), -- only one row can exist
    armed         boolean NOT NULL DEFAULT false,
    pause_seconds integer NOT NULL DEFAULT 0,
    armed_at      timestamptz
);
