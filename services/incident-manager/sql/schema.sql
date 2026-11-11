-- Incident numbers run I-45, I-46, ... so the first report matches the worked example in the design document
CREATE SEQUENCE IF NOT EXISTS incident_number START WITH 45;

-- One row per reported incident; pending_txn_id is set while a diversion for it is prepared but undecided
CREATE TABLE IF NOT EXISTS incidents (
    incident_id    text PRIMARY KEY DEFAULT 'I-' || nextval('incident_number'),
    kind           text NOT NULL CHECK (kind IN ('rail_fracture', 'signal_failure', 'track_obstruction', 'overhead_wire_fault')),
    section        text NOT NULL,
    train_number   text,
    description    text NOT NULL DEFAULT '',
    status         text NOT NULL DEFAULT 'REPORTED' CHECK (status IN ('REPORTED', 'MITIGATING', 'RESOLVED')),
    pending_txn_id text,
    reported_at    timestamptz NOT NULL DEFAULT now(),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

-- Every status an incident has had, oldest first; rows are only ever added
CREATE TABLE IF NOT EXISTS incident_status_history (
    id          bigserial PRIMARY KEY,
    incident_id text NOT NULL REFERENCES incidents (incident_id),
    status      text NOT NULL,
    note        text NOT NULL DEFAULT '',
    txn_id      text,
    changed_at  timestamptz NOT NULL DEFAULT now()
);

-- The highest leader term seen; calls carrying a lower term are refused
CREATE TABLE IF NOT EXISTS term_fence (
    id           boolean PRIMARY KEY DEFAULT true CHECK (id),
    highest_term bigint  NOT NULL DEFAULT 0
);

-- One row per two-phase commit transaction, written before any reply is sent
CREATE TABLE IF NOT EXISTS transactions (
    txn_id          text PRIMARY KEY,
    state           text   NOT NULL CHECK (state IN ('PREPARED', 'COMMITTED', 'ABORTED')),
    vote            text   CHECK (vote IN ('YES', 'NO')), -- empty when Abort arrived before Prepare
    vote_reason     text,
    term            bigint NOT NULL,
    coordinator_id  text   NOT NULL,
    incident_id     text,
    train_number    text,
    blocked_section text,
    loop_line       text,
    updated_at      timestamptz NOT NULL DEFAULT now()
);