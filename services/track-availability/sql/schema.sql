-- Block sections (S1 to S10) and loop lines (L1, L2) of the division
CREATE TABLE IF NOT EXISTS segments (
    code text PRIMARY KEY,
    kind text NOT NULL CHECK (kind IN ('block_section', 'loop_line')),
    name text NOT NULL
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

-- A hold is the lock on a segment: a closure for a broken section, a reservation for a diverted train
CREATE TABLE IF NOT EXISTS segment_holds (
    id           bigserial PRIMARY KEY,
    segment_code text NOT NULL REFERENCES segments (code),
    txn_id       text NOT NULL REFERENCES transactions (txn_id),
    hold_type    text NOT NULL CHECK (hold_type IN ('closure', 'reservation')),
    train_number text,
    state        text NOT NULL CHECK (state IN ('PREPARED', 'COMMITTED', 'RELEASED')),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

-- At most one prepared or committed hold per segment
CREATE UNIQUE INDEX IF NOT EXISTS one_active_hold_per_segment
    ON segment_holds (segment_code)
    WHERE state IN ('PREPARED', 'COMMITTED');