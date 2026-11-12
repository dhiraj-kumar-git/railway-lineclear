"""Track Availability's side of two-phase commit.

Every change happens inside one database transaction that starts by locking the term fence row,
so calls are handled one at a time, a lower term is refused before anything else is read, and the
vote is committed to the database before the caller gets an answer.
"""

import logging

from psycopg import Connection
from psycopg_pool import ConnectionPool

logger = logging.getLogger("track_availability")


class StaleTerm(Exception):
    """The call carries a lower term than one already seen: it comes from a former leader."""

    def __init__(self, term: int, highest_term: int) -> None:
        super().__init__(f"term {term} is older than term {highest_term} already seen")


class UnknownTransaction(Exception):
    """Commit was asked for a transaction this participant never prepared."""


class DecisionConflict(Exception):
    """The transaction was already decided the other way."""


def _check_term(conn: Connection, term: int | None) -> None:
    """Lock the fence row, refuse a lower term and remember a higher one.

    None means the decision was read from the coordinator's log after a restart (recovery.py). It is
    not an order from a leader, so it is not fenced; the row is still locked to keep calls in turn.
    """
    row = conn.execute("SELECT highest_term FROM term_fence WHERE id FOR UPDATE").fetchone()
    if term is None:
        return
    if term < row["highest_term"]:
        raise StaleTerm(term, row["highest_term"])
    if term > row["highest_term"]:
        conn.execute("UPDATE term_fence SET highest_term = %s WHERE id", (term,))


def _plan_problem(conn: Connection, plan: dict) -> str | None:
    """Return why the plan cannot be carried out here, or None if it can."""
    for field in ("incident_id", "train_number", "blocked_section", "loop_line"):
        if not plan.get(field):
            return f"the plan has no {field}"
    kinds = {
        row["code"]: row["kind"]
        for row in conn.execute(
            "SELECT code, kind FROM segments WHERE code IN (%s, %s)",
            (plan["blocked_section"], plan["loop_line"]),
        ).fetchall()
    }
    if kinds.get(plan["blocked_section"]) != "block_section":
        return f"{plan['blocked_section']} is not a block section"
    if kinds.get(plan["loop_line"]) != "loop_line":
        return f"{plan['loop_line']} is not a loop line"
    return None


def _conflict(conn: Connection, plan: dict) -> str | None:
    """Return why a segment in the plan is already taken, or None if both are free."""
    hold = conn.execute(
        """
        SELECT segment_code, hold_type, train_number, txn_id, state
        FROM segment_holds
        WHERE segment_code IN (%s, %s) AND state IN ('PREPARED', 'COMMITTED')
        ORDER BY segment_code
        LIMIT 1
        """,
        (plan["blocked_section"], plan["loop_line"]),
    ).fetchone()
    if hold is None:
        return None
    pending = " (not yet decided)" if hold["state"] == "PREPARED" else ""
    if hold["hold_type"] == "reservation":
        return (
            f"{hold['segment_code']} is already reserved for train {hold['train_number']} "
            f"by {hold['txn_id']}{pending}"
        )
    return f"{hold['segment_code']} is already closed by {hold['txn_id']}{pending}"


class TrackParticipant:
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def prepare(self, txn_id: str, term: int, coordinator_id: str, plan: dict) -> tuple[str, str, bool]:
        """Vote YES and hold both segments, or vote NO with a reason.

        Returns (vote, reason, first_time); first_time is False when the saved vote is repeated.
        """
        with self._pool.connection() as conn:
            _check_term(conn, term)
            existing = conn.execute(
                "SELECT state, vote, vote_reason FROM transactions WHERE txn_id = %s FOR UPDATE",
                (txn_id,),
            ).fetchone()
            if existing is not None:
                # Asked again: answer as before, so a retried Prepare is harmless
                if existing["state"] == "ABORTED":
                    return "NO", existing["vote_reason"] or "the transaction was already aborted", False
                return existing["vote"], existing["vote_reason"] or "", False

            problem = _plan_problem(conn, plan) or _conflict(conn, plan)
            if problem is not None:
                # A participant that votes no may abort at once
                conn.execute(
                    """
                    INSERT INTO transactions (txn_id, state, vote, vote_reason, term, coordinator_id,
                                              incident_id, train_number, blocked_section, loop_line)
                    VALUES (%s, 'ABORTED', 'NO', %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (txn_id, problem, term, coordinator_id, plan.get("incident_id"), plan.get("train_number"),
                     plan.get("blocked_section"), plan.get("loop_line")),
                )
                logger.info("Voted NO: %s", problem, extra={"txn_id": txn_id, "term": term})
                return "NO", problem, True

            conn.execute(
                """
                INSERT INTO transactions (txn_id, state, vote, term, coordinator_id,
                                          incident_id, train_number, blocked_section, loop_line)
                VALUES (%s, 'PREPARED', 'YES', %s, %s, %s, %s, %s, %s)
                """,
                (txn_id, term, coordinator_id, plan["incident_id"], plan["train_number"],
                 plan["blocked_section"], plan["loop_line"]),
            )
            conn.execute(
                """
                INSERT INTO segment_holds (segment_code, txn_id, hold_type, train_number, state) VALUES
                    (%s, %s, 'closure', NULL, 'PREPARED'),
                    (%s, %s, 'reservation', %s, 'PREPARED')
                """,
                (plan["blocked_section"], txn_id, plan["loop_line"], txn_id, plan["train_number"]),
            )
            logger.info(
                "Voted YES: holding %s closed and %s for train %s",
                plan["blocked_section"], plan["loop_line"], plan["train_number"],
                extra={"txn_id": txn_id, "term": term},
            )
            return "YES", "", True

    def commit(self, txn_id: str, term: int | None) -> tuple[str, bool]:
        """Make the holds permanent. Returns (state, changed); changed is False for a repeat."""
        with self._pool.connection() as conn:
            _check_term(conn, term)
            row = conn.execute(
                "SELECT state FROM transactions WHERE txn_id = %s FOR UPDATE", (txn_id,)
            ).fetchone()
            if row is None:
                raise UnknownTransaction(f"{txn_id} was never prepared here")
            if row["state"] == "COMMITTED":
                return "COMMITTED", False
            if row["state"] == "ABORTED":
                raise DecisionConflict(f"{txn_id} was already aborted")
            conn.execute(
                "UPDATE segment_holds SET state = 'COMMITTED', updated_at = now() "
                "WHERE txn_id = %s AND state = 'PREPARED'",
                (txn_id,),
            )
            conn.execute(
                "UPDATE transactions SET state = 'COMMITTED', term = COALESCE(%s, term), updated_at = now() "
                "WHERE txn_id = %s",
                (term, txn_id),
            )
            logger.info("Committed", extra={"txn_id": txn_id, "term": term})
            return "COMMITTED", True

    def abort(self, txn_id: str, term: int | None, coordinator_id: str) -> tuple[str, bool]:
        """Release the holds. Returns (state, changed); changed is False for a repeat."""
        with self._pool.connection() as conn:
            _check_term(conn, term)
            row = conn.execute(
                "SELECT state FROM transactions WHERE txn_id = %s FOR UPDATE", (txn_id,)
            ).fetchone()
            if row is None:
                # Remember the abort, so a Prepare that arrives late is refused
                conn.execute(
                    "INSERT INTO transactions (txn_id, state, vote_reason, term, coordinator_id) "
                    "VALUES (%s, 'ABORTED', 'the transaction was aborted before it was prepared', %s, %s)",
                    (txn_id, term, coordinator_id),
                )
                logger.info("Aborted before Prepare arrived", extra={"txn_id": txn_id, "term": term})
                return "ABORTED", True
            if row["state"] == "ABORTED":
                return "ABORTED", False
            if row["state"] == "COMMITTED":
                raise DecisionConflict(f"{txn_id} was already committed")
            conn.execute(
                "UPDATE segment_holds SET state = 'RELEASED', updated_at = now() "
                "WHERE txn_id = %s AND state = 'PREPARED'",
                (txn_id,),
            )
            conn.execute(
                "UPDATE transactions SET state = 'ABORTED', term = COALESCE(%s, term), updated_at = now() "
                "WHERE txn_id = %s",
                (term, txn_id),
            )
            logger.info("Aborted and released holds", extra={"txn_id": txn_id, "term": term})
            return "ABORTED", True

    def in_doubt(self) -> list[str]:
        """Transactions this participant voted YES for whose outcome has not arrived yet."""
        with self._pool.connection() as conn:
            rows = conn.execute(
                "SELECT txn_id FROM transactions WHERE state = 'PREPARED' ORDER BY updated_at"
            ).fetchall()
        return [row["txn_id"] for row in rows]

    def decision(self, txn_id: str) -> dict | None:
        with self._pool.connection() as conn:
            return conn.execute(
                "SELECT state, vote, term FROM transactions WHERE txn_id = %s", (txn_id,)
            ).fetchone()

    def segments(self, held_only: bool) -> tuple[list[dict], int]:
        """Every segment with its active hold, if any, and the highest term seen."""
        with self._pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT s.code, s.kind, h.hold_type, h.state AS hold_state, h.txn_id, h.train_number
                FROM segments s
                LEFT JOIN segment_holds h
                    ON h.segment_code = s.code AND h.state IN ('PREPARED', 'COMMITTED')
                WHERE NOT %s OR h.id IS NOT NULL
                ORDER BY s.kind, length(s.code), s.code
                """,
                (held_only,),
            ).fetchall()
            highest = conn.execute("SELECT highest_term FROM term_fence WHERE id").fetchone()
            return rows, highest["highest_term"]

    def release_all_holds(self) -> tuple[int, int]:
        """Free every segment and abort undecided transactions. Returns (holds, transactions)."""
        with self._pool.connection() as conn:
            conn.execute("SELECT highest_term FROM term_fence WHERE id FOR UPDATE")
            holds = conn.execute(
                "UPDATE segment_holds SET state = 'RELEASED', updated_at = now() "
                "WHERE state IN ('PREPARED', 'COMMITTED')"
            ).rowcount
            aborted = conn.execute(
                "UPDATE transactions SET state = 'ABORTED', updated_at = now() WHERE state = 'PREPARED'"
            ).rowcount
            logger.info("Released %d holds and aborted %d prepared transactions", holds, aborted)
            return holds, aborted