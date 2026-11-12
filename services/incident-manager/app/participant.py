"""Incident Manager's side of two-phase commit: moving an incident to Mitigating when its diversion commits.

The rules are the same as Track Availability's. Every change happens inside one database transaction
that starts by locking the term fence row, so a lower term is refused before anything else is read,
and the vote is committed before the caller gets an answer. A YES vote holds the incident: no other
diversion can be prepared for it, and it cannot be resolved, until this one is decided.
"""

import logging

from psycopg import Connection
from psycopg_pool import ConnectionPool

logger = logging.getLogger("incident_manager")


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
    """Return why the incident cannot move to Mitigating under this plan, or None if it can."""
    for field in ("incident_id", "train_number", "blocked_section", "loop_line"):
        if not plan.get(field):
            return f"the plan has no {field}"
    incident = conn.execute(
        "SELECT section, status, pending_txn_id FROM incidents WHERE incident_id = %s FOR UPDATE",
        (plan["incident_id"],),
    ).fetchone()
    if incident is None:
        return f"incident {plan['incident_id']} is not known here"
    if incident["pending_txn_id"]:
        return f"incident {plan['incident_id']} is held by {incident['pending_txn_id']} (not yet decided)"
    if incident["status"] != "REPORTED":
        return f"incident {plan['incident_id']} is already {incident['status'].lower()}"
    if incident["section"] != plan["blocked_section"]:
        return f"incident {plan['incident_id']} is on {incident['section']}, not {plan['blocked_section']}"
    return None


class IncidentParticipant:
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def prepare(self, txn_id: str, term: int, coordinator_id: str, plan: dict) -> tuple[str, str, bool]:
        """Vote YES and hold the incident, or vote NO with a reason.

        Returns (vote, reason, first_time); first_time is False when the saved vote is repeated.
        """
        with self._pool.connection() as conn:
            _check_term(conn, term)
            existing = conn.execute(
                "SELECT state, vote, vote_reason FROM transactions WHERE txn_id = %s FOR UPDATE", (txn_id,)
            ).fetchone()
            if existing is not None:
                # Asked again: answer as before, so a retried Prepare is harmless
                if existing["state"] == "ABORTED":
                    return "NO", existing["vote_reason"] or "the transaction was already aborted", False
                return existing["vote"], existing["vote_reason"] or "", False

            problem = _plan_problem(conn, plan)
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
                "UPDATE incidents SET pending_txn_id = %s WHERE incident_id = %s",
                (txn_id, plan["incident_id"]),
            )
            logger.info(
                "Voted YES: holding %s for the diversion of train %s through %s",
                plan["incident_id"], plan["train_number"], plan["loop_line"],
                extra={"txn_id": txn_id, "term": term, "incident_id": plan["incident_id"]},
            )
            return "YES", "", True

    def commit(self, txn_id: str, term: int | None) -> tuple[str, dict | None]:
        """Move the incident to Mitigating.

        Returns (state, change); change describes the new status, or is None for a repeated Commit.
        """
        with self._pool.connection() as conn:
            _check_term(conn, term)
            txn = conn.execute(
                "SELECT state, incident_id, train_number, blocked_section, loop_line "
                "FROM transactions WHERE txn_id = %s FOR UPDATE",
                (txn_id,),
            ).fetchone()
            if txn is None:
                raise UnknownTransaction(f"{txn_id} was never prepared here")
            if txn["state"] == "COMMITTED":
                return "COMMITTED", None
            if txn["state"] == "ABORTED":
                raise DecisionConflict(f"{txn_id} was already aborted")
            note = (
                f"Train {txn['train_number']} diverted through {txn['loop_line']} "
                f"while {txn['blocked_section']} is closed"
            )
            conn.execute(
                "UPDATE incidents SET status = 'MITIGATING', pending_txn_id = NULL, updated_at = now() "
                "WHERE incident_id = %s AND pending_txn_id = %s",
                (txn["incident_id"], txn_id),
            )
            conn.execute(
                "INSERT INTO incident_status_history (incident_id, status, note, txn_id) "
                "VALUES (%s, 'MITIGATING', %s, %s)",
                (txn["incident_id"], note, txn_id),
            )
            conn.execute(
                "UPDATE transactions SET state = 'COMMITTED', term = COALESCE(%s, term), updated_at = now() "
                "WHERE txn_id = %s",
                (term, txn_id),
            )
            logger.info(
                "Committed: %s is now mitigating", txn["incident_id"],
                extra={"txn_id": txn_id, "term": term, "incident_id": txn["incident_id"]},
            )
            return "COMMITTED", {"incident_id": txn["incident_id"], "status": "MITIGATING", "note": note}

    def abort(self, txn_id: str, term: int | None, coordinator_id: str) -> tuple[str, bool]:
        """Release the incident. Returns (state, changed); changed is False for a repeat."""
        with self._pool.connection() as conn:
            _check_term(conn, term)
            row = conn.execute("SELECT state FROM transactions WHERE txn_id = %s FOR UPDATE", (txn_id,)).fetchone()
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
                "UPDATE incidents SET pending_txn_id = NULL WHERE pending_txn_id = %s",
                (txn_id,),
            )
            conn.execute(
                "UPDATE transactions SET state = 'ABORTED', term = COALESCE(%s, term), updated_at = now() "
                "WHERE txn_id = %s",
                (term, txn_id),
            )
            logger.info("Aborted and released the incident", extra={"txn_id": txn_id, "term": term})
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
            return conn.execute("SELECT state, vote, term FROM transactions WHERE txn_id = %s", (txn_id,)).fetchone()