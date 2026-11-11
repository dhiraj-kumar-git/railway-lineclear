"""The two-phase commit coordinator for diversions around a track incident. Only the leader runs it.

For each reported incident:
1. PREPARING is written to the log, with the plan, before any participant hears of it.
2. Every participant is asked to prepare, in parallel, each with a 2-second deadline.
   No answer, an error or a NO vote means the diversion cannot go ahead.
3. COMMIT (all voted YES) or ABORT is written to the log before anyone is told. On COMMIT the
   revised arrival times are written to the schedule in the same database transaction.
4. The decision is sent to every participant, retrying until each one acknowledges.

The decision is written with "UPDATE ... WHERE state = 'PREPARING'", so of two coordinators that
somehow race to decide the same transaction, only the first write counts and the other follows it.
"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor

import grpc
from prometheus_client import Counter, Histogram
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from app.events import CoordinationEvents
from app.leadership import Leadership
from app.participants import ParticipantClient
from app.revisions import revised_stops
from app.train_registry import TrainRegistryClient, TrainRegistryUnavailable

RETRYABLE = {
    grpc.StatusCode.UNAVAILABLE,
    grpc.StatusCode.DEADLINE_EXCEEDED,
    grpc.StatusCode.RESOURCE_EXHAUSTED,
    grpc.StatusCode.INTERNAL,
    grpc.StatusCode.UNKNOWN,
}

TRANSACTIONS = Counter("lineclear_transactions_total", "Diversions decided, by decision", ["decision"])
PREPARE_SECONDS = Histogram(
    "lineclear_transaction_prepare_seconds",
    "Time from writing PREPARING to writing the decision",
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 2.5, 5.0),
)

logger = logging.getLogger("schedule_manager")


class NotLeader(Exception):
    """This replica is not the coordinator, or stopped being it during the work."""


class Coordinator:
    def __init__(
        self,
        pool: ConnectionPool,
        leadership: Leadership,
        participants: list[ParticipantClient],
        registry: TrainRegistryClient,
        events: CoordinationEvents,
        delay_minutes: int,
    ) -> None:
        self._pool = pool
        self._leadership = leadership
        self._participants = {client.name: client for client in participants}
        self._registry = registry
        self._events = events
        self._delay_minutes = delay_minutes
        self._phase_one = ThreadPoolExecutor(max_workers=len(participants), thread_name_prefix="prepare")

    def handle_incident(self, event: dict) -> None:
        """Divert the affected train around a reported incident.

        Raises NotLeader if this replica is not, or stops being, the coordinator. The caller then
        leaves the Kafka message unacknowledged, so the next leader picks the incident up.
        """
        term = self._leadership.term_if_leader()
        if not term:
            raise NotLeader()
        incident_id = event["incident_id"]
        if not event.get("train_number"):
            logger.info("Incident %s affects no train; nothing to divert", incident_id,
                        extra={"incident_id": incident_id})
            return
        txn, created = self._begin(incident_id, event["train_number"], event["section"], term)
        if txn is None:
            logger.warning("No loop line relieves %s; incident %s left to staff", event["section"], incident_id,
                           extra={"incident_id": incident_id})
            return
        if created:
            self._run(txn)
        else:
            self.resume(txn)

    def _begin(self, incident_id: str, train_number: str, section: str, term: int) -> tuple[dict | None, bool]:
        """Write PREPARING for a new diversion. Returns (transaction, created)."""
        with self._pool.connection() as conn:
            loop = conn.execute("SELECT loop_line FROM loop_lines WHERE blocked_section = %s", (section,)).fetchone()
            if loop is None:
                return None, False
            txn = conn.execute("SELECT * FROM transactions WHERE incident_id = %s", (incident_id,)).fetchone()
            if txn is not None:
                return txn, False  # the same incident event arrived again: carry on with what is logged
            txn = conn.execute(
                """
                INSERT INTO transactions (incident_id, train_number, blocked_section, loop_line,
                                          term, coordinator_id, state)
                VALUES (%s, %s, %s, %s, %s, %s, 'PREPARING')
                ON CONFLICT (incident_id) DO NOTHING
                RETURNING *
                """,
                (incident_id, train_number, section, loop["loop_line"], term, self._leadership.candidate_id),
            ).fetchone()
            if txn is None:  # another coordinator logged it a moment ago
                txn = conn.execute("SELECT * FROM transactions WHERE incident_id = %s", (incident_id,)).fetchone()
                return txn, False
            with conn.cursor() as cursor:
                cursor.executemany(
                    "INSERT INTO transaction_participants (txn_id, participant) VALUES (%s, %s)",
                    [(txn["txn_id"], name) for name in self._participants],
                )
        logger.info(
            "PREPARING: divert train %s through %s while %s is closed for %s",
            train_number, loop["loop_line"], section, incident_id,
            extra={"txn_id": txn["txn_id"], "term": term, "incident_id": incident_id},
        )
        return txn, True

    def _run(self, txn: dict) -> None:
        txn_id, term = txn["txn_id"], txn["term"]
        started = time.monotonic()
        plan = {
            "incident_id": txn["incident_id"],
            "train_number": txn["train_number"],
            "blocked_section": txn["blocked_section"],
            "loop_line": txn["loop_line"],
        }

        # The route is needed for the revised arrival times; without it the diversion cannot be planned
        try:
            train = self._registry.train(txn["train_number"], txn_id)
        except TrainRegistryUnavailable as error:
            self._decide(txn, "ABORT", f"Train Registry could not be reached: {error}", [], started)
            return
        if train is None:
            self._decide(txn, "ABORT", f"train {txn['train_number']} is not in Train Registry", [], started)
            return
        stops = revised_stops(train, txn["blocked_section"], self._delay_minutes)
        logger.info("Route of %s read from Train Registry: %d of its %d stops arrive %d minutes late",
                    txn["train_number"], len(stops), len(train["stops"]), self._delay_minutes,
                    extra={"txn_id": txn_id, "term": term})
        with self._pool.connection() as conn:
            conn.execute("UPDATE transactions SET revised_stops = %s WHERE txn_id = %s", (Jsonb(stops), txn_id))

        # Phase 1: every participant votes, in parallel
        coordinator_id = self._leadership.candidate_id
        futures = {
            name: self._phase_one.submit(client.prepare, txn_id, term, coordinator_id, plan)
            for name, client in self._participants.items()
        }
        votes = {name: future.result() for name, future in futures.items()}
        with self._pool.connection() as conn:
            for name, (vote, reason) in votes.items():
                conn.execute(
                    "UPDATE transaction_participants SET vote = %s, reason = %s WHERE txn_id = %s AND participant = %s",
                    (vote, reason or None, txn_id, name),
                )
                logger.info("%s voted %s%s", name, vote, f": {reason}" if reason else "",
                            extra={"txn_id": txn_id, "term": term, "participant": name, "vote": vote})

        refusals = [reason for vote, reason in votes.values() if vote != "YES"]
        if refusals:
            self._decide(txn, "ABORT", "; ".join(refusals), [], started)
        else:
            self._decide(txn, "COMMIT", None, stops, started)

    def _decide(self, txn: dict, decision: str, reason: str | None, stops: list[dict], started: float) -> None:
        """Write the decision, then deliver it. Nothing is sent to a participant before the write."""
        if self._leadership.term_if_leader() != txn["term"]:
            logger.warning("Lost the coordinator role before deciding; %s stays PREPARING for the next leader",
                           txn["txn_id"], extra={"txn_id": txn["txn_id"], "term": txn["term"]})
            raise NotLeader()
        decided = self._log_decision(txn, decision, reason, stops)
        if decided == decision:
            prepare_seconds = time.monotonic() - started
            PREPARE_SECONDS.observe(prepare_seconds)
            self._events.transaction_decided(txn, decision, reason, int(prepare_seconds * 1000))
        self._finish(txn, decided)

    def _log_decision(self, txn: dict, decision: str, reason: str | None, stops: list[dict]) -> str:
        """Write COMMIT or ABORT, unless the transaction was already decided. Returns the decision in the log."""
        txn_id = txn["txn_id"]
        with self._pool.connection() as conn:
            written = conn.execute(
                "UPDATE transactions SET state = %s, reason = %s, decided_at = now() "
                "WHERE txn_id = %s AND state = 'PREPARING' RETURNING state",
                (decision, reason, txn_id),
            ).fetchone()
            if written is None:
                logged = conn.execute("SELECT state FROM transactions WHERE txn_id = %s", (txn_id,)).fetchone()
                logger.warning("%s was already decided as %s; following that", txn_id, logged["state"],
                               extra={"txn_id": txn_id})
                return logged["state"]
            if decision == "COMMIT":
                with conn.cursor() as cursor:
                    cursor.executemany(
                        """
                        INSERT INTO schedule_revisions (txn_id, train_number, stop_seq, station_code, station_name,
                                                        planned_arrival, revised_arrival, delay_minutes)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        [(txn_id, txn["train_number"], stop["stop_seq"], stop["station_code"], stop["station_name"],
                          stop["planned_arrival"], stop["revised_arrival"], stop["delay_minutes"]) for stop in stops],
                    )
        TRANSACTIONS.labels(decision.lower()).inc()
        logger.info("Decision %s logged%s", decision, f": {reason}" if reason else "",
                    extra={"txn_id": txn_id, "term": txn["term"], "decision": decision})
        return decision

    def _finish(self, txn: dict, decision: str) -> None:
        """Phase 2: send the decision to every participant that has not acknowledged it yet."""
        txn_id = txn["txn_id"]
        with self._pool.connection() as conn:
            waiting = [
                row["participant"]
                for row in conn.execute(
                    "SELECT participant FROM transaction_participants "
                    "WHERE txn_id = %s AND acknowledged_at IS NULL ORDER BY participant",
                    (txn_id,),
                ).fetchall()
            ]
        acknowledged = all([self._deliver(name, decision, txn_id) for name in waiting])
        if acknowledged:
            with self._pool.connection() as conn:
                conn.execute(
                    "UPDATE transactions SET completed_at = now() WHERE txn_id = %s AND completed_at IS NULL",
                    (txn_id,),
                )
            logger.info("%s complete: every participant acknowledged %s", txn_id, decision,
                        extra={"txn_id": txn_id, "decision": decision})

    def _deliver(self, name: str, decision: str, txn_id: str) -> bool:
        """Send Commit or Abort to one participant until it acknowledges. False if it refused."""
        client = self._participants[name]
        wait = 0.5
        while True:
            # Phase 2 carries the current leader's term, which may be newer than the transaction's own
            term = self._leadership.term_if_leader()
            if not term:
                logger.warning("Lost the coordinator role while delivering %s; the next leader will finish it",
                               decision, extra={"txn_id": txn_id})
                raise NotLeader()
            try:
                client.decide(decision, txn_id, term, self._leadership.candidate_id)
            except grpc.RpcError as error:
                if error.code() not in RETRYABLE:
                    logger.error("%s refused %s: %s: %s", name, decision, error.code().name, error.details(),
                                 extra={"txn_id": txn_id, "term": term, "participant": name})
                    return False
                logger.warning("%s did not acknowledge %s (%s); retrying in %.1f s", name, decision,
                               error.code().name, wait, extra={"txn_id": txn_id, "participant": name})
                time.sleep(wait)
                wait = min(wait * 2, 4.0)
                continue
            with self._pool.connection() as conn:
                conn.execute(
                    "UPDATE transaction_participants SET acknowledged_at = now() "
                    "WHERE txn_id = %s AND participant = %s",
                    (txn_id, name),
                )
            logger.info("%s acknowledged %s", name, decision,
                        extra={"txn_id": txn_id, "term": term, "participant": name})
            return True

    def resume(self, txn: dict) -> None:
        """Finish a transaction this or an earlier leader started.

        Undecided means nobody can have been told COMMIT, so it is aborted (presumed abort).
        Decided but not acknowledged by everyone means the decision is sent again.
        """
        if not self._leadership.term_if_leader():
            raise NotLeader()
        if txn["completed_at"] is not None:
            logger.info("%s is already complete (%s); nothing to do", txn["txn_id"], txn["state"],
                        extra={"txn_id": txn["txn_id"]})
            return
        decision = txn["state"]
        if decision == "PREPARING":
            decision = self._log_decision(txn, "ABORT", "the coordinator changed before deciding", [])
            if decision == "ABORT":
                self._events.transaction_decided(txn, "ABORT", "the coordinator changed before deciding", 0)
        logger.info("Resuming %s to deliver %s", txn["txn_id"], decision, extra={"txn_id": txn["txn_id"]})
        self._finish(txn, decision)

    def close(self) -> None:
        self._phase_one.shutdown(wait=False)