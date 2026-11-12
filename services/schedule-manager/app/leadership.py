"""Keeps this replica's place in the election: campaign until leader, then renew every 2 seconds.

The lease in Leader Election lasts 6 seconds. This replica acts as leader for at most 4 seconds after
its last successful renewal, so it stops coordinating before the lease can pass to another replica,
even if Leader Election cannot be reached.

pause() stops the renewals for a while, as if the whole process had frozen; the fault switch uses it.
"""

import logging
import threading
import time

import grpc
from prometheus_client import Gauge

from lineclear import election_pb2, election_pb2_grpc

RENEW_SECONDS = 2.0
ACT_FOR_SECONDS = 4.0  # shorter than the 6-second lease

IS_LEADER = Gauge("lineclear_coordinator_is_leader", "1 while this replica is the coordinator, else 0")
TERM = Gauge("lineclear_coordinator_term", "The term this replica last led with")

logger = logging.getLogger("schedule_manager")


class Leadership:
    def __init__(self, target: str, election: str, candidate_id: str) -> None:
        self._channel = grpc.insecure_channel(target)
        self._stub = election_pb2_grpc.LeaderElectionStub(self._channel)
        self._election = election
        self.candidate_id = candidate_id
        self._lock = threading.Lock()
        self._term = 0
        self._leader_id = ""
        self._valid_until = 0.0  # time.monotonic() after which this replica stops acting as leader
        self._paused_until = 0.0  # no renewals before this time.monotonic()
        self._stopping = threading.Event()
        self._thread = threading.Thread(target=self._run, name="leadership", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stopping.set()
        self._thread.join(timeout=5)
        self._channel.close()

    def pause(self, seconds: float) -> None:
        """Stop renewing for this many seconds. For the demo fault switch only."""
        with self._lock:
            self._paused_until = time.monotonic() + seconds

    def term_if_leader(self) -> int:
        """The current term while this replica may act as leader, else 0."""
        with self._lock:
            return self._term if time.monotonic() < self._valid_until else 0

    def status(self) -> dict:
        term = self.term_if_leader()
        with self._lock:
            return {
                "replica": self.candidate_id,
                "is_leader": term > 0,
                "term": self._term,
                "leader_id": self.candidate_id if term else self._leader_id,
            }

    def _run(self) -> None:
        leading = False
        while not self._stopping.is_set():
            if time.monotonic() >= self._paused_until:
                self._renew()
            now_leading = self.term_if_leader() > 0
            if leading and not now_leading:
                logger.warning("Stopped acting as coordinator for term %d", self._term, extra={"term": self._term})
            leading = now_leading
            IS_LEADER.set(1 if leading else 0)
            self._stopping.wait(RENEW_SECONDS)

    def _renew(self) -> None:
        asked_at = time.monotonic()  # measured before the call, so the margin is never overstated
        term = self.term_if_leader()
        try:
            if term:
                reply = self._stub.Heartbeat(
                    election_pb2.HeartbeatRequest(election=self._election, candidate_id=self.candidate_id, term=term),
                    timeout=1.0,
                )
                leading, reply_term = reply.still_leader, reply.term
            else:
                reply = self._stub.Campaign(
                    election_pb2.CampaignRequest(election=self._election, candidate_id=self.candidate_id),
                    timeout=1.0,
                )
                leading, reply_term = reply.is_leader, reply.term
        except grpc.RpcError as error:
            logger.warning("Could not reach Leader Election: %s", error.code().name)
            return  # keep acting until the margin runs out; a later renewal may still succeed

        with self._lock:
            was_leader = time.monotonic() < self._valid_until
            self._leader_id = reply.leader_id
            if leading:
                if not was_leader or reply_term != self._term:
                    logger.info("Became the coordinator with term %d", reply_term, extra={"term": reply_term})
                self._term = reply_term
                self._valid_until = asked_at + ACT_FOR_SECONDS
                TERM.set(reply_term)
            elif was_leader:
                self._valid_until = 0.0
                logger.warning(
                    "No longer the coordinator; %s leads with term %d", reply.leader_id or "nobody", reply_term,
                    extra={"term": self._term},
                )