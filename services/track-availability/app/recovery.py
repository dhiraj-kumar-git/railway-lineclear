"""After a restart, settle the transactions this participant voted YES for but never heard the outcome of.

A participant that voted YES may not decide alone: the coordinator may have logged either outcome. So it
asks the coordinator's own log through Coordinator.GetDecision on Schedule Manager:
- COMMITTED: commit here. ABORTED, or a transaction the coordinator never logged: abort here.
- PREPARED (not decided yet), or no answer: keep holding, and ask again in 5 seconds.
The answer comes from the log, not from a leader, so it is applied without a term check.
"""

import logging
import threading
from collections.abc import Callable

import grpc

from lineclear import participant_pb2, participant_pb2_grpc

RETRY_SECONDS = 5.0

logger = logging.getLogger("track_availability")


class InDoubtRecovery:
    def __init__(self, participant, coordinator_target: str, on_commit: Callable | None = None) -> None:
        self._participant = participant
        self._on_commit = on_commit  # called with (txn_id, what commit returned)
        self._channel = grpc.insecure_channel(coordinator_target)
        self._coordinator = participant_pb2_grpc.CoordinatorStub(self._channel)
        self._stopping = threading.Event()
        self._thread = threading.Thread(target=self._run, name="in-doubt-recovery", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stopping.set()
        self._thread.join(timeout=5)
        self._channel.close()

    def _run(self) -> None:
        in_doubt = self._participant.in_doubt()
        if not in_doubt:
            logger.info("No transactions in doubt after start-up")
            return
        logger.warning("%d transaction(s) in doubt after start-up: %s", len(in_doubt), ", ".join(in_doubt))
        while in_doubt and not self._stopping.is_set():
            in_doubt = [txn_id for txn_id in in_doubt if not self._settle(txn_id)]
            if in_doubt:
                self._stopping.wait(RETRY_SECONDS)

    def _settle(self, txn_id: str) -> bool:
        """Ask the coordinator and apply its decision. True once the transaction is settled here."""
        try:
            reply = self._coordinator.GetDecision(
                participant_pb2.GetDecisionRequest(txn_id=txn_id), timeout=2.0, metadata=(("txn-id", txn_id),)
            )
        except grpc.RpcError as error:
            logger.warning("Could not ask the coordinator about %s (%s); asking again in %d s",
                           txn_id, error.code().name, RETRY_SECONDS, extra={"txn_id": txn_id})
            return False
        try:
            if reply.state == participant_pb2.TXN_STATE_COMMITTED:
                result = self._participant.commit(txn_id, None)
                logger.info("Coordinator's log says COMMIT; committed after start-up", extra={"txn_id": txn_id})
                if self._on_commit is not None:
                    self._on_commit(txn_id, result)
                return True
            if reply.state == participant_pb2.TXN_STATE_ABORTED:
                self._participant.abort(txn_id, None, "")
                logger.info("Coordinator's log says ABORT; aborted after start-up", extra={"txn_id": txn_id})
                return True
        except Exception as error:  # settled meanwhile, for example by the coordinator's own retry
            logger.info("Already settled: %s", error, extra={"txn_id": txn_id})
            return True
        logger.info("Coordinator has not decided %s yet; still holding", txn_id, extra={"txn_id": txn_id})
        return False
