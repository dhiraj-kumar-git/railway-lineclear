"""The gRPC side of Incident Manager: the two-phase commit Participant, health and reflection."""

import logging
import os
from concurrent import futures

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection
from prometheus_client import Counter

from app.events import IncidentEvents
from app.participant import DecisionConflict, IncidentParticipant, StaleTerm, UnknownTransaction
from app.telemetry import MetricsInterceptor
from lineclear import participant_pb2, participant_pb2_grpc

GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))

VOTES = Counter("lineclear_participant_votes_total", "Prepare votes given", ["vote"])
DECISIONS = Counter("lineclear_participant_decisions_total", "Transactions committed or aborted", ["decision"])
STALE_TERMS = Counter("lineclear_participant_stale_term_rejections_total", "Calls refused for carrying an old term")

TXN_STATES = {
    None: participant_pb2.TXN_STATE_UNSPECIFIED,
    "PREPARED": participant_pb2.TXN_STATE_PREPARED,
    "COMMITTED": participant_pb2.TXN_STATE_COMMITTED,
    "ABORTED": participant_pb2.TXN_STATE_ABORTED,
}
VOTES_BY_NAME = {None: participant_pb2.VOTE_UNSPECIFIED, "YES": participant_pb2.VOTE_YES, "NO": participant_pb2.VOTE_NO}

logger = logging.getLogger("incident_manager")


def _require(context: grpc.ServicerContext, txn_id: str, term: int | None = None) -> None:
    if not txn_id:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "txn_id is required")
    if term is not None and term < 1:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "term must be 1 or more")


def _refuse_stale(context: grpc.ServicerContext, error: StaleTerm, txn_id: str) -> None:
    STALE_TERMS.inc()
    logger.warning("Refused: %s", error, extra={"txn_id": txn_id})
    context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))


class ParticipantService(participant_pb2_grpc.ParticipantServicer):
    def __init__(self, participant: IncidentParticipant, events: IncidentEvents) -> None:
        self._participant = participant
        self._events = events

    def Prepare(self, request, context):
        _require(context, request.txn_id, request.term)
        plan = {
            "incident_id": request.plan.incident_id,
            "train_number": request.plan.train_number,
            "blocked_section": request.plan.blocked_section,
            "loop_line": request.plan.loop_line,
        }
        try:
            vote, reason, first_time = self._participant.prepare(
                request.txn_id, request.term, request.coordinator_id, plan
            )
        except StaleTerm as error:
            _refuse_stale(context, error, request.txn_id)
        if first_time:
            VOTES.labels(vote).inc()
        return participant_pb2.PrepareReply(vote=VOTES_BY_NAME[vote], reason=reason)

    def Commit(self, request, context):
        _require(context, request.txn_id, request.term)
        try:
            state, change = self._participant.commit(request.txn_id, request.term)
        except StaleTerm as error:
            _refuse_stale(context, error, request.txn_id)
        except UnknownTransaction as error:
            context.abort(grpc.StatusCode.NOT_FOUND, str(error))
        except DecisionConflict as error:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
        if change is not None:
            DECISIONS.labels("committed").inc()
            # Sent only after the database change is committed
            self._events.status_changed(change["incident_id"], change["status"], change["note"], request.txn_id)
        return participant_pb2.DecisionReply(state=TXN_STATES[state])

    def Abort(self, request, context):
        _require(context, request.txn_id, request.term)
        try:
            state, changed = self._participant.abort(request.txn_id, request.term, request.coordinator_id)
        except StaleTerm as error:
            _refuse_stale(context, error, request.txn_id)
        except DecisionConflict as error:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
        if changed:
            DECISIONS.labels("aborted").inc()
        return participant_pb2.DecisionReply(state=TXN_STATES[state])

    def GetDecision(self, request, context):
        _require(context, request.txn_id)
        row = self._participant.decision(request.txn_id)
        if row is None:
            return participant_pb2.GetDecisionReply(state=participant_pb2.TXN_STATE_UNSPECIFIED)
        return participant_pb2.GetDecisionReply(
            state=TXN_STATES[row["state"]], vote=VOTES_BY_NAME[row["vote"]], term=row["term"]
        )


class GrpcServer:
    """Runs beside the REST API in the same process, on its own threads."""

    def __init__(self, participant: IncidentParticipant, events: IncidentEvents) -> None:
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=8), interceptors=[MetricsInterceptor()])
        participant_pb2_grpc.add_ParticipantServicer_to_server(ParticipantService(participant, events), self._server)
        self._health = health.HealthServicer()
        health_pb2_grpc.add_HealthServicer_to_server(self._health, self._server)
        # Reflection lets grpcurl list and call the methods without the .proto files
        reflection.enable_server_reflection(
            (
                participant_pb2.DESCRIPTOR.services_by_name["Participant"].full_name,
                health.SERVICE_NAME,
                reflection.SERVICE_NAME,
            ),
            self._server,
        )
        self._server.add_insecure_port(f"[::]:{GRPC_PORT}")

    def start(self) -> None:
        self._server.start()
        self._health.set("", health_pb2.HealthCheckResponse.SERVING)
        logger.info("Participant listening on %d", GRPC_PORT)

    def stop(self) -> None:
        # Report not serving first, then let calls in flight finish for up to 5 seconds
        self._health.enter_graceful_shutdown()
        self._server.stop(grace=5).wait()