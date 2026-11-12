"""Track Availability: the gRPC server for the two-phase commit participant and the track view."""

import logging
import os
import signal
from concurrent import futures

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection
from prometheus_client import Counter, start_http_server

from app.db import apply_schema, create_pool
from app.participant import DecisionConflict, StaleTerm, TrackParticipant, UnknownTransaction
from app.recovery import InDoubtRecovery
from app.telemetry import MetricsInterceptor, configure_logging
from lineclear import participant_pb2, participant_pb2_grpc, track_pb2, track_pb2_grpc

GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9100"))

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

logger = logging.getLogger("track_availability")


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
    def __init__(self, participant: TrackParticipant) -> None:
        self._participant = participant

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
            state, changed = self._participant.commit(request.txn_id, request.term)
        except StaleTerm as error:
            _refuse_stale(context, error, request.txn_id)
        except UnknownTransaction as error:
            context.abort(grpc.StatusCode.NOT_FOUND, str(error))
        except DecisionConflict as error:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, str(error))
        if changed:
            DECISIONS.labels("committed").inc()
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


class TrackStatusService(track_pb2_grpc.TrackStatusServicer):
    def __init__(self, participant: TrackParticipant) -> None:
        self._participant = participant

    def ListSegments(self, request, context):
        rows, highest_term = self._participant.segments(request.held_only)
        segments = [
            track_pb2.Segment(
                code=row["code"],
                kind=row["kind"],
                hold=row["hold_type"] or "none",
                hold_state=row["hold_state"] or "",
                txn_id=row["txn_id"] or "",
                train_number=row["train_number"] or "",
            )
            for row in rows
        ]
        return track_pb2.ListSegmentsReply(segments=segments, highest_term=highest_term)

    def ReleaseAllHolds(self, request, context):
        holds, aborted = self._participant.release_all_holds()
        return track_pb2.ReleaseAllHoldsReply(released_holds=holds, aborted_transactions=aborted)


def serve() -> None:
    configure_logging()
    pool = create_pool()
    pool.open(wait=True, timeout=30)
    apply_schema(pool)
    participant = TrackParticipant(pool)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8), interceptors=[MetricsInterceptor()])
    participant_pb2_grpc.add_ParticipantServicer_to_server(ParticipantService(participant), server)
    track_pb2_grpc.add_TrackStatusServicer_to_server(TrackStatusService(participant), server)

    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    # Reflection lets grpcurl list and call the methods without the .proto files
    reflection.enable_server_reflection(
        (
            participant_pb2.DESCRIPTOR.services_by_name["Participant"].full_name,
            track_pb2.DESCRIPTOR.services_by_name["TrackStatus"].full_name,
            health.SERVICE_NAME,
            reflection.SERVICE_NAME,
        ),
        server,
    )

    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    start_http_server(METRICS_PORT)
    server.start()
    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    logger.info("Track Availability listening on %d, metrics on %d", GRPC_PORT, METRICS_PORT)

    # Settle anything left PREPARED by a previous run, by asking the coordinator's log
    recovery = None
    if os.environ.get("COORDINATOR_TARGET"):
        recovery = InDoubtRecovery(participant, os.environ["COORDINATOR_TARGET"])
        recovery.start()

    def stop(signum, frame):
        # Report not serving first, so Kubernetes stops sending calls, then finish the ones in flight
        health_servicer.enter_graceful_shutdown()
        server.stop(grace=5)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    server.wait_for_termination()
    if recovery is not None:
        recovery.stop()
    pool.close()


if __name__ == "__main__":
    serve()