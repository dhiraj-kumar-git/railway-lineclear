"""The gRPC side of Schedule Manager: Coordinator.GetDecision, health and reflection."""

import logging
import os
from concurrent import futures

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection

from app.diversions import DiversionLog
from app.telemetry import MetricsInterceptor
from lineclear import participant_pb2, participant_pb2_grpc

GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))

# PREPARING means the decision is not made yet: ask again later
STATES = {
    "PREPARING": participant_pb2.TXN_STATE_PREPARED,
    "COMMIT": participant_pb2.TXN_STATE_COMMITTED,
    "ABORT": participant_pb2.TXN_STATE_ABORTED,
}

logger = logging.getLogger("schedule_manager")


class CoordinatorService(participant_pb2_grpc.CoordinatorServicer):
    def __init__(self, log: DiversionLog) -> None:
        self._log = log

    def GetDecision(self, request, context):
        if not request.txn_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "txn_id is required")
        row = self._log.decision(request.txn_id)
        if row is None:
            # Presumed abort: a transaction the coordinator has no record of was never committed
            return participant_pb2.GetDecisionReply(state=participant_pb2.TXN_STATE_ABORTED)
        return participant_pb2.GetDecisionReply(state=STATES[row["state"]], term=row["term"])


class GrpcServer:
    """Runs beside the REST API in the same process, on its own threads. Every replica answers."""

    def __init__(self, log: DiversionLog) -> None:
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), interceptors=[MetricsInterceptor()])
        participant_pb2_grpc.add_CoordinatorServicer_to_server(CoordinatorService(log), self._server)
        self._health = health.HealthServicer()
        health_pb2_grpc.add_HealthServicer_to_server(self._health, self._server)
        reflection.enable_server_reflection(
            (
                participant_pb2.DESCRIPTOR.services_by_name["Coordinator"].full_name,
                health.SERVICE_NAME,
                reflection.SERVICE_NAME,
            ),
            self._server,
        )
        self._server.add_insecure_port(f"[::]:{GRPC_PORT}")

    def start(self) -> None:
        self._server.start()
        self._health.set("", health_pb2.HealthCheckResponse.SERVING)
        logger.info("Coordinator.GetDecision listening on %d", GRPC_PORT)

    def stop(self) -> None:
        self._health.enter_graceful_shutdown()
        self._server.stop(grace=5).wait()