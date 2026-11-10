"""Leader Election: the gRPC server for Campaign, Heartbeat and GetLeader."""

import logging
import os
import signal
import threading
from concurrent import futures

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from grpc_reflection.v1alpha import reflection
from prometheus_client import Gauge, start_http_server

from app.events import ElectionEvents
from app.lease import LeaseStore
from app.telemetry import MetricsInterceptor, configure_logging
from lineclear import election_pb2, election_pb2_grpc

GRPC_PORT = int(os.environ.get("GRPC_PORT", "50051"))
METRICS_PORT = int(os.environ.get("METRICS_PORT", "9100"))
WATCHED_ELECTION = os.environ.get("WATCHED_ELECTION", "schedule-coordinator")

HAS_LEADER = Gauge("lineclear_election_has_leader", "1 while someone holds a live lease, else 0", ["election"])
TERM = Gauge("lineclear_election_term", "The current or last leader term", ["election"])

logger = logging.getLogger("leader_election")


def _require(context: grpc.ServicerContext, election: str, candidate_id: str | None = None) -> None:
    if not election:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "election is required")
    if candidate_id is not None and not candidate_id:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "candidate_id is required")


class LeaderElectionService(election_pb2_grpc.LeaderElectionServicer):
    def __init__(self, store: LeaseStore) -> None:
        self._store = store

    def Campaign(self, request, context):
        _require(context, request.election, request.candidate_id)
        view = self._store.campaign(request.election, request.candidate_id)
        return election_pb2.CampaignReply(
            is_leader=view["is_leader"], leader_id=view["leader_id"], term=view["term"],
            lease_remaining_ms=view["lease_remaining_ms"],
        )

    def Heartbeat(self, request, context):
        _require(context, request.election, request.candidate_id)
        if request.term < 1:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "term must be 1 or more")
        view = self._store.heartbeat(request.election, request.candidate_id, request.term)
        return election_pb2.HeartbeatReply(
            still_leader=view["is_leader"], leader_id=view["leader_id"], term=view["term"],
            lease_remaining_ms=view["lease_remaining_ms"],
        )

    def GetLeader(self, request, context):
        _require(context, request.election)
        view = self._store.get_leader(request.election)
        return election_pb2.GetLeaderReply(
            has_leader=view["has_leader"], leader_id=view["leader_id"], term=view["term"],
            lease_remaining_ms=view["lease_remaining_ms"],
        )


def watch_lease(store: LeaseStore, stopping: threading.Event) -> None:
    """Every 2 seconds, record whether the watched election has a leader, for the dashboard and alerts."""
    while not stopping.wait(2):
        try:
            view = store.get_leader(WATCHED_ELECTION)
        except Exception:
            logger.exception("Could not read the lease for %s", WATCHED_ELECTION)
            continue
        HAS_LEADER.labels(WATCHED_ELECTION).set(1 if view["has_leader"] else 0)
        TERM.labels(WATCHED_ELECTION).set(view["term"])


def serve() -> None:
    configure_logging()
    events = ElectionEvents()
    store = LeaseStore(events)

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8), interceptors=[MetricsInterceptor()])
    election_pb2_grpc.add_LeaderElectionServicer_to_server(LeaderElectionService(store), server)

    health_servicer = health.HealthServicer()
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    # Reflection lets grpcurl list and call the methods without the .proto file
    reflection.enable_server_reflection(
        (
            election_pb2.DESCRIPTOR.services_by_name["LeaderElection"].full_name,
            health.SERVICE_NAME,
            reflection.SERVICE_NAME,
        ),
        server,
    )

    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    start_http_server(METRICS_PORT)
    server.start()

    stopping = threading.Event()
    threading.Thread(target=watch_lease, args=(store, stopping), daemon=True).start()

    health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
    logger.info("Leader Election listening on %d, metrics on %d", GRPC_PORT, METRICS_PORT)

    def stop(signum, frame):
        # Report not serving first, so Kubernetes stops sending calls, then finish the ones in flight
        health_servicer.enter_graceful_shutdown()
        stopping.set()
        server.stop(grace=5)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    server.wait_for_termination()
    events.close()


if __name__ == "__main__":
    serve()