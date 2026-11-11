"""gRPC clients for the two-phase commit participants: Track Availability and Incident Manager.

Every call carries the transaction ID as gRPC metadata (txn-id) and a deadline, so a participant
that does not answer in time cannot hold the coordinator up.
"""

import grpc

from lineclear import participant_pb2, participant_pb2_grpc

PREPARE_DEADLINE_SECONDS = 2.0
DECISION_DEADLINE_SECONDS = 2.0


class ParticipantClient:
    def __init__(self, name: str, target: str) -> None:
        self.name = name
        self._channel = grpc.insecure_channel(target)
        self._stub = participant_pb2_grpc.ParticipantStub(self._channel)

    def prepare(self, txn_id: str, term: int, coordinator_id: str, plan: dict) -> tuple[str, str]:
        """Ask for a vote. No answer within the deadline, or any error, counts as NO."""
        try:
            reply = self._stub.Prepare(
                participant_pb2.PrepareRequest(
                    txn_id=txn_id, term=term, coordinator_id=coordinator_id,
                    plan=participant_pb2.DiversionPlan(**plan),
                ),
                timeout=PREPARE_DEADLINE_SECONDS,
                metadata=(("txn-id", txn_id),),
            )
        except grpc.RpcError as error:
            return "NO", f"{self.name} did not vote: {error.code().name}: {error.details()}"
        if reply.vote == participant_pb2.VOTE_YES:
            return "YES", ""
        return "NO", reply.reason or f"{self.name} voted no"

    def decide(self, decision: str, txn_id: str, term: int, coordinator_id: str) -> None:
        """Send Commit or Abort. Returns when acknowledged; raises grpc.RpcError otherwise."""
        request = participant_pb2.DecisionRequest(txn_id=txn_id, term=term, coordinator_id=coordinator_id)
        send = self._stub.Commit if decision == "COMMIT" else self._stub.Abort
        send(request, timeout=DECISION_DEADLINE_SECONDS, metadata=(("txn-id", txn_id),))

    def close(self) -> None:
        self._channel.close()