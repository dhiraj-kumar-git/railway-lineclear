"""Election events on the coordination.events Kafka topic, keyed by election name.

Sending is asynchronous: an election never waits for Kafka, so a Kafka outage cannot stop
a new leader being chosen.
"""

import json
import logging
import os
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "coordination.events"

logger = logging.getLogger("leader_election")


class ElectionEvents:
    def __init__(self) -> None:
        settings = {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "security.protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            "client.id": "leader-election",
            "enable.idempotence": True,  # a retried send is never written twice
        }
        if settings["security.protocol"] == "SSL":
            settings["ssl.ca.location"] = "/etc/ssl/certs/ca-certificates.crt"
        self._producer = Producer(settings)

    def leader_elected(
        self, election: str, leader_id: str, term: int, previous_leader_id: str | None, leaderless_ms: int | None
    ) -> None:
        event = {
            "event_type": "leader_elected",
            "election": election,
            "leader_id": leader_id,
            "term": term,
            "previous_leader_id": previous_leader_id,
            "leaderless_ms": leaderless_ms,  # how long the division had no duty controller
            "occurred_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        }
        self._producer.produce(TOPIC, key=election, value=json.dumps(event).encode(), on_delivery=self._delivered)
        self._producer.poll(0)  # runs delivery reports for earlier events

    @staticmethod
    def _delivered(error, message) -> None:
        if error is not None:
            logger.error("Election event for %s was not delivered: %s", message.key().decode(), error)

    def close(self) -> None:
        self._producer.flush(5)