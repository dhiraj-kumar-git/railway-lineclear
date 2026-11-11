"""Transaction outcomes on the coordination.events Kafka topic, keyed by transaction ID.

Operations Analytics counts commits and aborts from these. Sending does not wait for Kafka.
"""

import json
import logging
import os
from datetime import datetime, timezone

from confluent_kafka import Producer

TOPIC = "coordination.events"

logger = logging.getLogger("schedule_manager")


def kafka_settings(client_id: str) -> dict:
    settings = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        "client.id": client_id,
    }
    if settings["security.protocol"] == "SSL":
        settings["ssl.ca.location"] = "/etc/ssl/certs/ca-certificates.crt"
    return settings


class CoordinationEvents:
    def __init__(self) -> None:
        self._producer = Producer(kafka_settings("schedule-manager") | {"enable.idempotence": True})

    def transaction_decided(self, txn: dict, decision: str, reason: str | None, prepare_ms: int) -> None:
        event = {
            "event_type": "transaction_decided",
            "txn_id": txn["txn_id"],
            "decision": decision,
            "reason": reason,
            "term": txn["term"],
            "coordinator_id": txn["coordinator_id"],
            "incident_id": txn["incident_id"],
            "train_number": txn["train_number"],
            "blocked_section": txn["blocked_section"],
            "loop_line": txn["loop_line"],
            "prepare_ms": prepare_ms,  # how long phase 1 took
            "occurred_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        }
        self._producer.produce(
            TOPIC, key=txn["txn_id"], value=json.dumps(event).encode(),
            headers=[("txn-id", txn["txn_id"].encode())], on_delivery=self._delivered,
        )
        self._producer.poll(0)

    @staticmethod
    def _delivered(error, message) -> None:
        if error is not None:
            logger.error("Event for %s was not delivered: %s", message.key().decode(), error)

    def close(self) -> None:
        self._producer.flush(5)