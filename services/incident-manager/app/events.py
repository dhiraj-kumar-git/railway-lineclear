"""Incident events on the incident.events Kafka topic, keyed by incident number.

The key keeps every event for one incident in one partition, so readers see them in order.
Events are sent after the database change is committed and without waiting for Kafka.
"""

import json
import logging
import os
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer

TOPIC = "incident.events"

logger = logging.getLogger("incident_manager")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class IncidentEvents:
    def __init__(self) -> None:
        settings = {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "security.protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            "client.id": "incident-manager",
            "enable.idempotence": True,  # a retried send is never written twice or out of order
        }
        if settings["security.protocol"] == "SSL":
            settings["ssl.ca.location"] = "/etc/ssl/certs/ca-certificates.crt"
        self._producer = Producer(settings)

    def _send(self, event: dict, txn_id: str | None = None) -> None:
        headers = [("txn-id", txn_id.encode())] if txn_id else None
        self._producer.produce(
            TOPIC, key=event["incident_id"], value=json.dumps(event, default=str).encode(),
            headers=headers, on_delivery=self._delivered,
        )
        self._producer.poll(0)  # runs delivery reports for earlier events

    def incident_reported(self, incident: dict) -> None:
        reported_at = incident["reported_at"].astimezone(timezone.utc)
        self._send({
            "event_type": "incident_reported",
            "incident_id": incident["incident_id"],
            "kind": incident["kind"],
            "section": incident["section"],
            "train_number": incident["train_number"],
            "description": incident["description"],
            "reported_at": reported_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        })

    def status_changed(self, incident_id: str, status: str, note: str, txn_id: str | None = None) -> None:
        self._send(
            {
                "event_type": "incident_status_changed",
                "incident_id": incident_id,
                "status": status,
                "note": note,
                "txn_id": txn_id,
                "occurred_at": _now(),
            },
            txn_id,
        )

    def ready(self) -> bool:
        """True when a broker answers and the topic exists."""
        try:
            metadata = self._producer.list_topics(TOPIC, timeout=3)
        except KafkaException:
            return False
        topic = metadata.topics.get(TOPIC)
        return topic is not None and topic.error is None

    @staticmethod
    def _delivered(error, message) -> None:
        if error is not None:
            logger.error("Event for %s was not delivered: %s", message.key().decode(), error)

    def close(self) -> None:
        self._producer.flush(5)