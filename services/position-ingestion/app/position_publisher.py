"""Publishes position updates to the train.positions Kafka topic, keyed by train number."""

import json
import logging
import os

from confluent_kafka import KafkaException, Producer

TOPIC = "train.positions"

logger = logging.getLogger("position_ingestion")


class PositionPublisher:
    def __init__(self) -> None:
        settings = {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "security.protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
            "client.id": "position-ingestion",
            "enable.idempotence": True,
            "linger.ms": 10,
        }
        if settings["security.protocol"] == "SSL":
            settings["ssl.ca.location"] = "/etc/ssl/certs/ca-certificates.crt"
        self._producer = Producer(settings)

    def publish(self, position: dict) -> None:
        self._producer.produce(
            TOPIC,
            key=position["train_number"],
            value=json.dumps(position).encode(),
            on_delivery=self._delivered,
        )
        self._producer.poll(0)

    @staticmethod
    def _delivered(error, message) -> None:
        if error is not None:
            logger.error("Position for train %s was not delivered: %s", message.key().decode(), error)

    def ready(self) -> bool:
        """True when a broker answers and the topic exists."""
        try:
            metadata = self._producer.list_topics(TOPIC, timeout=3)
        except KafkaException:
            return False
        topic = metadata.topics.get(TOPIC)
        return topic is not None and topic.error is None

    def close(self) -> None:
        self._producer.flush(10)