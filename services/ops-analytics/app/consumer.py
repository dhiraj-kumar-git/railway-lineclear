"""Reads Flink's results from the ops.results Kafka topic into DynamoDB and the metrics."""

import json
import logging
import os
import threading
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.admin import AdminClient

from .metrics import REJECTED, LatestResults
from .results_store import ResultsStore

TOPIC = "ops.results"
METRICS = ("section_traffic", "late_position", "section_incidents", "coordination")
logger = logging.getLogger("ops_analytics")


def connection_settings() -> dict:
    settings = {
        "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "security.protocol": os.environ.get("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
        "client.id": "ops-analytics",
    }
    if settings["security.protocol"] == "SSL":
        settings["ssl.ca.location"] = "/etc/ssl/certs/ca-certificates.crt"
    return settings


def kafka_settings() -> dict:
    return connection_settings() | {
        "group.id": "ops-analytics-reader",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # offsets are committed only after the result is stored
    }


def parse(raw: bytes) -> dict:
    """One message from ops.results, checked enough to store and chart it."""
    result = json.loads(raw)
    if not isinstance(result, dict) or result.get("metric") not in METRICS or not result.get("result_key"):
        raise ValueError("not an operations result")
    if result["metric"] != "late_position":
        datetime.fromisoformat(result["window_start"].replace("Z", "+00:00"))
    return result


class ResultsReader:
    def __init__(self, store: ResultsStore, latest: LatestResults) -> None:
        self._store = store
        self._latest = latest
        self._consumer = Consumer(kafka_settings())
        # The readiness check asks Kafka through its own client: a consumer that one thread is
        # polling does not answer list_topics from another thread, it waits with no time limit
        self._admin = AdminClient(connection_settings())
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="results-reader", daemon=True)
        self.assigned = False

    def start(self) -> None:
        self._consumer.subscribe([TOPIC], on_assign=self._on_assign)
        self._thread.start()

    def _on_assign(self, consumer, partitions) -> None:
        self.assigned = True
        logger.info("Reading %s, %d partition(s)", TOPIC, len(partitions))

    def handle(self, raw: bytes) -> None:
        try:
            result = parse(raw)
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            REJECTED.inc()
            logger.warning("Skipped a message on %s: %s", TOPIC, error)
            return
        self._store.save(result)
        self._latest.record(result)
        if result.get("window_end"):
            end = datetime.fromisoformat(result["window_end"].replace("Z", "+00:00"))
            self._latest.window_closed(result["metric"], end.timestamp())
        logger.info(
            "Stored %s",
            result["result_key"],
            extra={"metric": result["metric"], "section": result.get("section")},
        )

    def _run(self) -> None:
        while not self._stop.is_set():
            message = self._consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka error: %s", message.error())
                continue
            try:
                self.handle(message.value())
            except Exception:
                # Not stored, so the offset is not committed: the result is read again after a restart
                logger.exception("Could not store a result; stopping so the pod restarts")
                self._stop.set()
                break
            self._consumer.commit(message=message, asynchronous=True)

    def alive(self) -> bool:
        return self._thread.is_alive()

    def kafka_ready(self) -> bool:
        try:
            metadata = self._admin.list_topics(TOPIC, timeout=3)
        except KafkaException:
            return False
        topic = metadata.topics.get(TOPIC)
        return topic is not None and topic.error is None

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self._consumer.close()
