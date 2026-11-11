"""Reads incident.events while this replica is the coordinator, and hands each report to it.

The offset is committed only after the diversion has been handled (at-least-once). If leadership
is lost first, the consumer closes without committing, and the next leader reads the same event.
A repeated event is harmless: the coordinator finds the transaction already logged for the incident.
"""

import json
import logging
import threading

from confluent_kafka import Consumer, KafkaError

from app.coordinator import Coordinator, NotLeader
from app.events import kafka_settings
from app.leadership import Leadership

TOPIC = "incident.events"

logger = logging.getLogger("schedule_manager")


class IncidentConsumer:
    def __init__(self, leadership: Leadership, coordinator: Coordinator) -> None:
        self._leadership = leadership
        self._coordinator = coordinator
        self._stopping = threading.Event()
        self._thread = threading.Thread(target=self._run, name="incident-consumer", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stopping.set()
        self._thread.join(timeout=10)

    def _open(self) -> Consumer:
        settings = kafka_settings("schedule-manager") | {
            "group.id": "schedule-manager",
            "enable.auto.commit": False,  # committed by hand, after the work is done
            "auto.offset.reset": "earliest",  # incidents reported before the first leader started still count
            "session.timeout.ms": 10000,
        }
        consumer = Consumer(settings)
        consumer.subscribe([TOPIC])
        logger.info("Reading %s as the coordinator", TOPIC)
        return consumer

    def _run(self) -> None:
        consumer = None
        while not self._stopping.is_set():
            if not self._leadership.term_if_leader():
                if consumer is not None:
                    consumer.close()
                    consumer = None
                    logger.info("Stopped reading %s: not the coordinator", TOPIC)
                self._stopping.wait(1)
                continue
            if consumer is None:
                consumer = self._open()

            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    logger.warning("Kafka: %s", message.error())
                continue

            try:
                event = json.loads(message.value())
            except ValueError:
                logger.error("Skipped a message that is not JSON at offset %d", message.offset())
                consumer.commit(message=message, asynchronous=False)
                continue

            if event.get("event_type") == "incident_reported":
                try:
                    self._coordinator.handle_incident(event)
                except NotLeader:
                    # Leave the offset uncommitted: the next leader handles this incident
                    consumer.close()
                    consumer = None
                    continue
                except Exception:
                    logger.exception("Could not handle incident %s; it will be read again", event.get("incident_id"))
                    consumer.close()  # reopening starts again from the last committed offset
                    consumer = None
                    self._stopping.wait(5)
                    continue
            consumer.commit(message=message, asynchronous=False)

        if consumer is not None:
            consumer.close()