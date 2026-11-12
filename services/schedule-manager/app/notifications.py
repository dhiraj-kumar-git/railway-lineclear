"""Committed diversions to the passenger notification queue (SQS), for the Passenger Notification Lambda.

A notice is sent only after COMMIT is in the log, and the log row's notified_at records that it was
handed over. A new leader sends any COMMIT without one, so a crash cannot lose a notice; it can send
one twice, which the Lambda ignores because it keys notices by transaction ID.

The queue is found by its name, so no account number is written in Git. Without
NOTIFICATION_QUEUE_NAME (on the laptop) notices are skipped.
"""

import json
import logging
import os

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

logger = logging.getLogger("schedule_manager")


def _iso(value) -> str | None:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z") if value else None


class PassengerNotices:
    def __init__(self) -> None:
        self._queue_name = os.environ.get("NOTIFICATION_QUEUE_NAME") or None
        self._queue_url = None
        self._sqs = None
        if self._queue_name:
            self._sqs = boto3.client(
                "sqs",
                region_name=os.environ.get("AWS_REGION", "ap-south-1"),
                endpoint_url=os.environ.get("SQS_ENDPOINT_URL") or None,  # set only for local testing
                config=Config(connect_timeout=2, read_timeout=5, retries={"max_attempts": 3, "mode": "standard"}),
            )

    def send(self, txn: dict) -> bool:
        """Hand one committed diversion to the queue. False if it could not be sent; it is sent again later."""
        txn_id = txn["txn_id"]
        if self._sqs is None:
            logger.info("No notification queue here; passenger notice for train %s skipped", txn["train_number"],
                        extra={"txn_id": txn_id})
            return True
        notice = {
            "txn_id": txn_id,
            "incident_id": txn["incident_id"],
            "train_number": txn["train_number"],
            "blocked_section": txn["blocked_section"],
            "loop_line": txn["loop_line"],
            "decided_at": _iso(txn["decided_at"]),
            "revised_stops": txn["revised_stops"],
        }
        try:
            if self._queue_url is None:
                self._queue_url = self._sqs.get_queue_url(QueueName=self._queue_name)["QueueUrl"]
            self._sqs.send_message(
                QueueUrl=self._queue_url,
                MessageBody=json.dumps(notice),
                MessageAttributes={"txn-id": {"DataType": "String", "StringValue": txn_id}},
            )
        except (BotoCoreError, ClientError) as error:
            logger.error("Passenger notice not sent: %s", error, extra={"txn_id": txn_id})
            return False
        logger.info("Passenger notice for train %s sent to %s", txn["train_number"], self._queue_name,
                    extra={"txn_id": txn_id})
        return True
