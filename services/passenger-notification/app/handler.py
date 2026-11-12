"""Passenger Notification: turns committed diversions into passenger notices, on AWS Lambda.

Two ways in, one function:
- SQS trigger: each message is one committed diversion from Schedule Manager. The function writes the
  notice (revised arrival time per station) to DynamoDB, keyed by transaction ID, and a plain-text bulletin
  to S3, and logs the SMS it would send. A message it cannot use is reported as failed; after three
  tries SQS moves it to the dead-letter queue.
- Function URL (HTTPS, read-only): GET /notices/<train number> lists a train's notices, newest first;
  GET /bulletins/<transaction ID> returns one bulletin.

Notices are written only if absent, so a message delivered twice changes nothing.
"""

import json
import logging
import os
import re
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

NOTICES_TABLE = os.environ["NOTICES_TABLE"]
BULLETINS_BUCKET = os.environ["BULLETINS_BUCKET"]
TRAIN_INDEX = "by-train"

TXN_ID = re.compile(r"^T-\d+$")
TRAIN_NUMBER = re.compile(r"^\d{5}$")
STOP_FIELDS = ("station_code", "station_name", "planned_arrival", "revised_arrival", "delay_minutes")

logger = logging.getLogger()
logger.setLevel(logging.INFO)

notices = boto3.resource("dynamodb").Table(NOTICES_TABLE)
s3 = boto3.client("s3")


class BadNotice(ValueError):
    """The message is not a committed diversion this function can use."""


def handle(event, context):
    if "Records" in event:
        return _from_queue(event["Records"])
    return _from_url(event)


def _log(message: str, **fields) -> None:
    """One JSON line per event, so the logs can be searched by field like the services' logs."""
    logger.info(json.dumps({"message": message, **fields}))


# SQS trigger

def _from_queue(records: list) -> dict:
    failures = []
    for record in records:
        try:
            _notify(_parse(record["body"]))
        except BadNotice as error:
            _log("Message cannot be used; it goes back to the queue", reason=str(error),
                 message_id=record["messageId"], receive_count=record["attributes"].get("ApproximateReceiveCount"))
            failures.append({"itemIdentifier": record["messageId"]})
        except ClientError as error:
            _log("AWS call failed; the message goes back to the queue", reason=str(error),
                 message_id=record["messageId"])
            failures.append({"itemIdentifier": record["messageId"]})
    # Only the failed messages return to the queue; the rest are deleted
    return {"batchItemFailures": failures}


def _parse(body: str) -> dict:
    try:
        notice = json.loads(body)
    except ValueError:
        raise BadNotice("the body is not JSON")
    if not isinstance(notice, dict):
        raise BadNotice("the body is not a JSON object")
    if not TXN_ID.match(str(notice.get("txn_id", ""))):
        raise BadNotice("txn_id is missing or not like T-101")
    if not TRAIN_NUMBER.match(str(notice.get("train_number", ""))):
        raise BadNotice("train_number is missing or not five digits")
    for field in ("incident_id", "blocked_section", "loop_line", "decided_at"):
        if not notice.get(field):
            raise BadNotice(f"{field} is missing")
    stops = notice.get("revised_stops")
    if not isinstance(stops, list) or not stops:
        raise BadNotice("revised_stops is missing or empty")
    for stop in stops:
        if not isinstance(stop, dict) or any(field not in stop for field in STOP_FIELDS):
            raise BadNotice("a stop in revised_stops lacks a field")
    return notice


def _bulletin(notice: dict) -> str:
    lines = [
        f"Railway LineClear passenger bulletin {notice['txn_id']}",
        "",
        f"Train {notice['train_number']} is diverted through loop line {notice['loop_line']} "
        f"while block section {notice['blocked_section']} is closed (incident {notice['incident_id']}).",
        "Revised arrival times:",
    ]
    for stop in notice["revised_stops"]:
        lines.append(
            f"  {stop['station_name']} ({stop['station_code']}): {stop['revised_arrival']}, "
            f"was {stop['planned_arrival']}, {stop['delay_minutes']} min late"
        )
    lines += ["", f"Issued {notice['decided_at']}. We regret the inconvenience."]
    return "\n".join(lines) + "\n"


def _notify(notice: dict) -> None:
    txn_id, train = notice["txn_id"], notice["train_number"]
    bulletin_key = f"bulletins/{txn_id}.txt"
    try:
        notices.put_item(
            Item={
                "txn_id": txn_id,
                "train_number": train,
                "incident_id": notice["incident_id"],
                "blocked_section": notice["blocked_section"],
                "loop_line": notice["loop_line"],
                "decided_at": notice["decided_at"],
                "stations": [{field: stop[field] for field in STOP_FIELDS} for stop in notice["revised_stops"]],
                "bulletin_key": bulletin_key,
            },
            ConditionExpression="attribute_not_exists(txn_id)",
        )
    except ClientError as error:
        if error.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        _log("Notice already written; repeated message ignored", txn_id=txn_id, train_number=train)
        return
    s3.put_object(Bucket=BULLETINS_BUCKET, Key=bulletin_key, Body=_bulletin(notice).encode(),
                  ContentType="text/plain; charset=utf-8")
    first = notice["revised_stops"][0]
    # SMS is simulated: sending to real numbers in India needs DLT registration
    _log("SMS to passengers (simulated)", txn_id=txn_id, train_number=train,
         text=f"Train {train} diverted via {notice['loop_line']}; now due at {first['station_name']} "
              f"{first['revised_arrival']} ({first['delay_minutes']} min late).")
    _log("Notice written", txn_id=txn_id, train_number=train, stations=len(notice["revised_stops"]),
         bulletin_key=bulletin_key)


# Function URL

def _plain(value):
    """DynamoDB returns numbers as Decimal; JSON wants int."""
    return int(value) if isinstance(value, Decimal) else str(value)


def _reply(status: int, body, content_type: str = "application/json") -> dict:
    text = body if isinstance(body, str) else json.dumps(body, indent=2, default=_plain)
    return {"statusCode": status, "headers": {"Content-Type": content_type}, "body": text}


def _from_url(event: dict) -> dict:
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")
    if method != "GET":
        return _reply(405, {"detail": "Only GET is allowed"})
    parts = [part for part in event.get("rawPath", "/").split("/") if part]

    if len(parts) == 2 and parts[0] == "notices" and TRAIN_NUMBER.match(parts[1]):
        items = notices.query(
            IndexName=TRAIN_INDEX, KeyConditionExpression=Key("train_number").eq(parts[1]), ScanIndexForward=False
        )["Items"]
        if not items:
            return _reply(404, {"detail": f"No notices for train {parts[1]}"})
        return _reply(200, items)

    if len(parts) == 2 and parts[0] == "bulletins" and TXN_ID.match(parts[1]):
        try:
            obj = s3.get_object(Bucket=BULLETINS_BUCKET, Key=f"bulletins/{parts[1]}.txt")
        except ClientError as error:
            if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return _reply(404, {"detail": f"No bulletin for {parts[1]}"})
            raise
        return _reply(200, obj["Body"].read().decode(), "text/plain; charset=utf-8")

    return _reply(404, {"detail": "Try /notices/12951 or /bulletins/T-101"})
