"""The election lease: one DynamoDB item per election, changed only by conditional writes.

A conditional write succeeds only if the item is still in the state the writer expects, so two
candidates racing for the same lease can never both win: DynamoDB applies one write and refuses
the other.
"""

import logging
import os
import time

import boto3
from botocore.exceptions import ClientError
from prometheus_client import Counter

from app.events import ElectionEvents

LEASE_MS = 6000  # a leader that has not renewed for 6 seconds is presumed gone

ELECTIONS = Counter("lineclear_elections_total", "Times a candidate took the lease with a new term", ["election"])

logger = logging.getLogger("leader_election")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _condition_failed(error: ClientError) -> bool:
    return error.response["Error"]["Code"] == "ConditionalCheckFailedException"


def _view(item: dict | None, now: int, candidate_id: str = "") -> dict:
    """The lease as callers see it. The last term is kept even when nobody holds the lease."""
    live = item is not None and int(item["expires_at_ms"]) > now
    return {
        "has_leader": live,
        "is_leader": live and item["holder"] == candidate_id,
        "leader_id": item["holder"] if live else "",
        "term": int(item.get("term", 0)) if item else 0,
        "lease_remaining_ms": int(item["expires_at_ms"]) - now if live else 0,
    }


class LeaseStore:
    def __init__(self, events: ElectionEvents) -> None:
        endpoint_url = os.environ.get("DYNAMODB_ENDPOINT_URL") or None  # set only for local testing
        dynamodb = boto3.resource(
            "dynamodb",
            region_name=os.environ.get("AWS_REGION", "ap-south-1"),
            endpoint_url=endpoint_url,
        )
        self._table_name = os.environ["LEASE_TABLE"]
        if endpoint_url:
            self._create_local_table(dynamodb)
        self._table = dynamodb.Table(self._table_name)
        self._events = events

    def _create_local_table(self, dynamodb) -> None:
        """On the laptop there is no Terraform, so create the table if it is missing."""
        try:
            dynamodb.create_table(
                TableName=self._table_name,
                KeySchema=[{"AttributeName": "election", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "election", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceInUseException":
                raise

    def campaign(self, election: str, candidate_id: str) -> dict:
        now = _now_ms()

        # 1. Already the leader: extend the lease and keep the term
        try:
            item = self._table.update_item(
                Key={"election": election},
                UpdateExpression="SET expires_at_ms = :expires, renewed_at_ms = :now",
                ConditionExpression="holder = :me AND expires_at_ms > :now",
                ExpressionAttributeValues={":me": candidate_id, ":now": now, ":expires": now + LEASE_MS},
                ReturnValues="ALL_NEW",
            )["Attributes"]
            return _view(item, now, candidate_id)
        except ClientError as error:
            if not _condition_failed(error):
                raise

        # 2. Nobody holds a live lease: take it, and the term goes up by one
        try:
            previous = self._table.update_item(
                Key={"election": election},
                UpdateExpression=(
                    "SET holder = :me, expires_at_ms = :expires, acquired_at_ms = :now, renewed_at_ms = :now "
                    "ADD term :one"
                ),
                ConditionExpression="attribute_not_exists(election) OR expires_at_ms <= :now",
                ExpressionAttributeValues={":me": candidate_id, ":now": now, ":expires": now + LEASE_MS, ":one": 1},
                ReturnValues="ALL_OLD",
            ).get("Attributes")
        except ClientError as error:
            if not _condition_failed(error):
                raise
            # 3. Someone else holds a live lease
            return self.get_leader(election, candidate_id)

        term = int(previous.get("term", 0)) + 1 if previous else 1
        previous_leader = previous.get("holder") if previous else None
        leaderless_ms = now - int(previous["expires_at_ms"]) if previous else None
        ELECTIONS.labels(election).inc()
        logger.info(
            "%s took the lease for %s with term %d", candidate_id, election, term,
            extra={"election": election, "term": term, "leader_id": candidate_id,
                   "previous_leader_id": previous_leader, "leaderless_ms": leaderless_ms},
        )
        self._events.leader_elected(election, candidate_id, term, previous_leader, leaderless_ms)
        return {"has_leader": True, "is_leader": True, "leader_id": candidate_id, "term": term,
                "lease_remaining_ms": LEASE_MS}

    def heartbeat(self, election: str, candidate_id: str, term: int) -> dict:
        now = _now_ms()
        try:
            item = self._table.update_item(
                Key={"election": election},
                UpdateExpression="SET expires_at_ms = :expires, renewed_at_ms = :now",
                ConditionExpression="holder = :me AND term = :term AND expires_at_ms > :now",
                ExpressionAttributeValues={":me": candidate_id, ":term": term, ":now": now,
                                           ":expires": now + LEASE_MS},
                ReturnValues="ALL_NEW",
            )["Attributes"]
            return _view(item, now, candidate_id)
        except ClientError as error:
            if not _condition_failed(error):
                raise
        logger.warning(
            "Heartbeat from %s with term %d refused: the lease has passed on or ran out", candidate_id, term,
            extra={"election": election, "term": term},
        )
        view = self.get_leader(election, candidate_id)
        view["is_leader"] = False  # the caller must campaign again to learn the current term
        return view

    def get_leader(self, election: str, candidate_id: str = "") -> dict:
        item = self._table.get_item(Key={"election": election}, ConsistentRead=True).get("Item")
        return _view(item, _now_ms(), candidate_id)