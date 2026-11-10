"""The latest known position of each train, kept in DynamoDB."""

import os
import time
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

KEEP_SECONDS = 24 * 3600


def _plain(value):
    """DynamoDB returns numbers as Decimal; turn them back into int or float."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    return value


class PositionStore:
    def __init__(self) -> None:
        endpoint_url = os.environ.get("DYNAMODB_ENDPOINT_URL") or None
        dynamodb = boto3.resource(
            "dynamodb",
            region_name=os.environ.get("AWS_REGION", "ap-south-1"),
            endpoint_url=endpoint_url,
        )
        self._table_name = os.environ["POSITIONS_TABLE"]
        if endpoint_url:
            self._create_local_table(dynamodb)
        self._table = dynamodb.Table(self._table_name)

    def _create_local_table(self, dynamodb) -> None:
        """On the laptop there is no Terraform, so create the table if it is missing."""
        try:
            dynamodb.create_table(
                TableName=self._table_name,
                KeySchema=[{"AttributeName": "train_number", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "train_number", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
        except ClientError as error:
            if error.response["Error"]["Code"] != "ResourceInUseException":
                raise

    def save_latest(self, position: dict) -> bool:
        """Store the position unless a newer one is already stored. False means it was older."""
        item = {
            "train_number": position["train_number"],
            "section": position["section"],
            "latitude": Decimal(str(position["latitude"])),
            "longitude": Decimal(str(position["longitude"])),
            "speed_kmph": Decimal(str(position["speed_kmph"])),
            "recorded_at": position["recorded_at"],
            "recorded_at_ms": position["recorded_at_ms"],
            "event_id": position["event_id"],
            "expires_at": int(time.time()) + KEEP_SECONDS,
        }
        try:
            self._table.put_item(
                Item=item,
                ConditionExpression="attribute_not_exists(train_number) OR recorded_at_ms < :recorded",
                ExpressionAttributeValues={":recorded": position["recorded_at_ms"]},
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise
        return True

    def latest(self, train_number: str) -> dict | None:
        item = self._table.get_item(Key={"train_number": train_number}).get("Item")
        if item is None:
            return None
        return {key: _plain(value) for key, value in item.items() if key != "expires_at"}