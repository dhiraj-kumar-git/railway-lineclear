"""Flink's window results, kept in DynamoDB under their result key."""

import os
import time
from decimal import Decimal

import boto3

KEEP_SECONDS = 7 * 24 * 3600  # DynamoDB deletes a result a week after it was written
COUNT_FIELDS = ("trains", "updates", "incidents", "elections", "leaderless_ms", "commits", "aborts")
TEXT_FIELDS = ("metric", "section", "window_start", "window_end", "train_number", "recorded_at")


class ResultsStore:
    def __init__(self) -> None:
        endpoint = os.environ.get("DYNAMODB_ENDPOINT_URL")  # set only on the laptop
        resource = boto3.resource("dynamodb", region_name=os.environ["AWS_REGION"], endpoint_url=endpoint)
        self._table = resource.Table(os.environ["RESULTS_TABLE"])
        if endpoint:
            self._create_table_locally(resource)

    def _create_table_locally(self, resource) -> None:
        """On the laptop there is no Terraform, so the table is created here."""
        names = [table.name for table in resource.tables.all()]
        if self._table.name not in names:
            resource.create_table(
                TableName=self._table.name,
                KeySchema=[{"AttributeName": "result_key", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "result_key", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            ).wait_until_exists()

    def save(self, result: dict) -> None:
        """Writes one result. The same result written again after a Flink restart replaces itself."""
        item = {"result_key": result["result_key"], "expires_at": int(time.time()) + KEEP_SECONDS}
        for field in TEXT_FIELDS:
            if result.get(field) is not None:
                item[field] = str(result[field])
        for field in COUNT_FIELDS:
            if result.get(field) is not None:
                item[field] = Decimal(int(result[field]))
        self._table.put_item(Item=item)

    def ready(self) -> bool:
        try:
            self._table.load()
        except Exception:  # any failure means the table cannot be used yet
            return False
        return True
