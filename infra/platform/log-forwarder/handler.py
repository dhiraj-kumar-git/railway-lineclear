"""Forwards Lambda log lines from CloudWatch Logs into Loki.

CloudWatch Logs calls this function with each new batch from a subscribed log group: base64 text of
gzip-compressed JSON. Every line is pushed to Loki's push API, so the Lambda's logs can be searched
next to the pods' logs, for example by txn_id.
"""

import base64
import gzip
import json
import os
import urllib.request

LOKI_PUSH_URL = os.environ["LOKI_PUSH_URL"]


def log_line(message: str) -> str:
    """The JSON part of a line written through Python's logging, so Loki's json filter can read it.

    The Lambda runtime writes such lines as "[INFO]<tab>time<tab>request id<tab>{...}". Lines with
    no JSON in them, such as START, END and REPORT, are kept as they are.
    """
    text = message.rstrip("\n")
    start = text.find("{")
    if start != -1:
        try:
            json.loads(text[start:])
        except ValueError:
            return text
        return text[start:]
    return text


def labels_for(log_group: str) -> dict:
    """Stream labels: the function name, and the service name as the pods' logs carry it in app."""
    function = log_group.rsplit("/", 1)[-1]
    project = os.environ.get("PROJECT", "lineclear")
    service = function[len(project) + 1:] if function.startswith(project + "-") else function
    return {"job": "lambda", "function": function, "app": service}


def loki_payload(batch: dict) -> dict:
    values = [[str(event["timestamp"] * 1_000_000), log_line(event["message"])] for event in batch["logEvents"]]
    return {"streams": [{"stream": labels_for(batch["logGroup"]), "values": values}]}


def handle(event, context):
    batch = json.loads(gzip.decompress(base64.b64decode(event["awslogs"]["data"])))
    if batch.get("messageType") != "DATA_MESSAGE":
        return {"forwarded": 0}  # CloudWatch sends one CONTROL_MESSAGE to test a new subscription
    payload = loki_payload(batch)
    request = urllib.request.Request(
        LOKI_PUSH_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    # An error here fails the call, and CloudWatch Logs tries the batch again
    with urllib.request.urlopen(request, timeout=5) as response:
        response.read()
    count = len(payload["streams"][0]["values"])
    print(json.dumps({"message": "Forwarded log lines to Loki", "log_group": batch["logGroup"], "lines": count}))
    return {"forwarded": count}
