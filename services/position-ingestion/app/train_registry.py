"""Checks train numbers against Train Registry, remembering each answer for a few minutes."""

import os
import threading
import time

import httpx

KNOWN_TRAIN_SECONDS = 300
UNKNOWN_TRAIN_SECONDS = 30


class TrainRegistryUnavailable(Exception):
    """Train Registry could not be reached, so the train cannot be checked."""


class TrainRegistryClient:
    def __init__(self) -> None:
        self._client = httpx.Client(base_url=os.environ["TRAIN_REGISTRY_URL"], timeout=2.0)
        self._answers: dict[str, tuple[bool, float]] = {}
        self._lock = threading.Lock()

    def train_exists(self, train_number: str) -> bool:
        now = time.monotonic()
        with self._lock:
            cached = self._answers.get(train_number)
        if cached is not None and cached[1] > now:
            return cached[0]

        try:
            response = self._client.get(f"/api/trains/{train_number}")
        except httpx.HTTPError as error:
            raise TrainRegistryUnavailable(str(error)) from error
        if response.status_code == 200:
            exists = True
        elif response.status_code == 404:
            exists = False
        else:
            raise TrainRegistryUnavailable(f"Train Registry answered {response.status_code}")

        keep_for = KNOWN_TRAIN_SECONDS if exists else UNKNOWN_TRAIN_SECONDS
        with self._lock:
            self._answers[train_number] = (exists, now + keep_for)
        return exists

    def close(self) -> None:
        self._client.close()