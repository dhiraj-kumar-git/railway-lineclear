"""Reads a train's route from Train Registry over REST, passing the transaction ID along."""

import os

import httpx


class TrainRegistryUnavailable(Exception):
    """Train Registry could not be reached or gave an unexpected answer."""


class TrainRegistryClient:
    def __init__(self) -> None:
        self._client = httpx.Client(base_url=os.environ["TRAIN_REGISTRY_URL"], timeout=2.0)

    def train(self, train_number: str, txn_id: str) -> dict | None:
        """The train with its ordered stops, or None if Train Registry does not know it."""
        try:
            response = self._client.get(f"/api/trains/{train_number}", headers={"X-Txn-Id": txn_id})
        except httpx.HTTPError as error:
            raise TrainRegistryUnavailable(str(error)) from error
        if response.status_code == 404:
            return None
        if response.status_code != 200:
            raise TrainRegistryUnavailable(f"Train Registry answered {response.status_code}")
        return response.json()

    def close(self) -> None:
        self._client.close()