"""Reporting, reading and resolving incidents, with every status change kept in the history."""

from psycopg import Connection
from psycopg_pool import ConnectionPool

INCIDENT_COLUMNS = (
    "incident_id, kind, section, train_number, description, status, pending_txn_id, reported_at, updated_at"
)


class IncidentNotFound(Exception):
    """No incident with that number."""


class IncidentBusy(Exception):
    """The incident cannot change now, for the reason given."""


def _with_history(conn: Connection, incident: dict) -> dict:
    incident["history"] = conn.execute(
        "SELECT status, note, txn_id, changed_at FROM incident_status_history "
        "WHERE incident_id = %s ORDER BY id",
        (incident["incident_id"],),
    ).fetchall()
    return incident


class IncidentStore:
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def report(self, kind: str, section: str, train_number: str | None, description: str) -> dict:
        with self._pool.connection() as conn:
            incident = conn.execute(
                f"INSERT INTO incidents (kind, section, train_number, description) "
                f"VALUES (%s, %s, %s, %s) RETURNING {INCIDENT_COLUMNS}",
                (kind, section, train_number, description),
            ).fetchone()
            conn.execute(
                "INSERT INTO incident_status_history (incident_id, status, note) VALUES (%s, 'REPORTED', %s)",
                (incident["incident_id"], description),
            )
            return _with_history(conn, incident)

    def get(self, incident_id: str) -> dict:
        with self._pool.connection() as conn:
            incident = conn.execute(
                f"SELECT {INCIDENT_COLUMNS} FROM incidents WHERE incident_id = %s", (incident_id,)
            ).fetchone()
            if incident is None:
                raise IncidentNotFound(incident_id)
            return _with_history(conn, incident)

    def recent(self, status: str | None, limit: int = 50) -> list[dict]:
        with self._pool.connection() as conn:
            return conn.execute(
                f"SELECT {INCIDENT_COLUMNS} FROM incidents WHERE %(status)s::text IS NULL OR status = %(status)s "
                "ORDER BY reported_at DESC LIMIT %(limit)s",
                {"status": status, "limit": limit},
            ).fetchall()

    def resolve(self, incident_id: str, note: str) -> dict:
        """The line is restored. Refused while a diversion for this incident is being decided."""
        with self._pool.connection() as conn:
            incident = conn.execute(
                "SELECT status, pending_txn_id FROM incidents WHERE incident_id = %s FOR UPDATE", (incident_id,)
            ).fetchone()
            if incident is None:
                raise IncidentNotFound(incident_id)
            if incident["pending_txn_id"]:
                raise IncidentBusy(f"a diversion for {incident_id} is being decided by {incident['pending_txn_id']}")
            if incident["status"] == "RESOLVED":
                raise IncidentBusy(f"{incident_id} is already resolved")
            conn.execute(
                "UPDATE incidents SET status = 'RESOLVED', updated_at = now() WHERE incident_id = %s", (incident_id,)
            )
            conn.execute(
                "INSERT INTO incident_status_history (incident_id, status, note) VALUES (%s, 'RESOLVED', %s)",
                (incident_id, note),
            )
        return self.get(incident_id)

    def ping(self) -> None:
        with self._pool.connection() as conn:
            conn.execute("SELECT 1")