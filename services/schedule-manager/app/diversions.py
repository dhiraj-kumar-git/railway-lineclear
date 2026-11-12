"""Read-only views of the coordinator's log and the revised schedule, for the REST API."""

from psycopg_pool import ConnectionPool

SUMMARY_COLUMNS = (
    "txn_id, incident_id, train_number, blocked_section, loop_line, term, coordinator_id, "
    "state, reason, started_at, decided_at, completed_at, notified_at"
)


class DiversionLog:
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def recent(self, limit: int = 20) -> list[dict]:
        with self._pool.connection() as conn:
            return conn.execute(
                f"SELECT {SUMMARY_COLUMNS} FROM transactions ORDER BY started_at DESC LIMIT %s", (limit,)
            ).fetchall()

    def get(self, txn_id: str) -> dict | None:
        with self._pool.connection() as conn:
            txn = conn.execute(f"SELECT {SUMMARY_COLUMNS} FROM transactions WHERE txn_id = %s", (txn_id,)).fetchone()
            if txn is None:
                return None
            txn["participants"] = conn.execute(
                "SELECT participant, vote, reason, acknowledged_at FROM transaction_participants "
                "WHERE txn_id = %s ORDER BY participant",
                (txn_id,),
            ).fetchall()
            txn["revised_stops"] = conn.execute(
                "SELECT stop_seq, station_code, station_name, planned_arrival, revised_arrival, delay_minutes "
                "FROM schedule_revisions WHERE txn_id = %s ORDER BY stop_seq",
                (txn_id,),
            ).fetchall()
            return txn

    def latest_revision(self, train_number: str) -> dict | None:
        """The most recent committed diversion of a train, with its revised arrival times."""
        with self._pool.connection() as conn:
            txn = conn.execute(
                "SELECT txn_id FROM transactions WHERE train_number = %s AND state = 'COMMIT' "
                "ORDER BY decided_at DESC LIMIT 1",
                (train_number,),
            ).fetchone()
        return self.get(txn["txn_id"]) if txn else None

    def decision(self, txn_id: str) -> dict | None:
        with self._pool.connection() as conn:
            return conn.execute("SELECT state, term FROM transactions WHERE txn_id = %s", (txn_id,)).fetchone()

    def ping(self) -> None:
        with self._pool.connection() as conn:
            conn.execute("SELECT 1")