"""The demo fault switch: freeze the coordinator right after it logs COMMIT, once.

It lives in the database, not in one pod's memory, so arming it works whichever replica the load
balancer sends the call to; the replica that is coordinator when the next COMMIT is logged takes it.
"""

from psycopg_pool import ConnectionPool


class FaultSwitch:
    def __init__(self, pool: ConnectionPool) -> None:
        self._pool = pool

    def arm(self, pause_seconds: int) -> dict:
        with self._pool.connection() as conn:
            return conn.execute(
                "UPDATE fault_switch SET armed = true, pause_seconds = %s, armed_at = now() "
                "RETURNING armed, pause_seconds, armed_at",
                (pause_seconds,),
            ).fetchone()

    def status(self) -> dict:
        with self._pool.connection() as conn:
            return conn.execute("SELECT armed, pause_seconds, armed_at FROM fault_switch").fetchone()

    def take(self) -> int:
        """Disarm the switch and return its pause in seconds, or 0 if it was not armed."""
        with self._pool.connection() as conn:
            row = conn.execute(
                "UPDATE fault_switch SET armed = false WHERE armed RETURNING pause_seconds"
            ).fetchone()
        return row["pause_seconds"] if row else 0
