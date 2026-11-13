"""Prometheus metrics of the newest result for each section, for the Grafana dashboard."""

from prometheus_client import Counter, Gauge

SECTION_TRAINS = Gauge(
    "lineclear_section_trains", "Distinct trains in the section in the newest closed minute", ["section"]
)
SECTION_UPDATES = Gauge(
    "lineclear_section_updates", "Position updates from the section in the newest closed minute", ["section"]
)
SECTION_INCIDENTS = Gauge(
    "lineclear_section_incidents", "Incidents reported in the section in the newest closed five minutes", ["section"]
)
ELECTIONS = Gauge("lineclear_coordination_elections", "Leader elections in the newest closed minute")
LEADERLESS_MS = Gauge(
    "lineclear_coordination_leaderless_ms", "Milliseconds without a duty controller in the newest closed minute"
)
COMMITS = Gauge("lineclear_coordination_commits", "Diversions committed in the newest closed minute")
ABORTS = Gauge("lineclear_coordination_aborts", "Diversions aborted in the newest closed minute")
RESULTS = Counter("lineclear_ops_results", "Results read from ops.results", ["metric"])
LATE_POSITIONS = Counter("lineclear_late_positions", "Position reports that arrived after their window closed")
REJECTED = Counter("lineclear_ops_results_rejected", "Messages on ops.results that could not be read")
WINDOW_END = Gauge(
    "lineclear_ops_results_window_end_seconds", "End of the newest window read, as Unix time", ["metric"]
)


class LatestResults:
    """Updates the gauges only from a window at least as new as the one they show,
    so a result replayed after a Flink restart never moves them back in time."""

    def __init__(self) -> None:
        self._newest: dict[tuple[str, str], str] = {}

    def record(self, result: dict) -> None:
        metric = result["metric"]
        RESULTS.labels(metric).inc()
        if metric == "late_position":
            LATE_POSITIONS.inc()
            return
        key = (metric, result.get("section") or "all")
        window_start = result["window_start"]
        if self._newest.get(key, "") > window_start:
            return  # an older window, already superseded
        self._newest[key] = window_start
        section = result.get("section")
        if metric == "section_traffic":
            SECTION_TRAINS.labels(section).set(result["trains"])
            SECTION_UPDATES.labels(section).set(result["updates"])
        elif metric == "section_incidents":
            SECTION_INCIDENTS.labels(section).set(result["incidents"])
        elif metric == "coordination":
            ELECTIONS.set(result["elections"])
            LEADERLESS_MS.set(result["leaderless_ms"])
            COMMITS.set(result["commits"])
            ABORTS.set(result["aborts"])

    @staticmethod
    def window_closed(metric: str, window_end_unix: float) -> None:
        WINDOW_END.labels(metric).set(window_end_unix)
