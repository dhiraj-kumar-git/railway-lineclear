#!/usr/bin/env python3
"""LineClear Control Office: the application demo of Railway LineClear, run on a laptop.

It serves one web page and answers its calls on one origin, so the page needs no CORS:

  /api/...          forwarded unchanged to the public load balancer: Train Registry, Position Ingestion,
                    Incident Manager and Schedule Manager, exactly as any client would call them
  /passenger/...    forwarded to the Passenger Notification Lambda's HTTPS function URL
  /staff/...        staff actions of the control office (resolve an incident, arm the fault switch,
                    free the track); this server adds the staff key, which never reaches the browser
  /cluster/...      the two autoscalers, read with kubectl, so the page can show the scale-out

  python3 application-demo/control-office.py remote   the live system on AWS (load balancer found with kubectl)
  python3 application-demo/control-office.py stub     everything simulated here, no AWS needed

  --port 8090        where to open the page (default 8090)
  --target URL       remote: the load balancer, http://... (default: found with kubectl)
  --passenger-url    remote: the Lambda function URL (default: found with the AWS CLI)

Every call the page makes is also printed in this terminal with the service that answered it.
Standard library only; remote mode needs kubectl and the AWS CLI signed in to the project account.
"""

import argparse
import collections
import itertools
import json
import math
import mimetypes
import re
import subprocess
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

LEASE_SECONDS = 6.0
STAFF_KEY = "local-staff-key"
KINDS = {"rail_fracture", "signal_failure", "track_obstruction", "overhead_wire_fault"}

TRAINS = [
    {"number": "12002", "name": "Bhopal Shatabdi Express", "category": "Shatabdi", "origin": "NDLS", "destination": "RKMP"},
    {"number": "12626", "name": "Kerala Express", "category": "Superfast", "origin": "NDLS", "destination": "TVC"},
    {"number": "12951", "name": "Mumbai Rajdhani Express", "category": "Rajdhani", "origin": "MMCT", "destination": "NDLS"},
    {"number": "22691", "name": "Bengaluru Rajdhani Express", "category": "Rajdhani", "origin": "SBC", "destination": "NZM"},
]
STOPS = {
    "12951": [("MMCT", "Mumbai Central", None, "17:00"), ("BRC", "Vadodara", "21:15", "21:20"), ("RTM", "Ratlam", "00:25", "00:28"),
              ("KOTA", "Kota Junction", "03:10", "03:20"), ("NDLS", "New Delhi", "08:30", None)],
    "12002": [("NDLS", "New Delhi", None, "06:00"), ("AGC", "Agra Cantt", "07:50", "07:55"), ("JHS", "Jhansi Junction", "10:48", "10:53"),
              ("RKMP", "Rani Kamlapati", "14:05", None)],
    "22691": [("SBC", "KSR Bengaluru", None, "20:00"), ("BPL", "Bhopal Junction", "10:00", "10:10"), ("NZM", "Hazrat Nizamuddin", "05:30", None)],
    "12626": [("NDLS", "New Delhi", None, "20:10"), ("BPL", "Bhopal Junction", "05:40", "05:50"), ("ERS", "Ernakulam Junction", "13:05", "13:10"),
              ("TVC", "Thiruvananthapuram Central", "17:10", None)],
}
LATE_STOPS = {
    "12951": [(4, "KOTA", "Kota Junction", "03:10"), (5, "NDLS", "New Delhi", "08:30")],
    "12002": [(3, "JHS", "Jhansi Junction", "10:48"), (4, "RKMP", "Rani Kamlapati", "14:05")],
    "22691": [(2, "BPL", "Bhopal Junction", "10:00"), (3, "NZM", "Hazrat Nizamuddin", "05:30")],
    "12626": [(3, "ERS", "Ernakulam Junction", "13:05"), (4, "TVC", "Thiruvananthapuram Central", "17:10")],
}


def iso(t: float | None) -> str | None:
    """A timestamp the way FastAPI writes a PostgreSQL timestamptz."""
    if t is None:
        return None
    return datetime.fromtimestamp(t, timezone.utc).isoformat(timespec="microseconds")


class ApiError(Exception):
    def __init__(self, status: int, detail):
        super().__init__(str(detail))
        self.status, self.detail = status, detail


class Division:
    """A small stand-in for the eight services: enough to drive every screen of the console."""

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.reset_all()

    def reset_all(self) -> None:
        with self.lock:
            self.positions = {}
            self.incidents = []
            self.txns = []
            self.holds = {}  # segment -> (txn_id, state, kind, train)
            self.incident_seq = itertools.count(45)
            self.txn_seq = itertools.count(101)
            self.fault = {"armed": False, "pause_seconds": 20, "armed_at": None}
            now = time.time()
            # Leader Election: replica a holds the lease in term 1
            self.replicas = {"schedule-manager-7c9d5-a1b2c": {"frozen_until": 0.0, "last_term": 1},
                             "schedule-manager-7c9d5-x8k4m": {"frozen_until": 0.0, "last_term": 0}}
            self.lease = {"holder": "schedule-manager-7c9d5-a1b2c", "term": 1, "renewed": now}
            self.waiting = []  # (incident) reported while nobody leads
            self.unfinished = []  # transactions a frozen leader left for the next one
            self.answer_order = itertools.cycle(sorted(self.replicas))

    # -------------------------------------------------------------- the election

    def tick(self) -> None:
        """Renewals and campaigns, as the replicas would do every 2 seconds."""
        with self.lock:
            now = time.time()
            holder = self.lease["holder"]
            if now < self.replicas[holder]["frozen_until"]:
                pass  # the holder is frozen: no renewals
            else:
                self.lease["renewed"] = now
            if now - self.lease["renewed"] > LEASE_SECONDS:
                for name, replica in self.replicas.items():
                    if name != holder and now >= replica["frozen_until"]:
                        self.lease = {"holder": name, "term": self.lease["term"] + 1, "renewed": now}
                        replica["last_term"] = self.lease["term"]
                        threading.Timer(1.0, self.recover).start()  # read the log before new work
                        break

    def leader(self) -> tuple[str, int] | None:
        with self.lock:
            now = time.time()
            holder = self.lease["holder"]
            acting = now - self.lease["renewed"] < 4.0 and now >= self.replicas[holder]["frozen_until"]
            return (holder, self.lease["term"]) if acting else None

    def coordinator(self) -> dict:
        with self.lock:
            replica = next(self.answer_order)
            lead = self.leader()
            is_leader = bool(lead and lead[0] == replica)
            return {"replica": replica, "is_leader": is_leader, "term": self.replicas[replica]["last_term"],
                    "leader_id": replica if is_leader else self.lease["holder"]}

    def recover(self) -> None:
        with self.lock:
            unfinished, self.unfinished = self.unfinished, []
            waiting, self.waiting = self.waiting, []
        for txn in unfinished:
            self._complete(txn)
        for incident in waiting:
            self._start_diversion(incident)

    # -------------------------------------------------------------- incidents and 2PC

    def report(self, body: dict) -> dict:
        errors = []
        if body.get("kind") not in KINDS:
            errors.append({"loc": ["body", "kind"], "msg": f"Input should be one of {sorted(KINDS)}"})
        if not re.fullmatch(r"S([1-9]|10)", str(body.get("section", ""))):
            errors.append({"loc": ["body", "section"], "msg": "String should match pattern '^S([1-9]|10)$'"})
        train = body.get("train_number")
        if train is not None and not re.fullmatch(r"\d{5}", str(train)):
            errors.append({"loc": ["body", "train_number"], "msg": "String should match pattern '^\\d{5}$'"})
        if errors:
            raise ApiError(422, errors)
        with self.lock:
            now = time.time()
            incident = {"incident_id": f"I-{next(self.incident_seq)}", "kind": body["kind"], "section": body["section"],
                        "train_number": train, "description": body.get("description", ""), "status": "REPORTED",
                        "pending_txn_id": None, "reported_at": iso(now), "updated_at": iso(now),
                        "history": [{"status": "REPORTED", "note": body.get("description", ""), "txn_id": None, "changed_at": iso(now)}]}
            self.incidents.insert(0, incident)
        if train:
            threading.Timer(0.8, self._start_diversion, args=(incident,)).start()  # through Kafka to the leader
        return incident

    def _start_diversion(self, incident: dict) -> None:
        lead = self.leader()
        with self.lock:
            if lead is None:
                self.waiting.append(incident)
                return
            loop = "L1" if int(incident["section"][1:]) <= 5 else "L2"
            txn = {"txn_id": f"T-{next(self.txn_seq)}", "incident_id": incident["incident_id"], "train_number": incident["train_number"],
                   "blocked_section": incident["section"], "loop_line": loop, "term": lead[1], "coordinator_id": lead[0],
                   "state": "PREPARING", "reason": None, "started_at": iso(time.time()), "decided_at": None,
                   "completed_at": None, "notified_at": None,
                   "participants": [{"participant": "incident-manager", "vote": None, "reason": None, "acknowledged_at": None},
                                    {"participant": "track-availability", "vote": None, "reason": None, "acknowledged_at": None}],
                   "revised_stops": []}
            self.txns.insert(0, txn)
        threading.Timer(0.6, self._vote, args=(txn, incident)).start()

    def _vote(self, txn: dict, incident: dict) -> None:
        with self.lock:
            track_reason = None
            for segment in (txn["blocked_section"], txn["loop_line"]):
                hold = self.holds.get(segment)
                if hold:
                    holder_txn, hold_state, kind, train = hold
                    pending = " (not yet decided)" if hold_state == "PREPARED" else ""
                    track_reason = (f"{segment} is already reserved for train {train} by {holder_txn}{pending}" if kind == "reservation"
                                    else f"{segment} is already closed by {holder_txn}{pending}")
                    break
            incident_reason = f"incident {incident['incident_id']} is held by {incident['pending_txn_id']} (not yet decided)" if incident["pending_txn_id"] else None
            votes = {"track-availability": track_reason, "incident-manager": incident_reason}
            for p in txn["participants"]:
                reason = votes[p["participant"]]
                p["vote"], p["reason"] = ("NO", reason) if reason else ("YES", None)
            if not track_reason:
                self.holds[txn["blocked_section"]] = (txn["txn_id"], "PREPARED", "closure", txn["train_number"])
                self.holds[txn["loop_line"]] = (txn["txn_id"], "PREPARED", "reservation", txn["train_number"])
            if not incident_reason:
                incident["pending_txn_id"] = txn["txn_id"]
        threading.Timer(0.3, self._decide, args=(txn, incident)).start()

    def _decide(self, txn: dict, incident: dict) -> None:
        with self.lock:
            refusals = [p["reason"] for p in txn["participants"] if p["vote"] != "YES"]
            txn["state"] = "ABORT" if refusals else "COMMIT"
            txn["reason"] = "; ".join(refusals) or None
            txn["decided_at"] = iso(time.time())
            if txn["state"] == "COMMIT":
                txn["revised_stops"] = [{"stop_seq": s, "station_code": c, "station_name": n, "planned_arrival": a,
                                         "revised_arrival": self._late(a, 25), "delay_minutes": 25}
                                        for s, c, n, a in LATE_STOPS.get(txn["train_number"], [])]
            freeze = 0
            if txn["state"] == "COMMIT" and self.fault["armed"]:
                self.fault["armed"] = False
                freeze = self.fault["pause_seconds"]
                self.replicas[txn["coordinator_id"]]["frozen_until"] = time.time() + freeze
        if freeze:
            with self.lock:
                self.unfinished.append(txn)  # the next leader finishes it
            return
        threading.Timer(0.4, self._complete, args=(txn,)).start()

    def _complete(self, txn: dict) -> None:
        with self.lock:
            now = time.time()
            incident = next(i for i in self.incidents if i["incident_id"] == txn["incident_id"])
            for p in txn["participants"]:
                p["acknowledged_at"] = iso(now)
            for segment in (txn["blocked_section"], txn["loop_line"]):
                hold = self.holds.get(segment)
                if hold and hold[0] == txn["txn_id"]:
                    if txn["state"] == "COMMIT":
                        self.holds[segment] = (hold[0], "COMMITTED", hold[2], hold[3])
                    else:
                        del self.holds[segment]
            if incident["pending_txn_id"] == txn["txn_id"]:
                incident["pending_txn_id"] = None
                if txn["state"] == "COMMIT":
                    incident["status"], incident["updated_at"] = "MITIGATING", iso(now)
                    incident["history"].append({"status": "MITIGATING", "note": f"Train {txn['train_number']} diverted via {txn['loop_line']}",
                                                "txn_id": txn["txn_id"], "changed_at": iso(now)})
            txn["completed_at"] = iso(now)
        if txn["state"] == "COMMIT":
            threading.Timer(0.4, lambda: txn.__setitem__("notified_at", iso(time.time()))).start()

    @staticmethod
    def _late(clock: str, minutes: int) -> str:
        h, m = map(int, clock.split(":"))
        total = h * 60 + m + minutes
        days, total = divmod(total, 24 * 60)
        text = f"{total // 60:02d}:{total % 60:02d}"
        return f"{text} (+{days} day)" if days else text

    def resolve(self, incident_id: str, staff_key: str | None, body: dict) -> dict:
        if staff_key != STAFF_KEY:
            raise ApiError(401, "A valid X-Staff-Key header is required")
        with self.lock:
            incident = next((i for i in self.incidents if i["incident_id"] == incident_id), None)
            if incident is None:
                raise ApiError(404, f"Incident {incident_id} not found")
            if incident["pending_txn_id"]:
                raise ApiError(409, f"a diversion for {incident_id} is being decided by {incident['pending_txn_id']}")
            if incident["status"] == "RESOLVED":
                raise ApiError(409, f"{incident_id} is already resolved")
            now = iso(time.time())
            incident["status"], incident["updated_at"] = "RESOLVED", now
            incident["history"].append({"status": "RESOLVED", "note": body.get("note", "Line restored"), "txn_id": None, "changed_at": now})
            return incident

    def release_holds(self) -> dict:
        with self.lock:
            released = len(self.holds)
            self.holds.clear()
            resolved = 0
            for incident in self.incidents:
                if incident["status"] != "RESOLVED" and not incident["pending_txn_id"]:
                    incident["status"] = "RESOLVED"
                    resolved += 1
            return {"released_holds": released, "resolved_incidents": resolved}

    def arm(self, staff_key: str | None, body: dict) -> dict:
        if staff_key != STAFF_KEY:
            raise ApiError(401, "A valid X-Staff-Key header is required")
        pause = body.get("pause_seconds", 20)
        if not isinstance(pause, int) or not 5 <= pause <= 120:
            raise ApiError(422, [{"loc": ["body", "pause_seconds"], "msg": "Input should be between 5 and 120"}])
        with self.lock:
            self.fault = {"armed": True, "pause_seconds": pause, "armed_at": iso(time.time())}
            return dict(self.fault)

    # -------------------------------------------------------------- positions

    def save_position(self, body: dict) -> dict:
        if not any(t["number"] == body.get("train_number") for t in TRAINS):
            raise ApiError(422, f"Train {body.get('train_number')} is not in the Train Registry")
        if not re.fullmatch(r"[A-Z][A-Z0-9]{0,7}", str(body.get("section", ""))):
            raise ApiError(422, [{"loc": ["body", "section"], "msg": "String should match pattern"}])
        now_ms = int(time.time() * 1000)
        with self.lock:
            self.positions[body["train_number"]] = {
                "train_number": body["train_number"], "section": body["section"], "latitude": body.get("latitude"),
                "longitude": body.get("longitude"), "speed_kmph": body.get("speed_kmph"),
                "recorded_at": datetime.fromtimestamp(now_ms / 1000, timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "recorded_at_ms": now_ms, "event_id": f"local-{now_ms}"}
        return {"event_id": f"local-{now_ms}", "stored_as_latest": True}

    def position(self, train: str) -> dict:
        with self.lock:
            position = self.positions.get(train)
            if position is None:
                raise ApiError(404, f"No position known for train {train}")
            answer = dict(position)
        answer["age_seconds"] = round(max(0, time.time() * 1000 - answer["recorded_at_ms"]) / 1000, 1)
        return answer

    def summary(self, txn: dict) -> dict:
        return {k: v for k, v in txn.items() if k not in ("participants", "revised_stops")}




class DemoDivision(Division):
    """The simulated division, plus what the stub needs for the passenger and scaling views."""

    def reset_all(self) -> None:
        super().reset_all()
        self.post_times = collections.deque()
        self.scalers = {name: {"current": 2, "desired": 2, "low_since": None, "changed": 0.0}
                        for name in ("position-ingestion", "train-registry")}

    def save_position(self, body: dict) -> dict:
        answer = super().save_position(body)
        with self.lock:
            self.post_times.append(time.time())
        return answer

    def notices(self, train: str) -> list:
        with self.lock:
            done = [t for t in self.txns if t["train_number"] == train and t["state"] == "COMMIT" and t["notified_at"]]
            return [{"txn_id": t["txn_id"], "train_number": t["train_number"], "incident_id": t["incident_id"],
                     "blocked_section": t["blocked_section"], "loop_line": t["loop_line"], "decided_at": t["decided_at"],
                     "stations": [{k: s[k] for k in ("station_code", "station_name", "planned_arrival", "revised_arrival",
                                                     "delay_minutes")} for s in t["revised_stops"]],
                     "bulletin_key": f"bulletins/{t['txn_id']}.txt"} for t in done]

    def bulletin(self, txn_id: str) -> str | None:
        with self.lock:
            t = next((t for t in self.txns if t["txn_id"] == txn_id and t["state"] == "COMMIT" and t["notified_at"]), None)
        if not t:
            return None
        lines = [f"Railway LineClear passenger bulletin {t['txn_id']}", "",
                 f"Train {t['train_number']} is diverted through loop line {t['loop_line']} while block section "
                 f"{t['blocked_section']} is closed (incident {t['incident_id']}).", "Revised arrival times:"]
        lines += [f"  {s['station_name']} ({s['station_code']}): {s['revised_arrival']}, was {s['planned_arrival']}, "
                  f"{s['delay_minutes']} min late" for s in t["revised_stops"]]
        lines += ["", f"Issued {t['decided_at']}. We regret the inconvenience."]
        return "\n".join(lines) + "\n"

    def autoscalers(self) -> list:
        """Two autoscalers that follow the GPS report rate, scaling up quickly and down after 60 s."""
        with self.lock:
            now = time.time()
            while self.post_times and now - self.post_times[0] > 30:
                self.post_times.popleft()
            rate = len(self.post_times) / 30
            rows = []
            for name, s in self.scalers.items():
                load = rate if name == "position-ingestion" else rate * 0.7
                wanted = max(2, min(8, math.ceil(load / 1.5)))
                if wanted > s["current"] and now - s["changed"] >= 10:
                    s["current"], s["changed"] = min(wanted, s["current"] + 2), now
                    s["low_since"] = None
                elif wanted < s["current"]:
                    s["low_since"] = s["low_since"] or now
                    if now - s["low_since"] >= 60:
                        s["current"], s["changed"], s["low_since"] = max(wanted, s["current"] - 2), now, None
                s["desired"] = wanted
                cpu = round(min(97, 8 + load * 45 / s["current"]))
                rows.append({"name": name, "current": s["current"], "desired": wanted,
                             "min": 2, "max": 8, "cpu_percent": cpu, "cpu_target": 60})
            return rows

    def rush(self, seconds: int = 60, per_second: int = 12) -> None:
        """Stub only: a burst of GPS reports, standing in for the k6 load test."""
        def run():
            end = time.time() + seconds
            i = 0
            while time.time() < end:
                for _ in range(per_second):
                    train = TRAINS[i % len(TRAINS)]["number"]
                    try:
                        self.save_position({"train_number": train, "section": f"S{1 + (i // 4) % 10}", "speed_kmph": 90})
                    except ApiError:
                        pass
                    i += 1
                time.sleep(1)
        threading.Thread(target=run, daemon=True).start()

def stub_answer(method: str, path: str, query: str, body: dict, staff_key: str | None) -> tuple[int, object]:
    d = DIVISION
    if method == "GET" and path == "/api/trains":
        return 200, TRAINS
    if method == "GET" and (m := re.fullmatch(r"/api/trains/(\d+)", path)):
        train = next((t for t in TRAINS if t["number"] == m[1]), None)
        if not train:
            return 404, {"detail": f"Train {m[1]} not found"}
        stops = [{"stop_seq": i + 1, "station_code": c, "station_name": n, "arrives": a, "departs": d, "journey_day": 1}
                 for i, (c, n, a, d) in enumerate(STOPS[m[1]])]
        return 200, {**train, "stops": stops}
    if method == "POST" and path == "/api/positions":
        return 202, d.save_position(body)
    if method == "GET" and (m := re.fullmatch(r"/api/positions/(\w+)", path)):
        return 200, d.position(m[1])
    if method == "POST" and path == "/api/incidents":
        return 201, d.report(body)
    if method == "GET" and path == "/api/incidents":
        status = dict(p.split("=", 1) for p in query.split("&") if "=" in p).get("status")
        with d.lock:
            rows = [{k: v for k, v in i.items() if k != "history"} for i in d.incidents if not status or i["status"] == status]
        return 200, rows[:50]
    if method == "GET" and (m := re.fullmatch(r"/api/incidents/([\w-]+)", path)):
        incident = next((i for i in d.incidents if i["incident_id"] == m[1]), None)
        return (200, incident) if incident else (404, {"detail": f"Incident {m[1]} not found"})
    if method == "POST" and (m := re.fullmatch(r"/api/incidents/([\w-]+)/resolve", path)):
        return 200, d.resolve(m[1], staff_key, body)
    if method == "GET" and path == "/api/schedule/coordinator":
        return 200, d.coordinator()
    if method == "GET" and path == "/api/schedule/diversions":
        with d.lock:
            return 200, [d.summary(t) for t in d.txns[:20]]
    if method == "GET" and (m := re.fullmatch(r"/api/schedule/diversions/([\w-]+)", path)):
        txn = next((t for t in d.txns if t["txn_id"] == m[1]), None)
        return (200, txn) if txn else (404, {"detail": f"Transaction {m[1]} not found"})
    if method == "GET" and (m := re.fullmatch(r"/api/schedule/trains/(\d+)", path)):
        txn = next((t for t in d.txns if t["train_number"] == m[1] and t["state"] == "COMMIT"), None)
        return (200, txn) if txn else (404, {"detail": f"No committed diversion for train {m[1]}"})
    if method == "GET" and path == "/api/schedule/fault-switch":
        return 200, dict(d.fault)
    if method == "POST" and path == "/api/schedule/fault-switch":
        return 200, d.arm(staff_key, body)
    return 404, {"detail": "Not Found"}



DIVISION = DemoDivision()
HERE = Path(__file__).resolve().parent
SITE = HERE / "site"
REPO = HERE.parent
WORKSPACE = REPO.parent                        # ~/code, where the bundles sit next to the repository
MAIN_REPO = WORKSPACE / "railway-lineclear"    # the main checkout, where changes.py is normally run
CHANGES = WORKSPACE / "lineclear-final-changes" / "changes.py"
PASSENGER_FUNCTION = "lineclear-passenger-notification"
ARGS = None
STAFF = {"key": None, "reason": "not read yet"}
PASSENGER = {"url": None, "reason": "not read yet"}

SERVICES = [("/api/trains", "Train Registry"), ("/api/positions", "Position Ingestion"),
            ("/api/incidents", "Incident Manager"), ("/api/schedule", "Schedule Manager"),
            ("/passenger", "Passenger Notification (Lambda)"), ("/staff/incidents", "Incident Manager, staff"),
            ("/staff/fault-switch", "Schedule Manager, staff"), ("/staff/reset", "Track Availability and Incident Manager, staff"),
            ("/cluster", "Kubernetes autoscalers")]


def service_for(path: str) -> str:
    return next((name for prefix, name in SERVICES if path.startswith(prefix)), "")


def forward(url: str, method: str, raw: bytes, headers: dict, timeout: int = 15) -> tuple[int, bytes, str]:
    request = urllib.request.Request(url, data=raw if method == "POST" else None, method=method,
                                     headers={k: v for k, v in headers.items() if v})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read(), response.headers.get("Content-Type", "application/json")
    except urllib.error.HTTPError as error:
        return error.code, error.read(), error.headers.get("Content-Type", "application/json")
    except (urllib.error.URLError, OSError) as error:
        return 502, json.dumps({"detail": f"{url.split('/api')[0]} did not answer ({error}). Is the environment up?"}).encode(), "application/json"


def run(command: list, timeout: int = 30, cwd: Path | None = None) -> subprocess.CompletedProcess | None:
    try:
        return subprocess.run(command, capture_output=True, text=True, timeout=timeout, cwd=cwd)
    except (OSError, subprocess.TimeoutExpired):
        return None


def find_load_balancer() -> str:
    """The public load balancer's address, from the Ingress Kubernetes gave it."""
    result = run(["kubectl", "get", "ingress", "-n", "lineclear", "-o",
                  "jsonpath={.items[0].status.loadBalancer.ingress[0].hostname}"])
    host = result.stdout.strip() if result else ""
    if not host:
        raise SystemExit("Could not find the load balancer with kubectl. Is the environment up? Or pass --target http://<address>")
    return f"http://{host}"


def read_staff_key() -> None:
    """Read the staff key from Secrets Manager into memory. It is never printed or sent to the page."""
    result = run(["aws", "secretsmanager", "get-secret-value", "--secret-id", "lineclear/staff-api-key",
                  "--query", "SecretString", "--output", "text"])
    try:
        STAFF["key"], STAFF["reason"] = json.loads(result.stdout)["api-key"], "read from Secrets Manager"
    except (AttributeError, ValueError, KeyError, TypeError):
        STAFF["key"] = None
        STAFF["reason"] = "could not read lineclear/staff-api-key from Secrets Manager: " + \
                          ((result.stderr.strip()[:160] if result else "the AWS CLI did not run") or "empty answer")


def read_passenger_url() -> None:
    result = run(["aws", "lambda", "get-function-url-config", "--function-name", PASSENGER_FUNCTION,
                  "--query", "FunctionUrl", "--output", "text"])
    url = result.stdout.strip() if result and result.returncode == 0 else ""
    if url.startswith("https://"):
        PASSENGER["url"], PASSENGER["reason"] = url.rstrip("/") + "/", "found with the AWS CLI"
    else:
        PASSENGER["url"], PASSENGER["reason"] = None, "could not read the function URL of " + PASSENGER_FUNCTION


def autoscalers_live() -> tuple[int, list]:
    result = run(["kubectl", "get", "hpa", "-n", "lineclear", "-o", "json"], timeout=15)
    if not result or result.returncode:
        return 502, {"detail": "kubectl get hpa failed: " + (result.stderr.strip()[:160] if result else "kubectl did not run")}
    rows = []
    for item in json.loads(result.stdout).get("items", []):
        spec, status = item.get("spec", {}), item.get("status", {})
        cpu = next((m["resource"]["current"].get("averageUtilization") for m in status.get("currentMetrics") or []
                    if m.get("type") == "Resource"), None)
        target = next((m["resource"]["target"].get("averageUtilization") for m in spec.get("metrics") or []
                       if m.get("type") == "Resource"), None)
        rows.append({"name": spec.get("scaleTargetRef", {}).get("name", item["metadata"]["name"]),
                     "current": status.get("currentReplicas"), "desired": status.get("desiredReplicas"),
                     "min": spec.get("minReplicas"), "max": spec.get("maxReplicas"), "cpu_percent": cpu, "cpu_target": target})
    return 200, rows


def reset_live() -> tuple[int, dict]:
    if not CHANGES.is_file():
        return 503, {"detail": f"{CHANGES} not found: free the track from the terminal with changes.py reset"}
    cwd = MAIN_REPO if MAIN_REPO.is_dir() else REPO
    result = run(["python3", str(CHANGES), "reset"], timeout=180, cwd=cwd)
    if result is None:
        return 504, {"detail": "changes.py reset did not finish in 3 minutes"}
    lines = [line for line in (result.stdout + result.stderr).splitlines() if line.strip()][-12:]
    return (200 if result.returncode == 0 else 500), {"ok": result.returncode == 0, "output": lines}


class Handler(BaseHTTPRequestHandler):
    server_version = "lineclear-control-office"

    def log_message(self, fmt, *args):
        pass  # replaced by the trail printed in _trail

    def _trail(self, status: int, started: float) -> None:
        path = self.path.split("?")[0]
        quiet = self.command == "GET" and not path.startswith("/passenger/bulletins") and status < 500
        if not path.startswith(("/api", "/passenger", "/staff", "/cluster", "/local")) or quiet:
            return
        stamp = datetime.now().strftime("%H:%M:%S")
        print(f"{stamp}  {self.command:4} {path:42} {status}  {service_for(path)}  {(time.time() - started) * 1000:.0f} ms", flush=True)

    def _send(self, status: int, payload: bytes, content_type: str = "application/json") -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(payload)

    def _json(self, status: int, answer) -> None:
        self._send(status, json.dumps(answer).encode())

    def _handle(self) -> None:
        started = time.time()
        path, _, query = self.path.partition("?")
        raw = self.rfile.read(int(self.headers.get("Content-Length") or 0)) if self.command == "POST" else b""
        try:
            status = self._route(path, query, raw)
        except BrokenPipeError:
            return
        self._trail(status, started)

    def _route(self, path: str, query: str, raw: bytes) -> int:
        remote = ARGS.mode == "remote"
        if path == "/local/config":
            config = {"mode": ARGS.mode, "target": ARGS.target if remote else None,
                      "grafana": "http://localhost:3000", "argocd": "https://localhost:8443",
                      "staff": {"ready": bool(STAFF["key"]) or not remote, "reason": STAFF["reason"] if remote else "simulated"},
                      "reset": {"ready": CHANGES.is_file() or not remote, "reason": str(CHANGES)},
                      "passenger": {"ready": bool(PASSENGER["url"]) or not remote, "reason": PASSENGER["reason"] if remote else "simulated"}}
            self._json(200, config)
            return 200
        if path == "/local/rush" and self.command == "POST":
            if remote:
                self._json(404, {"detail": "In the live system, run the k6 load test from the terminal"})
                return 404
            DIVISION.rush()
            self._json(202, {"started": True, "seconds": 60})
            return 202

        if path.startswith("/api/"):
            if remote:  # forwarded unchanged; a staff key from the browser is never passed on
                headers = {"Content-Type": self.headers.get("Content-Type"), "Accept": "application/json"}
                status, payload, ctype = forward(ARGS.target + self.path, self.command, raw, headers)
                self._send(status, payload, ctype)
                return status
            return self._stub(path, query, raw, None)

        if path.startswith("/passenger/"):
            m = re.fullmatch(r"/passenger/(notices/\d{5}|bulletins/T-\d+)", path)
            if not m:
                self._json(404, {"detail": "Try /passenger/notices/12951 or /passenger/bulletins/T-101"})
                return 404
            if remote:
                if not PASSENGER["url"]:
                    self._json(503, {"detail": PASSENGER["reason"]})
                    return 503
                status, payload, ctype = forward(PASSENGER["url"] + m[1], "GET", b"", {"Accept": "application/json"})
                self._send(status, payload, ctype)
                return status
            kind, _, ident = m[1].partition("/")
            if kind == "notices":
                rows = DIVISION.notices(ident)
                self._json(200 if rows else 404, rows if rows else {"detail": f"No notices for train {ident}"})
                return 200 if rows else 404
            text = DIVISION.bulletin(ident)
            if text is None:
                self._json(404, {"detail": f"No bulletin for {ident}"})
                return 404
            self._send(200, text.encode(), "text/plain; charset=utf-8")
            return 200

        if path.startswith("/staff/") and self.command == "POST":
            return self._staff(path, raw)

        if path == "/cluster/autoscalers":
            status, rows = autoscalers_live() if remote else (200, DIVISION.autoscalers())
            self._json(status, rows)
            return status

        if path.startswith(("/api", "/staff", "/cluster", "/local", "/passenger")):
            self._json(404, {"detail": "Not Found"})
            return 404
        return self._page(path)

    def _staff(self, path: str, raw: bytes) -> int:
        remote = ARGS.mode == "remote"
        if path == "/staff/reset":
            if remote:
                status, answer = reset_live()
            else:
                status, answer = 200, {"ok": True, "output": [f"{k.replace('_', ' ')}: {v}" for k, v in DIVISION.release_holds().items()]}
            self._json(status, answer)
            return status
        m = re.fullmatch(r"/staff/incidents/(I-\d+)/resolve", path)
        target = f"/api/incidents/{m[1]}/resolve" if m else "/api/schedule/fault-switch" if path == "/staff/fault-switch" else None
        if not target:
            self._json(404, {"detail": "Not Found"})
            return 404
        if remote:
            if not STAFF["key"]:
                self._json(503, {"detail": "Staff actions are off: " + STAFF["reason"]})
                return 503
            headers = {"Content-Type": "application/json", "Accept": "application/json", "X-Staff-Key": STAFF["key"]}
            status, payload, ctype = forward(ARGS.target + target, "POST", raw or b"{}", headers)
            self._send(status, payload, ctype)
            return status
        return self._stub(target, "", raw, STAFF_KEY)

    def _stub(self, path: str, query: str, raw: bytes, staff_key: str | None) -> int:
        try:
            body = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            self._json(422, {"detail": "JSON decode error"})
            return 422
        try:
            status, answer = stub_answer(self.command, path, query, body, staff_key)
        except ApiError as error:
            status, answer = error.status, {"detail": error.detail}
        self._json(status, answer)
        return status

    def _page(self, path: str) -> int:
        file = (SITE / (path.lstrip("/") or "index.html")).resolve()
        if SITE not in file.parents or not file.is_file():
            file = SITE / "index.html"
        self._send(200, file.read_bytes(), mimetypes.guess_type(file.name)[0] or "application/octet-stream")
        return 200

    do_GET = do_POST = _handle


def elections() -> None:
    while True:
        DIVISION.tick()
        time.sleep(0.5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("mode", choices=["remote", "stub"])
    parser.add_argument("--port", type=int, default=8090)
    parser.add_argument("--target", help="remote: the load balancer, http://... (default: found with kubectl)")
    parser.add_argument("--passenger-url", help="remote: the Lambda function URL (default: found with the AWS CLI)")
    ARGS = parser.parse_args()
    if not (SITE / "index.html").is_file():
        raise SystemExit(f"no index.html in {SITE}")
    print(f"LineClear Control Office ({ARGS.mode})", flush=True)
    if ARGS.mode == "remote":
        ARGS.target = (ARGS.target or find_load_balancer()).rstrip("/")
        read_staff_key()
        if ARGS.passenger_url:
            PASSENGER["url"], PASSENGER["reason"] = ARGS.passenger_url.rstrip("/") + "/", "given with --passenger-url"
        else:
            read_passenger_url()
        print(f"  public APIs:     {ARGS.target}  (the live system)", flush=True)
        print(f"  passenger URL:   {PASSENGER['url'] or 'not found: ' + PASSENGER['reason']}", flush=True)
        print(f"  staff actions:   {'on, key ' + STAFF['reason'] if STAFF['key'] else 'off: ' + STAFF['reason']}", flush=True)
        print(f"  free the track:  {'on, via ' + str(CHANGES) if CHANGES.is_file() else 'off: ' + str(CHANGES) + ' not found'}", flush=True)
    else:
        threading.Thread(target=elections, daemon=True).start()
        print("  every service simulated in this process; nothing leaves the laptop", flush=True)
    print(f"  open http://localhost:{ARGS.port}   (stop with Ctrl+C)", flush=True)
    print("  calls the page makes appear below, with the service that answered:", flush=True)
    ThreadingHTTPServer(("127.0.0.1", ARGS.port), Handler).serve_forever()
