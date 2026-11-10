"""Sends GPS position reports for a few trains moving along their block sections.

Each train runs between its origin and destination, crossing block sections S1 to S10
on the way, and reports its position every few seconds. Only the Python standard library is used.

Example:
    python3 loadtest/gps_simulator.py --base-url http://localhost:8001 --minutes 2
"""

import argparse
import json
import random
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

SECTIONS = 10

# Train number: (origin latitude, longitude), (destination latitude, longitude)
ROUTES = {
    "12951": ((18.9696, 72.8194), (28.6430, 77.2194)),  # Mumbai Central to New Delhi
    "12002": ((28.6430, 77.2194), (23.2683, 77.4344)),  # New Delhi to Rani Kamalapati
    "22691": ((12.9784, 77.5698), (28.5880, 77.2530)),  # KSR Bengaluru to Hazrat Nizamuddin
    "12626": ((28.6430, 77.2194), (8.4875, 76.9520)),  # New Delhi to Thiruvananthapuram Central
}


def position_report(train_number: str, progress: float) -> dict:
    (start_lat, start_lon), (end_lat, end_lon) = ROUTES[train_number]
    section = min(int(progress * SECTIONS), SECTIONS - 1) + 1
    return {
        "train_number": train_number,
        "section": f"S{section}",
        "latitude": round(start_lat + (end_lat - start_lat) * progress, 5),
        "longitude": round(start_lon + (end_lon - start_lon) * progress, 5),
        "speed_kmph": round(random.uniform(60, 130), 1),
        "recorded_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
    }


def send(base_url: str, report: dict) -> int:
    request = urllib.request.Request(
        f"{base_url}/api/positions",
        data=json.dumps(report).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code
    except (urllib.error.URLError, TimeoutError):
        return 0  # no answer at all


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--base-url", required=True, help="for example http://localhost:8001")
    parser.add_argument("--minutes", type=float, default=2, help="how long to run")
    parser.add_argument("--interval", type=float, default=2, help="seconds between rounds of reports")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    # Start the trains at different points so they sit in different sections
    progress = {train: index / (len(ROUTES) + 1) for index, train in enumerate(ROUTES)}
    step = 1 / 60  # a train crosses one section about every 6 rounds
    results: dict[int, int] = {}
    stop_at = time.monotonic() + args.minutes * 60

    while time.monotonic() < stop_at:
        for train in ROUTES:
            report = position_report(train, progress[train])
            status = send(base_url, report)
            results[status] = results.get(status, 0) + 1
            print(f"{report['recorded_at']}  train {train}  section {report['section']:>3}  HTTP {status}")
            progress[train] = (progress[train] + step) % 1.0
        time.sleep(args.interval)

    summary = ", ".join(f"HTTP {status}: {count}" for status, count in sorted(results.items()))
    print(f"Done. {summary}")


if __name__ == "__main__":
    main()