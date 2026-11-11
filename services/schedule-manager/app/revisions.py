"""Revised arrival times for a train diverted through a loop line.

Block sections S1 to S10 split each route into ten equal parts, as in the GPS simulator, so a stop
lying at or beyond the start of the closed section is reached late by the diversion's delay.
"""

SECTIONS = 10


def _minutes(clock: str) -> int:
    hours, minutes = clock.split(":")[:2]
    return int(hours) * 60 + int(minutes)


def _clock(minutes: int) -> str:
    days, minutes = divmod(minutes, 24 * 60)
    text = f"{minutes // 60:02d}:{minutes % 60:02d}"
    return f"{text} (+{days} day)" if days else text


def revised_stops(train: dict, blocked_section: str, delay_minutes: int) -> list[dict]:
    stops = train["stops"]
    if len(stops) < 2:
        return []
    section_starts_at = (int(blocked_section[1:]) - 1) / SECTIONS  # S7 starts 60% along the route
    last = len(stops) - 1
    revised = []
    for index, stop in enumerate(stops):
        if stop["arrives"] is None or index / last < section_starts_at:
            continue  # the origin, and stops before the closed section, keep their times
        planned = _minutes(stop["arrives"])
        revised.append({
            "stop_seq": stop["stop_seq"],
            "station_code": stop["station_code"],
            "station_name": stop["station_name"],
            "planned_arrival": _clock(planned),
            "revised_arrival": _clock(planned + delay_minutes),
            "delay_minutes": delay_minutes,
        })
    return revised