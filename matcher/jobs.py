"""Jobs."""

import typing
from datetime import datetime

from . import chat
from .place import Place

StrDict = dict[str, typing.Any]


def matcher_queue_request(
    command: str,
    **params: typing.Any,
) -> StrDict:
    """Make request to matcher queue."""
    sock = chat.connect_to_queue()
    chat.send_command(sock, command, **params)

    replies = []
    while True:
        msg: StrDict | None = chat.read_json_line(sock)
        if msg is None:
            break
        replies.append(msg)

    sock.close()

    assert len(replies) == 1
    return replies[0]


def parse_job_start(start: str) -> datetime:
    """Parse timestamp string to datetime."""
    return datetime.strptime(start, "%Y-%m-%d %H:%M:%S")


def get_job(place: Place) -> StrDict | None:
    """Get job for given place."""
    reply = matcher_queue_request("jobs")
    assert reply["type"] == "jobs"

    job: StrDict
    for job in reply["items"]:
        if not all(job[key] == getattr(place, key) for key in ("osm_type", "osm_id")):
            continue
        job["start"] = parse_job_start(job["start"])
        return job
    return None


def get_jobs() -> list[StrDict]:
    """Get jobs from matcher queue."""
    reply = matcher_queue_request("jobs")
    assert reply["type"] == "jobs"

    job_list = []
    for job in reply["items"]:
        osm_type, osm_id = job["osm_type"], job["osm_id"]
        place = Place.get_by_osm(osm_type, osm_id)
        job["start"] = parse_job_start(job["start"])
        job["place"] = place
        job_list.append(job)

    return job_list


def stop_job(place: Place) -> None:
    """Send stop job request to matcher queue."""
    reply = matcher_queue_request("stop", osm_type=place.osm_type, osm_id=place.osm_id)

    assert reply["type"] == "stop" and reply["success"]
