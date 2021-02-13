from . import chat
from datetime import datetime
from .place import Place


def matcher_queue_request(command, **params):
    sock = chat.connect_to_queue()
    chat.send_command(sock, command, **params)

    replies = []
    while True:
        msg = chat.read_json_line(sock)
        if msg is None:
            break
        replies.append(msg)

    sock.close()

    assert len(replies) == 1
    return replies[0]


def parse_job_start(start):
    return datetime.strptime(start, "%Y-%m-%d %H:%M:%S")


def get_job(place):
    reply = matcher_queue_request("jobs")
    assert reply["type"] == "jobs"

    for job in reply["items"]:
        if not all(job[key] == getattr(place, key) for key in ("osm_type", "osm_id")):
            continue
        job["start"] = parse_job_start(job["start"])
        return job


def get_jobs():
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


def stop_job(place):
    reply = matcher_queue_request("stop", osm_type=place.osm_type, osm_id=place.osm_id)

    assert reply["type"] == "stop" and reply["success"]
