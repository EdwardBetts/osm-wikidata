from . import chat
from datetime import datetime
from .place import Place

def matcher_queue_request(command):
    sock = chat.connect_to_queue()
    chat.send_msg(sock, command)

    replies = []
    while True:
        msg = chat.read_json_line(sock)
        if msg is None:
            break
        replies.append(msg)

    sock.close()

    assert len(replies) == 1
    return replies[0]

def get_jobs():
    reply = matcher_queue_request('jobs')
    assert reply['type'] == 'jobs'

    job_list = []
    for job in reply['items']:
        osm_type, osm_id = job['osm_type'], job['osm_id']
        place = Place.get_by_osm(osm_type, osm_id)
        job['start'] = datetime.strptime(job['start'], "%Y-%m-%d %H:%M:%S")
        job['place'] = place
        job_list.append(job)

    return job_list
