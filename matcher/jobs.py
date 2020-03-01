from . import netstring
from datetime import datetime
from .place import Place
import socket
import json


def connect_to_queue():
    address = ('localhost', 6030)
    sock = socket.create_connection(address)
    sock.setblocking(True)
    return sock

def matcher_queue_request(send_msg):
    sock = connect_to_queue()
    netstring.write(sock, json.dumps(send_msg))

    replies = []
    while True:
        network_msg = netstring.read(sock)
        if network_msg is None:
            break
        replies.append(json.loads(network_msg))

    sock.close()

    assert len(replies) == 1
    return replies[0]

def get_jobs():
    reply = matcher_queue_request({'type': 'jobs'})
    assert reply['type'] == 'jobs'

    job_list = []
    for job in reply['items']:
        osm_type, osm_id = job['osm_type'], job['osm_id']
        place = Place.get_by_osm(osm_type, osm_id)
        job['start'] = datetime.strptime(job['start'], "%Y-%m-%d %H:%M:%S")
        job['place'] = place
        job_list.append(job)

    return job_list
