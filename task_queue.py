#!/usr/bin/python3
import better_exceptions  # noqa: F401
from gevent.server import StreamServer
from gevent.queue import Queue
from gevent.event import Event
from gevent import monkey, spawn, sleep
monkey.patch_all()

from matcher import overpass, netstring
import json
import os.path

task_queue = Queue()
sockets = {}

listen_host, port = 'localhost', 6020

# almost there
# should give status update as each chunk is loaded.
# tell client the length of the rate limit pause

def queue_update(msg_type, msg, request_address=None):
    items = sockets.items()
    for address, sock in list(items):
        if address not in sockets:
            continue
        try:
            msg['type'] = msg_type
            netstring.write(sock, json.dumps(msg))
        except BrokenPipeError:
            print('socket closed')
            sock.close()
            del sockets[address]
            return
        reply = netstring.read(sock)
        print('reply:', reply)
        assert reply == 'ack'

        if msg_type == 'done' and address == request_address:
            to_send = 'request complete'
            print(to_send)
            netstring.write(sock, to_send)
            reply = netstring.read(sock)
            print('reply:', reply)
            assert reply == 'ack'
            sock.close()
            del sockets[address]

def wait_for_slot():
    status = overpass.get_status()

    if not status['slots']:
        return
    secs = status['slots'][0]
    if secs <= 0:
        return
    queue_update('status', {'wait': secs})
    sleep(secs)

def process_queue():
    while True:
        item = task_queue.get()
        place = item['place']
        address = item['address']
        for num, chunk in enumerate(item['chunks']):
            oql = chunk.get('oql')
            if not oql:
                continue
            filename = 'overpass/' + chunk['filename']
            msg = {
                'num': num,
                'filename': chunk['filename'],
                'place': place,
            }
            if not os.path.exists(filename):
                wait_for_slot()
                queue_update('run_query', msg, address)
                r = overpass.run_query(oql)
                with open(filename, 'wb') as out:
                    out.write(r.content)
            queue_update('chunk', msg, address)
        queue_update('done', {'place': place}, address)

def handle(sock, address):
    print('New connection from %s:%s' % address)
    try:
        msg = json.loads(netstring.read(sock))
    except json.decoder.JSONDecodeError:
        netstring.write(sock, 'invalid JSON')
        sock.close()
        return

    # print(msg)
    task_queue.put({
        'place': msg['place'],
        'address': address,
        'chunks': msg['chunks'],
    })
    netstring.write(sock, 'connected')

    sockets[address] = sock
    # end of function closes the socket
    Event().wait()

def main():
    spawn(process_queue)
    print('listening on port {}'.format(port))
    server = StreamServer((listen_host, port), handle)
    server.serve_forever()


if __name__ == '__main__':
    main()
