#!/usr/bin/python3
import better_exceptions  # noqa: F401
from gevent.server import StreamServer
from gevent.queue import Queue
from gevent.event import Event
from gevent import monkey, spawn, sleep
from gevent.lock import Semaphore
monkey.patch_all()

from matcher import overpass, netstring
import json
import os.path

class Counter(object):
    def __init__(self, start=0):
        self.semaphore = Semaphore()
        self.value = start

    def add(self, other):
        self.semaphore.acquire()
        self.value += other
        self.semaphore.release()

    def sub(self, other):
        self.semaphore.acquire()
        self.value -= other
        self.semaphore.release()

    def get_value(self):
        return self.value


chunk_count = Counter()
task_queue = Queue()
# how many chunks ahead of this socket in the queue
chunk_count_sock = {}
sockets = {}

listen_host, port = 'localhost', 6020

# almost there
# should give status update as each chunk is loaded.
# tell client the length of the rate limit pause

def queue_update(msg_type, msg, request_address=None):
    chunk_count.sub(1)
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
        item['done'].set()

def handle(sock, address):
    print('New connection from %s:%s' % address)
    try:
        msg = json.loads(netstring.read(sock))
    except json.decoder.JSONDecodeError:
        netstring.write(sock, 'invalid JSON')
        sock.close()
        return

    queued_chunks = chunk_count.get_value()
    chunk_count_sock[address] = queued_chunks

    done = Event()

    # print(msg)
    task_queue.put({
        'place': msg['place'],
        'address': address,
        'chunks': msg['chunks'],
        'done': Event(),
    })
    chunk_count.add(len(msg['chunks']))
    msg = {'type': 'connected', 'queued_chunks': queued_chunks}
    netstring.write(sock, json.dumps(msg))

    sockets[address] = sock
    done.wait()  # end of function closes the socket

    to_send = 'request complete'
    print(to_send)
    netstring.write(sock, to_send)
    reply = netstring.read(sock)
    print('reply:', reply)
    assert reply == 'ack'
    sock.close()
    del sockets[address]

def main():
    spawn(process_queue)
    print('listening on port {}'.format(port))
    server = StreamServer((listen_host, port), handle)
    server.serve_forever()


if __name__ == '__main__':
    main()
