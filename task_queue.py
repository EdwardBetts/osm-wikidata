#!/usr/bin/python3
from gevent.server import StreamServer
from gevent.queue import PriorityQueue, Queue
from gevent import monkey, spawn, sleep
monkey.patch_all()

from matcher import overpass, netstring, utils
from matcher.view import app
import json
import os.path

# Priority queue
# We should switch to a priority queue ordered by number of chunks
# if somebody requests a place with 10 chunks they should go to the back
# of the queue
#
# Abort request
# If a user gives up and closes the page do we should remove their request from
# the queue if nobody else has made the same request.
#
# We can tell the page was closed by checking a websocket heartbeat.

app.config.from_object('config.default')

task_queue = PriorityQueue()

listen_host, port = 'localhost', 6020

# almost there
# should give status update as each chunk is loaded.
# tell client the length of the rate limit pause

def to_client(send_queue, msg_type, msg):
    msg['type'] = msg_type
    send_queue.put(msg)

def wait_for_slot(send_queue):
    print('get status')
    status = overpass.get_status()

    if not status['slots']:
        return
    secs = status['slots'][0]
    if secs <= 0:
        return
    send_queue.put({'type': 'status', 'wait': secs})
    sleep(secs)

def process_queue():
    while True:
        area, item = task_queue.get()
        place = item['place']
        send_queue = item['queue']
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
                utils.check_free_space()
                wait_for_slot(send_queue)
                to_client(send_queue, 'run_query', msg)
                print('run query')
                r = overpass.run_query(oql)
                print('query complete')
                with open(filename, 'wb') as out:
                    out.write(r.content)
                utils.check_free_space()
            print(msg)
            to_client(send_queue, 'chunk', msg)
        print('item complete')
        send_queue.put(None)

class Request:
    def __init__(self, sock, address):
        self.address = address
        self.sock = sock
        self.send_queue = None

    def send_msg(self, msg, check_ack=True):
        netstring.write(self.sock, json.dumps(msg))
        if check_ack:
            msg = netstring.read(self.sock)
            assert msg == 'ack'

    def reply_and_close(self, msg):
        self.send_msg(msg, check_ack=False)
        self.sock.close()

    def new_place_request(self, msg):
        self.send_queue = Queue()
        task_queue.put((msg['place']['area'], {
            'place': msg['place'],
            'address': self.address,
            'chunks': msg['chunks'],
            'queue': self.send_queue,
        }))

        self.send_msg({'type': 'connected'})

    def handle(self):
        print('New connection from %s:%s' % self.address)
        try:
            msg = json.loads(netstring.read(self.sock))
        except json.decoder.JSONDecodeError:
            msg = {'type': 'error', 'error': 'invalid JSON'}
            return self.reply_and_close(msg)

        if msg.get('type') == 'ping':
            return self.reply_and_close({'type': 'pong'})

        self.new_place_request(msg)
        try:
            to_send = self.send_queue.get()
            while to_send:
                self.send_msg(to_send)
                to_send = self.send_queue.get()
        except BrokenPipeError:
            print('socket closed')
        else:
            print('request complete')
            self.send_msg({'type': 'done'})

        self.sock.close()

def handle_request(sock, address):
    r = Request(sock, address)
    return r.handle()

def main():
    spawn(process_queue)
    print('listening on port {}'.format(port))
    server = StreamServer((listen_host, port), handle_request)
    server.serve_forever()


if __name__ == '__main__':
    main()
