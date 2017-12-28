from flask import Blueprint, current_app
from time import time, sleep
from .place import Place
from . import wikipedia, database, wikidata, netstring, utils
from datetime import datetime
import re
import json
import socket
import subprocess
import os.path
import shutil

ws = Blueprint('ws', __name__)
re_point = re.compile('^Point\(([-E0-9.]+) ([-E0-9.]+)\)$')

# TODO: different coloured icons
# - has enwiki article
# - match found
# - match not found

class MatcherSocket(object):
    def __init__(self, socket, place):
        self.socket = socket
        self.place = place
        self.t0 = time()

        start = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        self.log_filename = '{}_{}.log'.format(place.place_id, start)
        self.log_full_path = os.path.join(utils.log_location(),
                                          self.log_filename)
        self.log = open(self.log_full_path, 'w')

        self.task_host, self.task_port = 'localhost', 6020

    def mark_log_good(self):
        self.log.close()
        shutil.move(self.log_full_path, utils.good_location())

    def send(self, msg_type, **data):
        data['time'] = time() - self.t0
        data['type'] = msg_type
        json_msg = json.dumps(data)
        self.log.write(json_msg + "\n")
        self.log.flush()
        return self.socket.send(json_msg)

    def status(self, msg):
        if msg:
            self.send('msg', msg=msg)

    def item_line(self, msg):
        if msg:
            self.send('item', msg=msg)

    def error(self, msg):
        if msg:
            self.send('error', msg=msg)

    def report_empty_chunks(self, chunks):
        empty = [chunk['num'] for chunk in chunks if not chunk['oql']]
        if empty:
            self.send('empty', empty=empty)

    def already_done(self):
        pins = get_pins(self.place)
        self.send_pins(pins, self.place.items.count())
        self.report_empty_chunks(self.place.get_chunks())
        self.send('already_done')
        # FIXME - send error mail

    def wikidata_chunked(self, chunks):
        items = {}
        for num, bbox in enumerate(chunks):
            msg = 'requesting wikidata chunk {}'.format(num + 1)
            print(msg)
            self.status(msg)
            items.update(self.place.items_from_wikidata(bbox))
        return items

    def get_items(self):
        self.send('get_wikidata_items')
        print('items from wikidata')
        place = self.place
        chunk_size = place.wikidata_chunk_size()
        if chunk_size == 1:
            print('wikidata unchunked')
            wikidata_items = place.items_from_wikidata()
        else:
            chunks = list(place.chunk_n(chunk_size))

            msg = 'downloading wikidata in {} chunks'.format(len(chunks))
            self.status(msg)
            wikidata_items = self.wikidata_chunked(chunks)
        self.status('wikidata query complete')
        print('done')
        pins = build_item_list(wikidata_items)
        print('send pins: ', len(pins))
        self.send('pins', pins=pins)
        print('sent')

        print('load categories')
        self.send('load_cat')
        wikipedia.add_enwiki_categories(wikidata_items)
        print('done')
        self.send('load_cat_done')

        def print_msg(msg):
            print(msg)
        self.place.save_items(wikidata_items, debug=print_msg)
        print('items saved')
        self.send('items_saved')

    def task_queue_address(self):
        return (self.task_host, self.task_port)

    def connect_to_task_queue(self):
        address = self.task_queue_address()
        sock = socket.create_connection(address)
        sock.setblocking(True)
        return sock

    def check_task_queue_running(self):
        try:
            sock = self.connect_to_task_queue()
        except ConnectionRefusedError:
            return False
        msg = {'type': 'ping'}
        netstring.write(sock, json.dumps(msg))
        reply = json.loads(netstring.read(sock))
        sock.close()
        return reply['type'] == 'pong'

    def overpass_request(self, chunks):
        sock = self.connect_to_task_queue()

        fields = ['place_id', 'osm_id', 'osm_type']
        msg = {
            'place': {f: getattr(self.place, f) for f in fields},
            'chunks': chunks,
        }

        netstring.write(sock, json.dumps(msg))
        complete = False
        while True:
            print('read')
            from_network = netstring.read(sock)
            print('read complete')
            if from_network is None:
                print('done')
                break
            msg = json.loads(from_network)
            print('message type {}'.format(repr(msg['type'])))
            if msg['type'] == 'connected':
                print('task runnner connected')
                self.send('connected')
            elif msg['type'] == 'run_query':
                chunk_num = msg['num']
                self.send('get_chunk', chunk_num=chunk_num)
            elif msg['type'] == 'chunk':
                chunk_num = msg['num']
                self.send('chunk_done', chunk_num=chunk_num)
            elif msg['type'] == 'done':
                complete = True
                self.send('overpass_done')
            else:
                self.status('from network: ' + from_network)
            netstring.write(sock, 'ack')
        return complete

    def merge_chunks(self, chunks):
        files = [os.path.join('overpass', chunk['filename'])
                 for chunk in chunks
                 if chunk.get('oql')]

        cmd = ['osmium', 'merge'] + files + ['-o', self.place.overpass_filename]
        # status(' '.join(cmd))
        p = subprocess.run(cmd,
                           encoding='utf-8',
                           universal_newlines=True,
                           stderr=subprocess.PIPE,
                           stdout=subprocess.PIPE)
        msg = p.stdout if p.returncode == 0 else p.stderr
        if msg:
            self.status(msg)

    def send_pins(self, pins, item_count):
        self.send('pins', pins=pins)
        self.status('{:,d} Wikidata items found'.format(item_count))

    def get_item_detail(self, db_items):
        def extracts_progress(item):
            msg = 'load extracts: ' + item.label_and_qid()
            self.item_line(msg)

        print('getting wikidata item details')
        self.status('getting wikidata item details')
        for qid, entity in wikidata.entity_iter(db_items.keys()):
            item = db_items[qid]
            item.entity = entity
            msg = 'load entity: ' + item.label_and_qid()
            print(msg)
            self.item_line(msg)
        self.item_line('wikidata entities loaded')
        print('done')

        self.status('loading wikipedia extracts')
        self.place.load_extracts(progress=extracts_progress)
        self.item_line('extracts loaded')

    def run_osm2pgsql(self):
        self.status('running osm2pgsql')
        cmd = self.place.osm2pgsql_cmd()
        env = {'PGPASSWORD': current_app.config['DB_PASS']}
        subprocess.run(cmd, env=env, check=True)
        print('osm2pgsql done')
        self.status('osm2pgsql done')
        # could echo osm2pgsql output via websocket

    def run_matcher(self):
        def progress(candidates, item):
            num = len(candidates)
            noun = 'candidate' if num == 1 else 'candidates'
            count = ': {num} {noun} found'.format(num=num, noun=noun)
            msg = item.label_and_qid() + count
            self.item_line(msg)

        self.place.run_matcher(progress=progress)

def build_item_list(items):
    item_list = []
    for qid, v in items.items():
        label = v['query_label']
        enwiki = v.get('enwiki')
        if enwiki and not enwiki.startswith(label + ','):
            label = enwiki
        m = re_point.match(v['location'])
        if not m:
            print(qid, label, enwiki, v['location'])
        lon, lat = map(float, m.groups())
        item = {'qid': qid, 'label': label, 'lat': lat, 'lon': lon}
        if 'tags' in v:
            item['tags'] = list(v['tags'])
        item_list.append(item)
    return item_list

def get_pins(place):
    ''' Build pins from items in database. '''
    pins = []
    for item in place.items:
        lat, lon = item.coords()
        pin = {
            'qid': item.qid,
            'lat': lat,
            'lon': lon,
            'label': item.label(),
        }
        if item.tags:
            pin['tags'] = list(item.tags)
        pins.append(pin)
    return pins

def replay_log(ws_sock, log_filename):
    prev_time = 0
    include_delay = True
    for line in open(log_filename):
        cur = json.loads(line)
        t = cur['time']
        if include_delay:
            sleep(t - prev_time)
        # print(line[:-1])
        ws_sock.send(line[:-1])
        prev_time = t

def run_matcher(place, m):
    running = m.check_task_queue_running()
    if not running:
        m.status("error: unable to connect to task queue")
        database.session.commit()
        return

    if not place:
        m.status('error: place not found')
        # FIXME - send error mail
        return

    if place.state == 'ready':
        return m.already_done()

    print('state:', place.state)

    if not place.state or place.state == 'refresh':
        print('get items')
        try:
            m.get_items()
        except wikidata.QueryError:
            print('wikidata query error')
            m.error('wikidata query error')
            return
        place.state = 'tags'
        database.session.commit()
    else:
        print('get pins')
        pins = get_pins(place)
        m.send('pins', pins=pins)

    db_items = {item.qid: item for item in place.items}
    item_count = len(db_items)
    m.status('{:,d} Wikidata items found'.format(item_count))

    if place.state == 'tags':
        m.get_item_detail(db_items)
        place.state = 'wbgetentities'
        database.session.commit()

    chunks = place.get_chunks()
    m.report_empty_chunks(chunks)

    if place.overpass_done:
        m.status('using existing overpass data')
    else:
        m.status('downloading data from overpass')
        try:
            overpass_good = m.overpass_request(chunks)
        except ConnectionRefusedError:
            m.status("error: unable to connect to task queue")
            database.session.commit()
            return
        if not overpass_good:
            m.send('overpass_error')
            # FIXME: e-mail admin
            return
        if len(chunks) > 1:
            m.merge_chunks(chunks)
        place.state = 'postgis'
        database.session.commit()

    if place.state == 'postgis':
        m.run_osm2pgsql()
        place.state = 'osm2pgsql'
        database.session.commit()

    if place.state == 'osm2pgsql':
        m.run_matcher()

    m.item_line('finished')
    place.state = 'ready'
    database.session.commit()
    print('done')
    m.send('done')
    m.mark_log_good()

@ws.route('/websocket/matcher/<osm_type>/<int:osm_id>')
def ws_matcher(ws_sock, osm_type, osm_id):
    # idea: catch exceptions, then pass to pass to web page as status update
    # also e-mail them

    print('websocket')

    place = Place.get_by_osm(osm_type, osm_id)
    if place.state == 'ready':
        log_filename = utils.find_log_file(place)
        if log_filename:
            print('replaying log:', log_filename)
            replay_log(ws_sock, log_filename)
            return

    m = MatcherSocket(ws_sock, place)
    return run_matcher(place, m)
