#!/usr/bin/python3

import threading
import socketserver
import json
import os.path
import requests.exceptions
import queue
import re
import lxml.etree
import subprocess

from matcher import (wikipedia, database, wikidata_api,
                     mail, overpass, space_alert, model, chat)
from time import time, sleep
from datetime import datetime
from matcher.place import Place, PlaceMatcher, bbox_chunk
from matcher.view import app

app.config.from_object('config.default')
database.init_app(app)
re_point = re.compile(r'^Point\(([-E0-9.]+) ([-E0-9.]+)\)$')

active_jobs = {}

task_queue = queue.PriorityQueue()

def wait_for_slot(send_queue):
    print('get status')
    try:
        status = overpass.get_status()
    except overpass.OverpassError as e:
        r = e.args[0]
        body = f'URL: {r.url}\n\nresponse:\n{r.text}'
        mail.send_mail('Overpass API unavailable', body)
        send_queue.put({'type': 'error',
                        'msg': "Can't access overpass API"})
        return False
    except requests.exceptions.Timeout:
        body = 'Timeout talking to overpass API'
        mail.send_mail('Overpass API timeout', body)
        send_queue.put({'type': 'error',
                        'msg': "Can't access overpass API"})
        return False

    print('status:', status)
    if not status['slots']:
        return True
    secs = status['slots'][0]
    if secs <= 0:
        return True
    send_queue.put({'type': 'status', 'wait': secs})
    sleep(secs)
    return True

def to_client(send_queue, msg_type, msg):
    msg['type'] = msg_type
    send_queue.put(msg)

def process_queue_loop():
    with app.app_context():
        while True:
            process_queue()

def process_queue():
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
            space_alert.check_free_space(app.config)
            if not wait_for_slot(send_queue):
                return
            to_client(send_queue, 'run_query', msg)
            print('run query')
            r = overpass.run_query(oql)
            print('query complete')
            with open(filename, 'wb') as out:
                out.write(r.content)
            space_alert.check_free_space(app.config)
        print(msg)
        to_client(send_queue, 'chunk', msg)
    print('item complete')
    send_queue.put({'type': 'done'})

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

class MatcherJobStopped(Exception):
    pass

class MatcherJob(threading.Thread):
    def __init__(self, osm_type, osm_id,
                 user=None, remote_addr=None, user_agent=None, want_isa=None,
                 wikidata_chunk_size=None, overpass_chunk_size=None):
        super(MatcherJob, self).__init__()
        self.osm_type = osm_type
        self.osm_id = osm_id
        self.start_time = time()
        self.subscribers = {}
        self.t0 = time()
        self.name = f'{osm_type}/{osm_id}  {self.t0}'
        self.user_id = user
        self.remote_addr = remote_addr
        self.user_agent = user_agent
        self.want_isa = set(want_isa) if want_isa else set()
        self._stop_event = threading.Event()
        self.wikidata_chunk_size = wikidata_chunk_size
        self.overpass_chunk_size = overpass_chunk_size

    def stop(self):
        self._stop_event.set()

    @property
    def stopping(self):
        return self._stop_event.is_set()

    def check_for_stop(self):
        if self._stop_event.is_set():
            raise MatcherJobStopped

    def prepare_for_refresh(self):
        self.place.delete_overpass()

        self.place.reset_all_items_to_not_done()

        engine = database.session.bind
        for t in database.get_tables():
            if not t.startswith(self.place.prefix):
                continue
            engine.execute('drop table if exists {}'.format(t))
        engine.execute('commit')
        database.session.commit()

        expect = [self.place.prefix + '_' + t for t in ('line', 'point', 'polygon')]
        tables = database.get_tables()
        assert not any(t in tables for t in expect)

        self.place.refresh_nominatim()
        database.session.commit()

    def matcher(self):
        place = self.place
        self.get_items()
        db_items = {item.qid: item for item in self.place.items}
        item_count = len(db_items)
        self.status('{:,d} Wikidata items found'.format(item_count))

        self.check_for_stop()
        self.get_item_detail(db_items)

        if self.overpass_chunk_size:
            chunk_size = self.overpass_chunk_size
        elif self.want_isa:
            chunk_size = 96
        else:
            chunk_size = None

        skip = {'building', 'building=yes'} if self.want_isa else set()

        if place.osm_type == 'node':
            oql = place.get_oql()
            chunks = [{'filename': f'{place.place_id}.xml', 'num': 0, 'oql': oql}]
        else:
            chunks = place.get_chunks(chunk_size=chunk_size, skip=skip)
            self.report_empty_chunks(chunks)
        self.check_for_stop()

        overpass_good = self.overpass_request(chunks)
        assert overpass_good
        self.check_for_stop()

        overpass_dir = app.config['OVERPASS_DIR']
        for chunk in chunks:
            self.check_for_stop()
            if not chunk['oql']:
                continue  # empty chunk
            filename = os.path.join(overpass_dir, chunk['filename'])
            if (os.path.getsize(filename) > 2000 or
                    "<remark> runtime error" not in open(filename).read()):
                continue
            root = lxml.etree.parse(filename).getroot()
            remark = root.find('.//remark')
            self.error('overpass: ' + remark.text)
            mail.send_mail('Overpass error', remark.text)
            return  # FIXME report error to admin

        if len(chunks) > 1:
            self.merge_chunks(chunks)

        self.check_for_stop()
        self.run_osm2pgsql()
        self.check_for_stop()
        self.load_isa()
        self.check_for_stop()
        self.run_matcher()
        self.check_for_stop()
        # self.place.clean_up()

    def run_in_app_context(self):
        self.place = Place.get_by_osm(self.osm_type, self.osm_id)
        if not self.place:
            self.send('not_found')
            self.send('done')
            del active_jobs[(self.osm_type, self.osm_id)]
            return

        if self.place.state == 'ready':
            self.send('already_done')
            self.send('done')
            del active_jobs[(self.osm_type, self.osm_id)]
            return

        is_refresh = self.place.state == 'refresh'

        user = model.User.query.get(self.user_id) if self.user_id else None

        run_obj = PlaceMatcher(place=self.place,
                               user=user,
                               remote_addr=self.remote_addr,
                               user_agent=self.user_agent,
                               is_refresh=is_refresh)
        database.session.add(run_obj)
        database.session.flush()

        self.prepare_for_refresh()
        self.matcher()

        run_obj.complete()
        self.place.state = 'ready'
        database.session.commit()
        print(run_obj.start, run_obj.end)

        print('sending done')
        self.send('done')
        print('done sent')
        del active_jobs[(self.osm_type, self.osm_id)]

    def run(self):
        with app.app_context():
            try:
                self.run_in_app_context()
            except Exception as e:
                error_str = f'{type(e).__name__}: {e}'
                print(error_str)
                self.send('error', msg=error_str)
                del active_jobs[(self.osm_type, self.osm_id)]

                info = 'matcher queue'
                mail.send_traceback(info, prefix='matcher queue')

        print('end thread:', self.name)

    def send(self, msg_type, **data):
        data['time'] = time() - self.t0
        data['type'] = msg_type
        for status_queue in self.subscribers.values():
            status_queue.put(data)

    def status(self, msg):
        if msg:
            self.send('msg', msg=msg)

    def error(self, msg):
        self.send('error', msg=msg)

    def item_line(self, msg):
        if msg:
            self.send('item', msg=msg)

    @property
    def subscriber_count(self):
        return len(self.subscribers)

    def subscribe(self, thread_name, status_queue):
        msg = {
            'time': time() - self.t0,
            'type': 'connected',
        }
        status_queue.put(msg)
        print('subscribe', self.name)
        self.subscribers[thread_name] = status_queue
        return status_queue

    def unsubscribe(self, thread_name):
        del self.subscribers[thread_name]

    def wikidata_chunked(self, chunks):
        items = {}
        num = 0
        while chunks:
            self.check_for_stop()
            bbox = chunks.pop()
            num += 1
            msg = f'requesting wikidata chunk {num}'
            print(msg)
            self.status(msg)
            try:
                items.update(self.place.bbox_wikidata_items(bbox,
                                                            want_isa=self.want_isa))
            except wikidata_api.QueryTimeout:
                msg = f'wikidata timeout, splitting chunk {num} into four'
                print(msg)
                self.status(msg)
                chunks += bbox_chunk(bbox, 2)

        return items

    def get_items(self):
        self.send('get_wikidata_items')

        if self.place.is_point:
            wikidata_items = self.get_items_point()
        else:
            wikidata_items = self.get_items_bbox()

        self.check_for_stop()

        self.status('wikidata query complete')
        pins = build_item_list(wikidata_items)
        self.send('pins', pins=pins)

        self.check_for_stop()

        self.send('load_cat')
        wikipedia.add_enwiki_categories(wikidata_items)
        self.send('load_cat_done')

        self.check_for_stop()

        self.place.save_items(wikidata_items)
        self.send('items_saved')

    def get_items_point(self):
        return self.place.point_wikidata_items()

    def get_items_bbox(self):
        ctx = app.test_request_context()
        ctx.push()  # to make url_for work
        place = self.place
        if self.wikidata_chunk_size:
            size = self.wikidata_chunk_size
        elif self.want_isa:
            size = 220
        else:
            size = 22
        chunk_size = place.wikidata_chunk_size(size=size)
        if chunk_size == 1:
            print('wikidata unchunked')
            try:
                wikidata_items = place.bbox_wikidata_items(want_isa=self.want_isa)
            except wikidata_api.QueryTimeout:
                place.wikidata_query_timeout = True
                database.session.commit()
                chunk_size = 2
                msg = 'wikidata query timeout, retrying with smaller chunks.'
                self.status(msg)

        if chunk_size != 1:
            chunks = list(place.polygon_chunk(size=size))

            msg = f'downloading wikidata in {len(chunks)} chunks'
            self.status(msg)
            wikidata_items = self.wikidata_chunked(chunks)

        return wikidata_items

    def get_item_detail(self, db_items):
        def extracts_progress(item):
            msg = 'load extracts: ' + item.label_and_qid()
            self.item_line(msg)

        print('getting wikidata item details')
        self.status('getting wikidata item details')
        for qid, entity in wikidata_api.entity_iter(db_items.keys()):
            item = db_items[qid]
            item.entity = entity
            msg = 'load entity: ' + item.label_and_qid()
            print(msg)
            self.item_line(msg)
        self.item_line('wikidata entities loaded')

        self.status('loading wikipedia extracts')
        self.place.load_extracts(progress=extracts_progress)
        self.item_line('extracts loaded')

    def report_empty_chunks(self, chunks):
        empty = [chunk['num'] for chunk in chunks if not chunk['oql']]
        if empty:
            self.send('empty', empty=empty)

    def overpass_request(self, chunks):
        send_queue = queue.Queue()

        fields = ['place_id', 'osm_id', 'osm_type', 'area']
        msg = {
            'place': {f: getattr(self.place, f) for f in fields},
            'chunks': chunks,
        }

        try:
            area = float(self.place.area)
        except ValueError:
            area = 0

        task_queue.put((area, {
            'place': self.place,
            'chunks': chunks,
            'queue': send_queue,
        }))

        complete = False
        while True:
            print('read from send queue')
            msg = send_queue.get()
            print('read complete')
            if msg is None:
                print('done (msg is None)')
                break
            print('message type {}'.format(repr(msg['type'])))
            if msg['type'] == 'run_query':
                chunk_num = msg['num']
                self.send('get_chunk', chunk_num=chunk_num)
            elif msg['type'] == 'chunk':
                chunk_num = msg['num']
                self.send('chunk_done', chunk_num=chunk_num)
            elif msg['type'] == 'done':
                complete = True
                self.send('overpass_done')
                break
            elif msg['type'] == 'error':
                self.error(msg['error'])
            else:
                self.status('from network: ' + repr(msg))
        return complete

    def merge_chunks(self, chunks):
        files = [os.path.join('overpass', chunk['filename'])
                 for chunk in chunks if chunk.get('oql')]

        cmd = ['osmium', 'merge'] + files + ['-o', self.place.overpass_filename]
        p = subprocess.run(cmd,
                           encoding='utf-8',
                           universal_newlines=True,
                           stderr=subprocess.PIPE,
                           stdout=subprocess.PIPE)
        msg = p.stdout if p.returncode == 0 else p.stderr
        if msg:
            self.status(msg)

    def run_osm2pgsql(self):
        self.status('running osm2pgsql')
        cmd = self.place.osm2pgsql_cmd()
        env = {'PGPASSWORD': app.config['DB_PASS']}
        subprocess.run(cmd, env=env, check=True)
        print('osm2pgsql done')
        self.status('osm2pgsql done')

    def load_isa(self):
        def progress(msg):
            self.status(msg)
            self.check_for_stop()
        self.status("downloading 'instance of' data for Wikidata items")
        self.place.load_isa(progress)
        self.status("Wikidata 'instance of' download complete")

    def run_matcher(self):
        def progress(candidates, item):
            num = len(candidates)
            noun = 'candidate' if num == 1 else 'candidates'
            count = f': {num} {noun} found'
            msg = item.label_and_qid() + count
            self.item_line(msg)
            self.check_for_stop()

        self.place.run_matcher(progress=progress,
                               want_isa=self.want_isa)

class RequestHandler(socketserver.BaseRequestHandler):
    def send_msg(self, msg):
        return chat.send_json(self.request, msg)

    def join_job(self):
        return

    def place_from_msg(self, msg):
        self.osm_type, self.osm_id = msg['osm_type'], msg['osm_id']
        self.place_tuple = (self.osm_type, self.osm_id)
        self.job_thread = active_jobs.get(self.place_tuple)

    def match_place(self, msg):
        t = threading.current_thread()
        job_need_start = False
        if not self.job_thread:
            job_need_start = True
            keys = ('user', 'remote_addr', 'user_agent',
                    'wikidata_chunk_size', 'overpass_chunk_size')
            kwargs = {key: msg.get(key) for key in keys}

            self.job_thread = MatcherJob(self.osm_type,
                                         self.osm_id,
                                         want_isa=set(msg.get('want_isa') or []),
                                         **kwargs)
            active_jobs[self.place_tuple] = self.job_thread

        status_queue = queue.Queue()
        updates = self.job_thread.subscribe(t.name, status_queue)

        if job_need_start:
            self.job_thread.start()

        while True:
            msg = updates.get()
            try:
                self.send_msg(msg)
                if msg['type'] in ('done', 'error'):
                    break
            except BrokenPipeError:
                self.job_thread.unsubscribe(t.name)
                break

    def stop_job(self):
        for t in threading.enumerate():
            if not isinstance(t, MatcherJob):
                continue
            print(t.osm_type, t.osm_id)
            if t.osm_type != self.osm_type or t.osm_id != self.osm_id:
                continue
            print('STOP')
            t.stop()

    def handle_message(self, msg):
        print(f'handle: {msg!r}')
        if msg == 'ping':
            self.send_msg({'type': 'pong'})
            return
        if msg.startswith('match'):
            json_msg = json.loads(msg[6:])
            self.place_from_msg(json_msg)
            return self.match_place(json_msg)
        if msg == 'jobs':
            job_list = []
            for t in threading.enumerate():
                if not isinstance(t, MatcherJob):
                    continue
                start = datetime.utcfromtimestamp(int(t.start_time))
                item = {
                    'osm_id': t.osm_id,
                    'osm_type': t.osm_type,
                    'subscribers': t.subscriber_count,
                    'start': str(start),
                    'stopping': t.stopping,
                }
                job_list.append(item)
            self.send_msg({'type': 'jobs', 'items': job_list})
            return
        if msg.startswith('stop'):
            json_msg = json.loads(msg[5:])
            self.place_from_msg(json_msg)
            self.stop_job()
            self.send_msg({'type': 'stop', 'success': True})

    def handle(self):
        print('New connection from %s:%s' % self.client_address)
        msg = chat.read_line(self.request)

        with app.app_context():
            try:
                return self.handle_message(msg)
            except Exception as e:
                error_str = f'{type(e).__name__}: {e}'
                self.send_msg({'type': 'error', 'msg': error_str})

                info = 'matcher queue'
                mail.send_traceback(info, prefix='matcher queue')

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

def main():
    HOST, PORT = "localhost", 6030

    overpass_thread = threading.Thread(target=process_queue_loop)
    overpass_thread.daemon = True
    overpass_thread.start()

    socketserver.ThreadingTCPServer.allow_reuse_address = True
    server = socketserver.ThreadingTCPServer((HOST, PORT), RequestHandler)
    ip, port = server.server_address

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.name = 'server thread'

    server_thread.start()
    print("Server loop running in thread:", server_thread.name)
    server_thread.join()


if __name__ == "__main__":
    main()

