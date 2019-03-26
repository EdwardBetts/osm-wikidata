from flask import Blueprint, g
from time import time, sleep
from .place import Place, bbox_chunk
from . import wikipedia, database, wikidata, utils, edit, mail
from flask_login import current_user
from .model import ItemCandidate, ChangesetEdit
from datetime import datetime
from lxml import etree
from sqlalchemy.orm.attributes import flag_modified
import requests
import re
import json
import os.path
import shutil

ws = Blueprint('ws', __name__)
re_point = re.compile(r'^Point\(([-E0-9.]+) ([-E0-9.]+)\)$')

# TODO: different coloured icons
# - has enwiki article
# - match found
# - match not found

class VersionMismatch(Exception):
    pass

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
        num = 0
        while chunks:
            bbox = chunks.pop()
            num += 1
            msg = f'requesting wikidata chunk {num}'
            print(msg)
            self.status(msg)
            try:
                items.update(self.place.bbox_wikidata_items(bbox))
            except wikidata.QueryTimeout:
                msg = f'wikidata timeout, splitting chunk {num} info four'
                print(msg)
                self.status(msg)
                chunks += bbox_chunk(bbox, 2)

        return items

    def get_items_bbox(self):
        place = self.place
        chunk_size = place.wikidata_chunk_size()
        if chunk_size == 1:
            print('wikidata unchunked')
            try:
                wikidata_items = place.bbox_wikidata_items()
            except wikidata.QueryTimeout:
                place.wikidata_query_timeout = True
                database.session.commit()
                chunk_size = 2
                msg = 'wikidata query timeout, retrying with smaller chunks.'
                self.status(msg)

        if chunk_size != 1:
            chunks = list(place.polygon_chunk(size=32))

            msg = f'downloading wikidata in {len(chunks)} chunks'
            self.status(msg)
            wikidata_items = self.wikidata_chunked(chunks)

        return wikidata_items

    def get_items_point(self):
        return self.place.point_wikidata_items()

    def get_items(self):
        self.send('get_wikidata_items')
        print('items from wikidata')

        if self.place.is_point:
            wikidata_items = self.get_items_point()
        else:
            wikidata_items = self.get_items_bbox()

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

        self.place.save_items(wikidata_items)
        print('items saved')
        self.send('items_saved')

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

    def run_matcher(self):
        def progress(candidates, item):
            num = len(candidates)
            noun = 'candidate' if num == 1 else 'candidates'
            count = f': {num} {noun} found'
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

    if not db_items:
        m.item_line('finished')
        place.state = 'ready'
        database.session.commit()
        print('done')
        m.send('done')
        m.mark_log_good()

    if place.state == 'tags':
        m.get_item_detail(db_items)
        place.state = 'load_isa'
        database.session.commit()

    if place.state == 'load_isa':
        m.status('running matcher')
        m.run_matcher()
        place.state = 'ready'
        database.session.commit()

    if place.state == 'refresh_isa':
        m.status('adding item type information')
        place.load_isa()
        place.state = 'ready'
        database.session.commit()

    m.item_line('finished')
    place.state = 'ready'
    database.session.commit()
    print('done')
    m.send('done')
    m.mark_log_good()

def add_wikipedia_tag(root, m):
    if 'wiki_lang' not in m or root.find('.//tag[@k="wikipedia"]') is not None:
        return
    key = 'wikipedia:' + m['wiki_lang']
    value = m['wiki_title']
    existing = root.find(f'.//tag[@k="{key}"]')
    if existing is not None:
        existing.set('v', value)
        return
    tag = etree.Element('tag', k=key, v=value)
    root[0].append(tag)

@ws.route('/websocket/matcher/<osm_type>/<int:osm_id>')
def ws_matcher(ws_sock, osm_type, osm_id):
    # idea: catch exceptions, then pass to pass to web page as status update
    # also e-mail them

    print('websocket')
    place = None

    try:
        place = Place.get_by_osm(osm_type, osm_id)
        if place.state == 'ready':
            log_filename = utils.find_log_file(place)
            if log_filename:
                print('replaying log:', log_filename)
                replay_log(ws_sock, log_filename)
                return

        m = MatcherSocket(ws_sock, place)
        return run_matcher(place, m)
    except Exception as e:
        msg = type(e).__name__ + ': ' + str(e)
        print(msg)
        ws_sock.send(json.dumps({'type': 'error', 'msg': msg}))

        g.user = current_user

        if place:
            name = place.display_name
        else:
            name = 'unknown place'
        info = f'''
place: {name}
https://openstreetmap.org/{osm_type}/{osm_id}

exception in matcher websocket
'''
        mail.send_traceback(info)

def process_match(ws_sock, changeset_id, m):
    osm_type, osm_id = m['osm_type'], m['osm_id']
    item_id = m['qid'][1:]

    r = edit.get_existing(osm_type, osm_id)
    if r.status_code == 410 or r.content == b'':
        return 'deleted'

    osm = (ItemCandidate.query
                        .filter_by(item_id=item_id,
                                   osm_type=osm_type,
                                   osm_id=osm_id)
                        .one_or_none())

    if b'wikidata' in r.content:
        root = etree.fromstring(r.content)
        existing = root.find('.//tag[@k="wikidata"]')
        if existing is not None:
            osm.tags['wikidata'] = existing.get('v')
            flag_modified(osm, 'tags')
        database.session.commit()
        return 'already_tagged'

    root = etree.fromstring(r.content)
    tag = etree.Element('tag', k='wikidata', v=m['qid'])
    root[0].set('changeset', changeset_id)
    root[0].append(tag)

    add_wikipedia_tag(root, m)

    element_data = etree.tostring(root)
    try:
        edit.save_element(osm_type, osm_id, element_data)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409 and 'Version mismatch' in r.text:
            raise VersionMismatch
        mail.error_mail('error saving element',
                        element_data.decode('utf-8'),
                        e.response)
        database.session.commit()
        return 'element-error'

    osm.tags['wikidata'] = m['qid']
    flag_modified(osm, 'tags')
    # TODO: also update wikipedia tag if appropriate
    db_edit = ChangesetEdit(changeset_id=changeset_id,
                            item_id=item_id,
                            osm_id=osm_id,
                            osm_type=osm_type)
    database.session.add(db_edit)
    database.session.commit()

    return 'saved'

@ws.route('/websocket/add_tags/<osm_type>/<int:osm_id>')
def ws_add_tags(ws_sock, osm_type, osm_id):
    g.user = current_user

    def send(msg_type, **kwars):
        ws_sock.send(json.dumps({'type': msg_type, **kwars}))

    place = None
    try:
        place = Place.get_by_osm(osm_type, osm_id)

        data = json.loads(ws_sock.receive())
        comment = data['comment']
        changeset = edit.new_changeset(comment)
        r = edit.create_changeset(changeset)
        changeset_id = r.text.strip()
        if not changeset_id.isdigit():
            send('changeset-error', msg='error opening changeset')
            return

        send('open', id=int(changeset_id))

        update_count = 0
        change = edit.record_changeset(id=changeset_id,
                                       place=place,
                                       comment=comment,
                                       update_count=update_count)

        for num, m in enumerate(data['matches']):
            send('progress', qid=m['qid'], num=num)
            while True:
                try:
                    result = process_match(ws_sock, changeset_id, m)
                except VersionMismatch:  # FIXME: limit number of attempts
                    continue  # retry
                else:
                    break
            if result == 'saved':
                update_count += 1
                change.update_count = update_count
            database.session.commit()
            send(result, qid=m['qid'], num=num)

        send('closing')
        edit.close_changeset(changeset_id)
        send('done')

    except Exception as e:
        msg = type(e).__name__ + ': ' + str(e)
        print(msg)
        send('error', msg=msg)

        if place:
            name = place.display_name
        else:
            name = 'unknown place'
        info = f'''
place: {name}
https://openstreetmap.org/{osm_type}/{osm_id}

exception in add tags websocket
'''
        mail.send_traceback(info)
