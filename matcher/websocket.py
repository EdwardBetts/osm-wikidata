from flask import Blueprint, current_app
from .place import Place
from . import wikipedia, database, wikidata, netstring
import re
import json
import socket
import subprocess
import os.path

ws = Blueprint('ws', __name__)
re_point = re.compile('^Point\((-?[0-9.]+) (-?[0-9.]+)\)$')
max_pin_count = 300

# TODO: different coloured icons
# - has enwiki article
# - match found
# - match not found

def build_item_list(items):
    item_list = []
    for qid, v in items.items():
        label = v['query_label']
        enwiki = v.get('enwiki')
        if enwiki and not enwiki.startswith(label + ','):
            label = enwiki
        lon, lat = re_point.match(v['location']).groups()
        item = {'qid': qid, 'label': label, 'lat': lat, 'lon': lon}
        if 'tags' in v:
            item['tags'] = list(v['tags'])
        item_list.append(item)
    return item_list

def overpass_request(ws_sock, place, chunks, status):
    host, port = 'localhost', 6020
    sock = socket.create_connection((host, port))
    sock.setblocking(True)

    fields = ['place_id', 'osm_id', 'osm_type']
    msg = {
        'place': {f: getattr(place, f) for f in fields},
        'chunks': chunks,
    }

    netstring.write(sock, json.dumps(msg))
    reply = netstring.read(sock)
    status(ws_sock, reply)
    while True:
        from_network = netstring.read(sock)
        if from_network is None:
            break
        status(ws_sock, 'from network: ' + from_network)
        netstring.write(sock, 'ack')
    status(ws_sock, 'socket closed')

def send(socket, data):
    socket.send(json.dumps(data))

def status(socket, msg):
    if not msg:
        return
    send(socket, {'msg': msg})

def item_line(socket, msg):
    if not msg:
        return
    send(socket, {'type': 'item', 'msg': msg})

def get_items(place):
    wikidata_items = place.items_from_wikidata(place.bbox)
    pins = build_item_list(wikidata_items)

    status('loading enwiki categories')
    wikipedia.add_enwiki_categories(wikidata_items)
    status('enwiki categories loaded')
    place.save_items(wikidata_items)
    status('items saved to database')

    place.state = 'tags'
    database.session.commit()

    return pins

def get_pins(place):
    if place.items.count() > max_pin_count:
        return []
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

def merge_chunks(ws_sock, place, chunks):
    files = [os.path.join('overpass', chunk['filename'])
             for chunk in chunks
             if chunk.get('oql')]

    cmd = ['osmium', 'merge'] + files + ['-o', place.overpass_filename]
    # status(' '.join(cmd))
    p = subprocess.run(cmd,
                       encoding='utf-8',
                       universal_newlines=True,
                       stderr=subprocess.PIPE,
                       stdout=subprocess.PIPE)
    status(ws_sock, p.stdout if p.returncode == 0 else p.stderr)

def run_osm2pgsql(place):
    cmd = place.osm2pgsql_cmd()
    env = {'PGPASSWORD': current_app.config['DB_PASS']}
    subprocess.run(cmd, env=env, check=True)
    # could echo osm2pgsql output via websocket

@ws.route('/matcher/<osm_type>/<int:osm_id>/run')
def ws_matcher(ws_sock, osm_type, osm_id):
    # idea: catch exceptions, then pass to pass to web page as status update
    # also e-mail them

    place = Place.get_by_osm(osm_type, osm_id)
    # place.state = 'tags'

    if not place:
        status(ws_sock, 'error: place not found')
        # FIXME - send error mail
        return

    if place.state == 'ready':
        status(ws_sock, 'error: place already ready')
        # FIXME - send error mail
        return

    if not place.state or place.state == 'refresh':
        pins = get_items(place)
    else:
        pins = get_pins(place)

    db_items = {item.qid: item for item in place.items}

    item_count = len(db_items)

    item_count_msg = '{:,d} Wikidata items found'.format(item_count)
    send_pins = item_count < max_pin_count
    if send_pins:
        send(ws_sock, {'pins': pins})
    status(ws_sock, item_count_msg + (', pins shown on map' if send_pins else ', too many to show on map'))

    def extracts_progress(item):
        msg = 'load extracts: ' + item.label_and_qid()
        item_line(ws_sock, msg)

    if place.state == 'tags':
        status(ws_sock, 'getting wikidata item details')
        for qid, entity in wikidata.entity_iter(db_items.keys()):
            item = db_items[qid]
            item.entity = entity
            item_line(ws_sock, 'load entity: ' + item.label_and_qid())
        item_line(ws_sock, 'wikidata entities loaded')

        status(ws_sock, 'loading wikipedia extracts')
        place.load_extracts(progress=extracts_progress)
        item_line(ws_sock, 'extracts loaded')

        place.state = 'wbgetentities'
        database.session.commit()

    chunks = place.get_chunks()

    empty = [chunk['num'] for chunk in chunks if not chunk['oql']]
    if empty:
        send(ws_sock, {'empty': empty})

    if place.overpass_done:
        status(ws_sock, 'using existing overpass data')
    else:
        status(ws_sock, 'downloading data from overpass')
        overpass_request(ws_sock, place, chunks, status)
        merge_chunks(ws_sock, place, chunks)

    if True:
        status(ws_sock, 'running osm2pgsql')
        run_osm2pgsql(place)
        status(ws_sock, 'osm2pgsql done')

    def progress(candidates, item):
        num = len(candidates)
        noun = 'candidate' if num == 1 else 'candidates'
        count = ': {num} {noun} found'.format(num=num, noun=noun)
        item_line(ws_sock, item.label_and_qid() + count)

    place.run_matcher(progress=progress)

    item_line(ws_sock, 'finished')
    send(ws_sock, {'type': 'done'})
