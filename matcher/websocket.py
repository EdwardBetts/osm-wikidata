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

def overpass_request(place, chunks, status):
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
    status(reply)
    while True:
        from_network = netstring.read(sock)
        if from_network is None:
            break
        status('from network: ' + from_network)
        netstring.write(sock, 'ack')
    status('socket closed')

@ws.route('/matcher/<osm_type>/<int:osm_id>/run')
def ws_matcher(socket, osm_type, osm_id):

    def send(data):
        socket.send(json.dumps(data))

    def status(msg, echo=True):
        if not msg:
            return
        if echo:
            print(msg)
        send({'msg': msg})

    place = Place.get_or_abort(osm_type, osm_id)
    # status('place found')

    items = place.items_from_wikidata(place.bbox)
    if len(items) < 300:
        send({'pins': build_item_list(items)})
        status('{:,d} Wikidata items found, pins shown on map'.format(len(items)))
    else:
        status('{:,d} Wikidata items found, too many to show on map'.format(len(items)))

    if False:
        status('loading enwiki categories')
        wikipedia.add_enwiki_categories(items)
        status('enwiki categories loaded')
        db_items = place.save_items(items, debug=status)
        status('items saved to database')

        for qid, entity in wikidata.entity_iter(db_items.keys()):
            db_items[qid].entity = entity

    place.state = 'wbgetentities'
    database.session.commit()

    chunks = place.get_chunks()

    empty = [chunk['num'] for chunk in chunks if not chunk['oql']]
    if empty:
        send({'empty': empty})

    if place.overpass_done:
        status('using existing overpass data')
    else:
        status('downloading data from overpass')
        overpass_request(place, chunks, status)

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
        if p.returncode == 0:
            status(p.stdout)
        else:
            status(p.stderr)

    if False:
        status('running osm2pgsql')
        cmd = place.osm2pgsql_cmd()
        env = {'PGPASSWORD': current_app.config['DB_PASS']}

        p = subprocess.Popen(cmd,
                             env=env,
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             universal_newlines=True)
        for line in p.stdout:
            if line.endswith('\n'):
                line = line[:-1]
            # status(line, echo=False)

        status('osm2pgsql done')

    def progress(candidates, item):
        msg = {
            'type': 'match',
            'candidate_count': len(candidates),
            'qid': item.qid,
            'enwiki': item.enwiki,
            'categories': item.categories,
            'query_label': item.query_label,
            'extract_names': item.extract_names,
            'wikidata_uri': item.wikidata_uri,
        }
        socket.send(json.dumps(msg))

    place.run_matcher(progress=progress)

    socket.send(json.dumps({'type': 'done'}))
