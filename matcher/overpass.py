#!/usr/bin/python3
import re
import requests
import os.path
import json
import simplejson
from .error_mail import send_error_mail
from flask import current_app, request
from time import sleep
from . import user_agent_headers

re_slot_available = re.compile('^Slot available after: ([^,]+), in (\d+) seconds?\.$')
re_available_now = re.compile('^\d+ slots available now.$')

name_only_tag = {'area=yes', 'type=tunnel', 'leisure=park', 'leisure=garden',
        'site=aerodome', 'amenity=hospital', 'boundary', 'amenity=pub',
        'amenity=cinema', 'ruins', 'retail=retail_park',
        'amenity=concert_hall', 'amenity=theatre', 'designation=civil_parish'}

name_only_key = ['place', 'landuse', 'admin_level', 'water', 'man_made',
        'railway', 'aeroway', 'bridge', 'natural']

def oql_from_tag(tag, large_area, filters='area.a'):
    if tag == 'highway':
        return []
    # optimisation: we only expect route, type or site on relations
    relation_only = tag == 'site'
    if large_area or tag in name_only_tag or any(tag.startswith(k) for k in name_only_key):
        name_filter = '[name]'
    else:
        name_filter = '[~"^(addr:housenumber|.*name.*)$"~".",i]'
    if '=' in tag:
        k, _, v = tag.partition('=')
        if tag == 'type=waterway' or k == 'route' or tag == 'type=route':
            return []  # ignore because osm2pgsql only does multipolygons
        if k in {'site', 'type', 'route'}:
            relation_only = True
        if not k.isalnum() or not v.isalnum():
            tag = '"{}"="{}"'.format(k, v)
    elif not tag.isalnum():
        tag = '"{}"'.format(tag)

    return ['\n    {}({})[{}]{};'.format(t, filters, tag, name_filter)
            for t in (('rel',) if relation_only else ('node', 'way', 'rel'))]

    # return ['\n    {}(area.a)[{}]{};'.format(t, tag, name_filter) for ('node', 'way', 'rel')]

def oql_from_wikidata_tag_or_key(tag_or_key, filters):
    osm_type, _, tag = tag_or_key.partition(':')
    osm_type = osm_type.lower()
    assert {'key': False, 'tag': True}[osm_type] == ('=' in tag)

    relation_only = tag == 'site'

    if tag in name_only_tag or any(tag.startswith(k) for k in name_only_key):
        name_filter = '[name]'
    else:
        name_filter = '[~"^(addr:housenumber|.*name.*)$"~".",i]'
    if osm_type == 'tag':
        k, _, v = tag.partition('=')
        if k in {'site', 'type', 'route'}:
            relation_only = True
        if not k.isalnum() or not v.isalnum():
            tag = '"{}"="{}"'.format(k, v)
    elif not tag.isalnum():
        tag = '"{}"'.format(tag)

    return ['\n    {}({})[{}]{};'.format(t, filters, tag, name_filter)
            for t in (('rel',) if relation_only else ('node', 'way', 'rel'))]

def parse_status(status):
    lines = status.splitlines()
    limit = 'Rate limit: '

    assert lines[0].startswith('Connected as: ')
    assert lines[1].startswith('Current time: ')
    assert lines[2].startswith(limit)

    slots = []
    for i in range(3, len(lines)):
        line = lines[i]
        if not line.startswith('Slot available after:'):
            break
        m = re_slot_available.match(line)
        slots.append(int(m.group(2)))

    next_line = lines[i]
    assert (re_available_now.match(next_line) or
            next_line == 'Currently running queries (pid, space limit, time limit, start time):')

    return {
        'rate_limit': int(lines[2][len(limit):]),
        'slots': slots,
        'running': len(lines) - (i + 1)
    }

def get_status():
    status = requests.get('https://overpass-api.de/api/status').text
    return parse_status(status)

def wait_for_slot(status=None):
    if status is None:
        status = get_status()
    slots = status['slots']
    if slots:
        print('waiting {} seconds'.format(slots[0]))
        sleep(slots[0] + 1)

def item_filename(wikidata_id, radius):
    assert wikidata_id[0] == 'Q'
    overpass_dir = current_app.config['OVERPASS_DIR']
    return os.path.join(overpass_dir, '{}_{}.json'.format(wikidata_id, radius))

def item_query(oql, wikidata_id, radius=1000, refresh=False):
    filename = item_filename(wikidata_id, radius)

    if not refresh and os.path.exists(filename):
        return json.load(open(filename))['elements']

    overpass_url = 'https://overpass-api.de/api/interpreter'
    r = requests.post(overpass_url, data=oql, headers=user_agent_headers())

    try:
        data = r.json()
    except simplejson.scanner.JSONDecodeError:
        send_error_mail('item overpass query error', '''
URL: {}
wikidata ID: {}
status code: {}

oql:
{}

reply:
{}
        '''.format(request.url, wikidata_id, r.status_code, oql, r.text))
        return []

    json.dump(data, open(filename, 'w'))
    return data['elements']

def get_existing(wikidata_id):
    oql = '''
[timeout:300][out:json];
(node[wikidata={qid}]; way[wikidata={qid}]; rel[wikidata={qid}];);
out qt center tags;
'''.format(qid=wikidata_id)

    overpass_url = 'https://overpass-api.de/api/interpreter'
    r = requests.post(overpass_url, data=oql, headers=user_agent_headers())

    return r.json()['elements']

def get_tags(elements):
    union = {'{}({});\n'.format({'relation': 'rel'}.get(i.osm_type, i.osm_type), i.osm_id)
             for i in elements}

    oql = '''
[timeout:300][out:json];
({});
out qt tags;
'''.format(''.join(union))

    overpass_url = 'https://overpass-api.de/api/interpreter'
    r = requests.post(overpass_url, data=oql, headers=user_agent_headers())

    return r.json()['elements']
