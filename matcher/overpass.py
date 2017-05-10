#!/usr/bin/python3
import re
import requests
from time import sleep

re_slot_available = re.compile('^Slot available after: ([^,]+), in (\d+) seconds?\.$')

name_only_tag = {'area=yes', 'type=tunnel', 'leisure=park', 'leisure=garden',
        'site=aerodome', 'amenity=hospital', 'boundary', 'amenity=pub',
        'amenity=cinema', 'ruins', 'retail=retail_park',
        'amenity=concert_hall', 'amenity=theatre'}

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
        if ':' in tag or ' ' in tag:
            tag = '"{}"="{}"'.format(k, v)

    return ['\n    {}({})[{}]{};'.format(t, filters, tag, name_filter)
            for t in (('rel',) if relation_only else ('node', 'way', 'rel'))]

    # return ['\n    {}(area.a)[{}]{};'.format(t, tag, name_filter) for ('node', 'way', 'rel')]

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
