#!/usr/bin/python3
import re
import requests
import os.path
import json
import simplejson
from flask import current_app
from time import sleep
from . import user_agent_headers, mail
from collections import defaultdict

re_slot_available = re.compile(r'^Slot available after: ([^,]+), in (-?\d+) seconds?\.$')
re_available_now = re.compile(r'^\d+ slots available now.$')

name_only_tag = {'area=yes', 'type=tunnel', 'leisure=park', 'leisure=garden',
        'site=aerodome', 'amenity=hospital', 'boundary', 'amenity=pub',
        'amenity=cinema', 'ruins', 'retail=retail_park',
        'amenity=concert_hall', 'amenity=theatre', 'designation=civil_parish'}

name_only_key = ['place', 'landuse', 'admin_level', 'water', 'man_made',
        'railway', 'aeroway', 'bridge', 'natural']

def endpoint():
    return current_app.config['OVERPASS_URL'] + '/api/interpreter'

class RateLimited(Exception):
    pass

class Timeout(Exception):
    pass

class OverpassError(Exception):
    def __init__(self, r):
        self.r = r

def run_query(oql, error_on_rate_limit=True):
    r = requests.post(endpoint(),
                      data=oql.encode('utf-8'),
                      headers=user_agent_headers())

    if (error_on_rate_limit and
            r.status_code == 429 and
            'rate_limited' in r.text):
        mail.error_mail('items_as_xml: overpass rate limit', oql, r)
        raise RateLimited

    return r

def get_elements(oql):
    return run_query(oql).json()['elements']

def name_only(t):
    return (t in name_only_tag or
            ('=' in t and any(t.startswith(key + '=') for key in name_only_key)))

def get_name_filter(tags):
    return ('[name]'
            if all(name_only(t) for t in tags)
            else '[~"^(addr:housenumber|.*name.*)$"~".",i]')

def oql_for_point(lat, lon, radius, tags, buildings):
    union = []

    for key, values in sorted(group_tags(tags).items()):
        u = oql_point_element_filter(key, values, filters='.a')
        if u:
            union.append(u)
    name_filter = get_name_filter(tags)

    if buildings:
        oql_building = f'nwr.a["building"][~"^(addr:housenumber|.*name.*)$"~"{buildings}",i];'
    else:
        oql_building = ''

    oql_template = '''
[timeout:600][out:xml];
nwr(around:{radius},{lat},{lon})->.a;
(
{tags}
) -> .b;
(
    nwr.a["wikidata"];
    nwr.a["addr:housenumber"];
    nwr.b{name_filter};
    nwr.b[~"^ref:"~"."];
    {oql_building}
);
(._;>;);
out;'''
    return oql_template.format(lat=lat,
                               lon=lon,
                               radius=radius,
                               tags='\n'.join(union),
                               name_filter=name_filter,
                               oql_building=oql_building)

def oql_for_area(overpass_type, osm_id, tags, bbox, buildings, include_self=True):
    union = []

    for key, values in sorted(group_tags(tags).items()):
        u = oql_element_filter(key, values)
        if u:
            union.append(u)

    if overpass_type == 'node':
        area_id = None
    else:
        area_id = int(osm_id) + {'way': 2400000000, 'rel': 3600000000}[overpass_type]

    name_filter = get_name_filter(tags)

    if buildings:
        oql_building = f'nwr(area.a)["building"][~"^(addr:housenumber|.*name.*)$"~"{buildings}",i];'
    else:
        oql_building = ''

    self = '    {}({});'.format(overpass_type, osm_id) if include_self else ''

    oql_template = '''
[timeout:600][out:xml][bbox:{bbox}];
area({area_id}) -> .a;
(
{tags}
) -> .b;
(
    {self}
    nwr(area.a)["wikidata"];
    nwr(area.a)["addr:housenumber"];
    nwr.b{name_filter};
    nwr.b[~"^ref:"~"."];
    {oql_building}
);
(._;>;);
out;'''
    return oql_template.format(bbox=bbox,
                               area_id=area_id,
                               tags='\n'.join(union),
                               self=self,
                               name_filter=name_filter,
                               oql_building=oql_building)

def group_tags(tags):
    '''given a list of keys and tags return a dict group by key'''
    ret = defaultdict(list)
    for tag_or_key in tags:
        if '=' in tag_or_key:
            key, _, value = tag_or_key.partition('=')
            ret[key].append(value)
        else:
            ret[tag_or_key] = []
    return dict(ret)

def oql_element_filter(key, values, filters='area.a'):
    # optimisation: we only expect route, type or site on relations
    relation_only = key in {'site', 'type', 'route'}

    if values:
        if len(values) == 1:
            tag = '"{}"="{}"'.format(key, values[0])
        else:
            tag = '"{}"~"^({})$"'.format(key, '|'.join(values))
    else:
        tag = '"{}"'.format(key)

    t = 'rel' if relation_only else 'nwr'
    return '{}({})[{}];'.format(t, filters, tag.replace('␣', ' '))

def oql_point_element_filter(key, values, filters=''):
    # optimisation: we only expect route, type or site on relations
    relation_only = key in {'site', 'type', 'route'}

    if values:
        if len(values) == 1:
            tag = '"{}"="{}"'.format(key, values[0])
        else:
            tag = '"{}"~"^({})$"'.format(key, '|'.join(values))
    else:
        tag = '"{}"'.format(key)

    t = 'rel' if relation_only else 'nwr'
    return '{}{}[{}];'.format(t, filters, tag.replace('␣', ' '))

def oql_from_tag(tag, filters='area.a'):
    if tag == 'highway':
        return []
    # optimisation: we only expect route, type or site on relations
    relation_only = tag == 'site'

    name_filter = get_name_filter([tag])

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
    if not {'key': False, 'tag': True}[osm_type] == ('=' in tag):
        return []

    relation_only = tag == 'site'

    name_filter = get_name_filter([tag])

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

def parse_status(r):
    lines = r.text.splitlines()
    limit = 'Rate limit: '

    try:
        assert lines[0].startswith('Connected as: ')
        assert lines[1].startswith('Current time: ')
        assert lines[2].startswith(limit)
    except AssertionError:
        raise OverpassError(r)

    slots = []
    for i in range(3, len(lines)):
        line = lines[i]
        if not line.startswith('Slot available after:'):
            break
        m = re_slot_available.match(line)
        if not m:
            raise OverpassError(r)
        slots.append(int(m.group(2)))

    next_line = lines[i]
    assert (re_available_now.match(next_line) or
            next_line == 'Currently running queries (pid, space limit, time limit, start time):')

    return {
        'rate_limit': int(lines[2][len(limit):]),
        'slots': slots,
        'running': len(lines) - (i + 1)
    }

def status_url():
    return current_app.config['OVERPASS_URL'] + '/api/status'

def get_status(url=None):
    r = requests.get(url or status_url(), timeout=10)
    if '502 Bad Gateway' in r.text:
        raise OverpassError(r)
    return parse_status(r)

def wait_for_slot(status=None, url=None):
    if status is None:
        status = get_status(url=url)
    slots = status['slots']
    if slots:
        print('waiting {} seconds'.format(slots[0]))
        sleep(slots[0] + 1)

def item_filename(wikidata_id, radius):
    assert wikidata_id[0] == 'Q'
    overpass_dir = current_app.config['OVERPASS_DIR']
    return os.path.join(overpass_dir, '{}_{}.json'.format(wikidata_id, radius))

def existing_item_filename(wikidata_id):
    assert wikidata_id[0] == 'Q'
    overpass_dir = current_app.config['OVERPASS_DIR']
    return os.path.join(overpass_dir, '{}_existing.json'.format(wikidata_id))

def item_query(oql, wikidata_id, radius=1000, refresh=False):
    filename = item_filename(wikidata_id, radius)

    if not refresh and os.path.exists(filename):
        return json.load(open(filename))['elements']

    r = run_query(oql)

    if len(r.content) < 2000 and b'<title>504 Gateway' in r.content:
        mail.error_mail('item query: overpass 504 gateway timeout', oql, r)
        raise Timeout

    try:
        data = r.json()
    except simplejson.scanner.JSONDecodeError:
        mail.error_mail('item overpass query error', oql, r)
        raise

    json.dump(data, open(filename, 'w'))
    return data['elements']

def get_existing(wikidata_id, refresh=False):
    filename = existing_item_filename(wikidata_id)

    if not refresh and os.path.exists(filename):
        return json.load(open(filename))['elements']

    oql = '''
[timeout:300][out:json];
(node[wikidata={qid}]; way[wikidata={qid}]; rel[wikidata={qid}];);
out qt center tags;
'''.format(qid=wikidata_id)

    r = run_query(oql)

    if len(r.content) < 2000 and b'<title>504 Gateway' in r.content:
        mail.error_mail('item query: overpass 504 gateway timeout', oql, r)
        raise Timeout

    try:
        data = r.json()
    except simplejson.scanner.JSONDecodeError:
        mail.error_mail('item overpass query error', oql, r)
        raise

    json.dump(data, open(filename, 'w'))
    return data['elements']

def get_tags(elements):
    union = {'{}({});\n'.format({'relation': 'rel'}.get(i.osm_type, i.osm_type), i.osm_id)
             for i in elements}

    oql = '''
[timeout:300][out:json];
({});
out qt tags;
'''.format(''.join(union))

    return get_elements(oql)

def run_query_persistent(oql, attempts=3, via_web=True):
    for attempt in range(attempts):
        wait_for_slot()
        print('calling overpass')
        r = run_query(oql, error_on_rate_limit=False)
        if r is None:
            seconds = 30
            print('retrying, waiting {} seconds'.format(seconds))
            sleep(seconds)
            continue
        if len(r.content) < 2000 and b'<remark> runtime error:' in r.content:

            msg = 'runtime error'
            mail.error_mail(msg, oql, r, via_web=via_web)
            print(msg)
            if b'<remark> runtime error: Query run out of memory' in r.content:
                return
            continue  # retry

        if len(r.content) < 2000 and b'<title>504 Gateway' in r.content:
            msg = 'overpass timeout'
            mail.error_mail(msg, oql, r, via_web=via_web)
            print(msg)
            continue  # retry

        return r

def items_as_xml(items):
    assert items
    union = ''
    for item, osm in items:
        union += '{}({});\n'.format(osm.osm_type, osm.osm_id)

    oql = '({});(._;>);out meta;'.format(union)

    return run_query(oql).content

def is_in(osm_type, osm_id):
    oql = f'''
[out:json][timeout:25];
{osm_type}({osm_id});
(._;>);
is_in->.a;
(way(pivot.a); rel(pivot.a););
out bb tags qt;'''

    return get_elements(oql)

def is_in_lat_lon(lat, lon):
    oql = f'''
[out:json][timeout:25];
is_in({lat},{lon})->.a;
(way(pivot.a); rel(pivot.a););
out bb tags qt;'''

    try:
        return get_elements(oql)
    except simplejson.errors.JSONDecodeError:
        return None
