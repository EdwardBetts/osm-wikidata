#!/usr/bin/python3

from flask import Flask, render_template, request, Response, current_app
from collections import defaultdict
from .match import check_for_match, get_wikidata_names
from .utils import cache_filename, load_from_cache, cache_dir
from .wikipedia import get_items_with_cats
from .wikidata import wbgetentities
from .overpass import generate_oql, overpass_done, overpass_filename

import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import subprocess
import os.path
import requests
import json
import re

app = Flask(__name__)

def simplify_tags(tags):
    key_only = sorted(t for t in tags if '=' not in t)
    for k in key_only:
        for t in set(tags):
            if t.startswith(k + '='):
                tags.remove(t)
    return tags

def build_cat_map():
    cat_to_entity = {}
    data_dir = current_app.config['DATA_DIR']
    filename = os.path.join(data_dir, 'entity_types.json')
    for i in json.load(open(filename)):
        for c in i['cats']:
            lc_cat = c.lower()
            if ' by ' in lc_cat:
                lc_cat = lc_cat[:lc_cat.find(' by ')]
            cat_to_entity[lc_cat] = i
    return cat_to_entity

def nominatim_lookup(q):
    url = 'http://nominatim.openstreetmap.org/search'

    params = {
        'q': q,
        'format': 'jsonv2',
        'addressdetails': 1,
        'email': current_app.config['ADMIN_EMAIL'],
        'extratags': 1,
        'limit': 20,
        'namedetails': 1,
        'accept-language': 'en',
    }
    r = requests.get(url, params=params)
    results = []
    for hit in r.json():
        results.append(hit)
        if hit.get('osm_type') == 'relation':
            osm_id = hit['osm_id']
            filename = cache_filename('{}_nominatim.json'.format(osm_id))
            out = open(filename, 'w')
            json.dump(hit, out, indent=2)
            out.close()
    return results

def find_tags(items):
    all_tags = set()

    cat_to_entity = build_cat_map()
    for item in items.values():
        if not item.get('cats'):
            continue

        tags = set()
        for cat in item['cats']:
            lc_cat = cat.lower()
            for key, value in cat_to_entity.items():
                pattern = re.compile(r'\b' + re.escape(key) + r'\b')
                if pattern.search(lc_cat):
                    tags |= set(value['tags'])
        item['tags'] = sorted(tags)
        all_tags |= tags
    return sorted(simplify_tags(all_tags))

def db_config(param):
    return current_app.config['DB_{}'.format(param.upper())]

def get_db_name(osm_id):
    return '{}{}'.format(current_app.config['DB_PREFIX'], osm_id)

def load_into_pgsql(osm_id):
    cmd = ['osm2pgsql', '--create', '--drop', '--slim',
            '--hstore-all', '--hstore-add-index',
            '--cache', '1000',
            '--multi-geometry',
            '--host', current_app.config['DB_HOST'],
            '--username', current_app.config['DB_USER'],
            '--database', get_db_name(osm_id),
            overpass_filename(osm_id)]

    p = subprocess.run(cmd,
                       stderr=subprocess.PIPE,
                       env={'PGPASSWORD': current_app.config['DB_PASS']})
    if p.returncode != 0:
        if b'Out of memory' in p.stderr:
            return 'out of memory'
        else:
            return p.stderr
    return

def db_connect(dbname):
    return psycopg2.connect(dbname=dbname,
                            user=db_config('user'),
                            password=db_config('pass'),
                            host=db_config('host'))

def create_database(dbname):
    conn = db_connect('postgres')
    # set the isolation level so we can create a new database
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    try:
        cur.execute('create database {}'.format(dbname))
    except psycopg2.ProgrammingError as e:  # already exists
        if e.args[0] != 'database "{}" already exists\n'.format(dbname):
            # print(repr(e.value))
            raise
    cur.close()
    conn.close()

    conn = db_connect(dbname)
    cur = conn.cursor()
    try:
        cur.execute('create extension hstore')
    except psycopg2.ProgrammingError as e:
        if e.args[0] != 'extension "hstore" already exists\n':
            raise
        conn.rollback()
    try:
        cur.execute('create extension postgis')
    except psycopg2.ProgrammingError as e:
        if e.args[0] != 'extension "postgis" already exists\n':
            raise
        conn.rollback()
    conn.commit()

    cur.execute("select table_name from information_schema.tables where table_schema = 'public'")
    tables = {t[0] for t in cur.fetchall()}

    cur.close()
    conn.close()

    return tables

def build_hstore_query(tags):
    tags = [tuple(tag.split('=')) if ('=' in tag) else (tag, None)
            for tag in tags]
    return ' or '.join("((tags->'{}') = '{}')".format(k, v)
                       if v else "(tags ? '{}')".format(k)
                       for k, v in tags)

def get_osm_id_and_type(source_type, source_id):
    if source_type == 'point':
        return ('node', source_id)
    if source_id > 0:
        return ('way', source_id)
    return ('relation', -source_id)

def find_matches(items, conn):
    bad_name_fields = {'tiger:name_base', 'old_name', 'name:right', 'name:left',
                       'gnis:county_name', 'openGeoDB:name'}

    cur = conn.cursor()
    seen_wikidata = set()
    assert isinstance(items, list)
    items.sort(key=lambda i: int(i['qid'][1:]))
    found = []
    for num, item in enumerate(items):
        if 'tags' not in item or not item['tags']:
            continue
        # print(num, item['qid'], item['label'])
        candidates = []
        cats = item['cats']
        # cats = {p[0] for p in item['cat_paths']}
        # item['cats'] = cats
        assert item['qid'] not in seen_wikidata
        seen_wikidata.add(item['qid'])
        hstore_query = build_hstore_query(item['tags'])
        item['names'] = dict(get_wikidata_names(item))
        point = "ST_TRANSFORM(ST_SETSRID(ST_MAKEPOINT({}, {}),4326), 3857)".format(item['lon'], item['lat'])

        # item_max_dist = max(max_dist[cat] for cat in item['cats'])
        item_max_dist = 4  # FIXME

        sql_list = []
        for obj_type in 'point', 'line', 'polygon':
            obj_sql = ('select \'{}\', osm_id, name, tags, '
                       'ST_Distance({}, way) as dist '
                       'from planet_osm_{} '
                       'where ST_DWithin({}, way, {} * 1000)').format(obj_type, point, obj_type, point, item_max_dist)
            sql_list.append(obj_sql)
        sql = 'select * from (' + ' union '.join(sql_list) + ') a where ({}) order by dist'.format(hstore_query)

        cur.execute(sql)
        rows = cur.fetchall()
        seen = set()

        for osm_num, (src_type, src_id, osm_name, osm_tags, dist) in enumerate(rows):
            (osm_type, osm_id) = get_osm_id_and_type(src_type, src_id)
            if (obj_type, osm_id) in seen:
                continue
            seen.add((obj_type, osm_id))

            try:
                admin_level = int(osm_tags['admin_level']) if 'admin_level' in osm_tags else None
            except Exception:
                admin_level = None
            names = {k: v for k, v in osm_tags.items() if 'name' in k and k not in bad_name_fields}
            if any(c.startswith('Cities ') for c in cats) and admin_level == 10:
                continue
            if not names:
                continue

            match = check_for_match(osm_tags, item)
            if not match:
                continue
            candidate = {
                'type': osm_type,
                'id': osm_id,
                'name': osm_name,
                'tags': osm_tags,
                'dist': dist,
                'match': match.name,
                'planet_table': src_type,
                'src_id': src_id,
            }
            candidates.append(candidate)
        if candidates:
            item['candidates'] = candidates
            found.append(item)
    return found

def planet_table_id(osm):
    if osm['type'] == 'node':
        return ('point', osm['id'])
    table = 'polygon' if 'way_area' in osm['tags'] else 'line'
    return (table, osm['id'] if osm['type'] == 'way' else -osm['id'])

def get_biggest_polygon(item):
    biggest = None
    biggest_size = None
    for osm in item['candidates']:
        if osm['type'] not in {'way', 'relation'}:
            continue
        if 'way_area' not in osm['tags']:
            continue
        area = float(osm['tags']['way_area'])
        if biggest is None or area > biggest_size:
            biggest_size = area
            biggest = osm

    return -osm['id'] if osm['type'] == 'relation' else osm['id']

def all_in_one(item, conn):
    cur = conn.cursor()
    biggest = get_biggest_polygon(item)
    if not biggest:
        return
    sql_list = []

    for table in 'point', 'line', 'polygon':
        id_list = ','.join(str(osm['src_id']) for osm in item['candidates']
                       if osm['table'] == table and (table == 'point' or osm['src_id'] != biggest))

        if not id_list:
            continue
        obj_sql = ('select \'{}\' as t, osm_id, way '
                   'from planet_osm_{} '
                   'where osm_id in ({})').format(table, table, id_list)
        sql_list.append(obj_sql)

    if not sql_list:
        return
    sql = 'select ST_Within(a.way, b.way) from (' + ' union '.join(sql_list) + ') a, planet_osm_polygon b where b.osm_id={}'.format(biggest)
    cur.execute(sql)
    if all(row[0] for row in cur.fetchall()):
        return biggest

def filter_candidates(items, conn):
    assert isinstance(items, list)
    for item in items[:]:
        candidates = item['candidates']
        for osm in candidates:
            osm['table'], osm['src_id'] = planet_table_id(osm)

        for line in candidates[:]:
            if line['table'] == 'line':
                if any(poly['table'] == 'polygon' and poly['src_id'] == line['src_id'] for poly in candidates):
                    candidates.remove(line)

        if not candidates:
            items.remove(item)
            continue

        item['candidates'] = candidates
        if len(candidates) == 1:
            continue

        re_place_cat = re.compile(r'\b(Districts|Areas|Cities|Towns|Villages|Airports)\b', re.I)
        if any(re_place_cat.search(cat) for cat in item['cats']):
            nodes = [osm for osm in candidates if osm['type'] == 'node']
            if len(nodes) == 1:
                candidates = nodes

        if len(candidates) == 1:
            item['candidates'] = candidates
            continue

        big = all_in_one(item, conn)
        if big:
            for osm in candidates:
                if osm['table'] == 'polygon' and osm['src_id'] == big:
                    candidates = [osm]
                    break
        if len(candidates) == 1:
            item['candidates'] = candidates
            continue
    return items

def filter_candidates_more(items):
    items2 = []
    osm_count = defaultdict(list)
    for item in items:
        if len(item['candidates']) != 1:
            continue
        osm = item['candidates'][0]
        item['osm'] = osm
        if 'wikidata' in item['osm']['tags']:
            continue
        items2.append(item)
        osm_count[(osm['type'], osm['id'])].append(item)
    for k, v in osm_count.items():
        if len(v) > 1:
            # print (k, len(v))
            for item in v:
                items2.remove(item)
    return items2

@app.route("/overpass/<int:osm_id>", methods=["POST"])
def post_overpass(osm_id):
    filename = overpass_filename(osm_id)
    out = open(filename, 'wb')
    out.write(request.data)
    out.close()
    return Response('done', mimetype='text/plain')

def run_matcher(osm_id):
    filename = cache_filename('{}_candidates.json'.format(osm_id))
    if os.path.exists(filename):
        candidates = json.load(open(filename))
        return candidates  # already filtered

    dbname = get_db_name(osm_id)
    conn = db_connect(dbname)
    psycopg2.extras.register_hstore(conn)

    items = load_from_cache('{}_wbgetentities.json'.format(osm_id))
    candidates = find_matches(list(items.values()), conn)
    candidates = filter_candidates(candidates, conn)
    json.dump(candidates, open(filename, 'w'), indent=2)

    conn.close()
    return candidates

@app.route('/candidates/<int:osm_id>')
def candidates(osm_id):
    wikidata_item = load_from_cache('{}_nominatim.json'.format(osm_id))

    items = get_items_with_cats(osm_id)

    all_tags = find_tags(items)
    oql = generate_oql(osm_id, all_tags)

    items = wbgetentities(osm_id, items)

    # get_from_overpass(osm_id, oql)
    dbname = 'gis_{}'.format(osm_id)
    tables = create_database(dbname)

    expect = {'spatial_ref_sys', 'geography_columns', 'geometry_columns',
              'raster_overviews', 'planet_osm_roads', 'raster_columns',
              'planet_osm_line', 'planet_osm_point', 'planet_osm_polygon'}
    if tables != expect:
        error = load_into_pgsql(osm_id)
        if error:
            return 'osm2pgsql error: ' + error

    candidates = run_matcher(osm_id)

    re_place_cat = re.compile(r'\b(Districts|Areas|Cities|Towns|Villages|Airports)\b', re.I)
    for item in candidates:
        if any(re_place_cat.search(cat) for cat in item['cats']):
            item['is_place'] = True

    # return render_template('wikidata_items.html',
    return render_template('candidates.html',
                           items=items,
                           hit=wikidata_item,
                           oql=oql,
                           candidates=candidates,
                           all_tags=all_tags)

@app.route('/load/<int:osm_id>/wbgetentities', methods=['POST'])
def load_wikidata(osm_id):
    items = get_items_with_cats(osm_id)
    find_tags(items)
    wbgetentities(osm_id, items)
    return 'done'

@app.route('/load/<int:osm_id>/checkover_pass', methods=['POST'])
def check_overpass(osm_id):
    reply = 'got' if overpass_done(osm_id) else 'get'
    return Response(reply, mimetype='text/plain')

@app.route('/load/<int:osm_id>/postgis', methods=['POST'])
def load_postgis(osm_id):
    dbname = 'gis_{}'.format(osm_id)
    tables = create_database(dbname)

    expect = {'spatial_ref_sys', 'geography_columns', 'geometry_columns',
              'raster_overviews', 'planet_osm_roads', 'raster_columns',
              'planet_osm_line', 'planet_osm_point', 'planet_osm_polygon'}
    reply = 'need osm2pgsql' if tables != expect else 'skip osm2pgsql'
    return Response(reply, mimetype='text/plain')

@app.route('/load/<int:osm_id>/osm2pgsql', methods=['POST'])
def load_osm2pgsql(osm_id):
    error = load_into_pgsql(osm_id)
    return Response(error or 'done', mimetype='text/plain')

@app.route('/load/<int:osm_id>/match', methods=['POST'])
def load_match(osm_id):
    candidates = run_matcher(osm_id)

    item = load_from_cache('{}_nominatim.json'.format(osm_id))
    out = open(cache_filename('{}_summary.json'.format(osm_id)), 'w')
    item['candidate_count'] = len(candidates)
    json.dump(item, out, indent=2)
    out.close()
    return Response('done', mimetype='text/plain')

@app.route('/get_wikidata/<int:osm_id>')
def get_wikidata(osm_id):
    wikidata_item = load_from_cache('{}_nominatim.json'.format(osm_id))
    items = get_items_with_cats(osm_id)
    all_tags = find_tags(items)

    oql = generate_oql(osm_id, all_tags)

    return render_template('wikidata_items.html',
                           items=items,
                           hit=wikidata_item,
                           osm_id=osm_id,
                           overpass_done=overpass_done(osm_id),
                           oql=oql,
                           candidates=candidates,
                           all_tags=all_tags)

def get_existing():
    existing = [load_from_cache(f)
                for f in os.listdir(cache_dir())
                if f.endswith('_summary.json')]
    existing.sort(key=lambda i: i['candidate_count'], reverse=True)
    return existing

@app.route("/")
def index():
    q = request.args.get('q')
    if not q:
        existing = get_existing()
        return render_template('index.html', existing=existing)

    results = nominatim_lookup(q)
    return render_template('index.html', results=results, q=q)

@app.route("/documentation")
def documentation():
    return render_template('documentation.html')
