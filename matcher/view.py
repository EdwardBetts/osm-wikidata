#!/usr/bin/python3

from flask import Flask, render_template, request, Response, redirect, url_for
from .utils import cache_filename, load_from_cache, cache_dir
from lxml import etree
from .relation import Relation, nominatim_lookup
from . import db
from .matcher import filter_candidates_more
# from pprint import pformat

import requests
import os.path
import json

app = Flask(__name__)

@app.route("/overpass/<int:osm_id>", methods=["POST"])
def post_overpass(osm_id):
    relation = Relation(osm_id)
    relation.save_overpass(request.data)
    return Response('done', mimetype='text/plain')

@app.route('/export/wikidata_<int:osm_id>_<name>.osm')
def export_osm(osm_id, name):
    relation = Relation(osm_id)
    items = relation.get_candidates()

    items = filter_candidates_more(items)

    lookup = {(i['osm']['type'], i['osm']['id']): i for i in items}

    filename = cache_filename('{}_overpass_export.xml'.format(osm_id))
    if os.path.exists(filename):
        overpass_xml = open(filename, 'rb').read()
    else:
        seen = set()
        union = ''
        for item in items:
            osm = item['osm']
            assert (osm['id'], osm['type']) not in seen
            seen.add((osm['id'], osm['type']))
            assert 'wikidata' not in osm['tags']
            union += '{}({});\n'.format(osm['type'], osm['id'])

        oql = '({});(._;>);out meta;'.format(union)

        overpass_url = 'http://overpass-api.de/api/interpreter'
        r = requests.post(overpass_url, data=oql)
        overpass_xml = r.content
        with open(filename, 'wb') as f:
            f.write(overpass_xml)
    root = etree.fromstring(overpass_xml)

    for e in root:
        if e.tag not in {'way', 'node', 'relation'}:
            continue
        for f in 'uid', 'user', 'timestamp', 'changeset':
            del e.attrib[f]
        pair = (e.tag, int(e.attrib['id']))
        item = lookup.get(pair)
        if not item:
            continue
        e.attrib['version'] = str(int(e.attrib['version']) + 1)
        e.attrib['action'] = 'modify'
        tag = etree.Element('tag', k='wikidata', v=item['qid'])
        e.append(tag)

    xml = etree.tostring(root, pretty_print=True)
    return Response(xml, mimetype='text/xml')

@app.route('/candidates/<int:osm_id>')
def candidates(osm_id):
    relation = Relation(osm_id)
    wikidata_item = relation.item_detail()

    if relation.overpass_error:
        error = open(relation.overpass_filename).read()
        return render_template('candidates.html',
                               overpass_error=error,
                               hit=wikidata_item,
                               oql='',
                               candidates=[])

    for i in 'summary', 'candidates':
        if not os.path.exists(cache_filename('{}_{}.json'.format(osm_id, i))):
            return redirect(url_for('matcher_progress', osm_id=osm_id))

    relation.wbgetentities()
    tables = db.create_database(relation.dbname)

    expect = {'spatial_ref_sys', 'geography_columns', 'geometry_columns',
              'raster_overviews', 'planet_osm_roads', 'raster_columns',
              'planet_osm_line', 'planet_osm_point', 'planet_osm_polygon'}
    if tables != expect:
        error = relation.load_into_pgsql(osm_id)
        if error:
            return 'osm2pgsql error: ' + error

    candidate_list = relation.run_matcher()

    multiple_only = bool(request.args.get('multiple'))
    if multiple_only:
        candidate_list = [i for i in candidate_list
                          if len(i['candidates']) > 1]

    return render_template('candidates.html',
                           hit=wikidata_item,
                           relation=relation,
                           multiple_only=multiple_only,
                           candidates=candidate_list)

@app.route('/load/<int:osm_id>/wbgetentities', methods=['POST'])
def load_wikidata(osm_id):
    Relation(osm_id).wbgetentities()
    return 'done'

@app.route('/load/<int:osm_id>/checkover_pass', methods=['POST'])
def check_overpass(osm_id):
    relation = Relation(osm_id)
    reply = 'got' if relation.overpass_done else 'get'
    return Response(reply, mimetype='text/plain')

@app.route('/load/<int:osm_id>/postgis', methods=['POST'])
def load_postgis(osm_id):
    relation = Relation(osm_id)
    tables = db.create_database(relation.dbname)

    expect = {'spatial_ref_sys', 'geography_columns', 'geometry_columns',
              'raster_overviews', 'planet_osm_roads', 'raster_columns',
              'planet_osm_line', 'planet_osm_point', 'planet_osm_polygon'}
    reply = 'need osm2pgsql' if tables != expect else 'skip osm2pgsql'
    return Response(reply, mimetype='text/plain')

@app.route('/load/<int:osm_id>/osm2pgsql', methods=['POST'])
def load_osm2pgsql(osm_id):
    relation = Relation(osm_id)
    error = relation.load_into_pgsql()
    return Response(error or 'done', mimetype='text/plain')

@app.route('/load/<int:osm_id>/match', methods=['POST'])
def load_match(osm_id):
    relation = Relation(osm_id)
    candidates = relation.run_matcher()

    item = load_from_cache('{}_nominatim.json'.format(osm_id))
    out = open(cache_filename('{}_summary.json'.format(osm_id)), 'w')
    item['candidate_count'] = len(candidates)
    json.dump(item, out, indent=2)
    out.close()
    return Response('done', mimetype='text/plain')

@app.route('/matcher/<int:osm_id>')
def matcher_progress(osm_id):
    return render_template('wikidata_items.html',
                           relation=Relation(osm_id),
                           osm_id=osm_id)

def get_existing():
    sort = request.args.get('sort') or 'candidate_count'
    existing = [load_from_cache(f)
                for f in os.listdir(cache_dir())
                if f.endswith('_summary.json')]
    if sort == 'match_percent':
        def get_match_percent(i):
            if i.get('item_count') and i.get('candidate_count'):
                return i['candidate_count'] / i['item_count']
        existing.sort(key=lambda i: (get_match_percent(i) or 0, i['candidate_count']), reverse=True)
    else:
        existing.sort(key=lambda i: i[sort], reverse=True)
    return existing

@app.route("/")
def index():
    q = request.args.get('q')
    if not q:
        return render_template('index.html', existing=get_existing())
    else:
        return render_template('index.html', results=nominatim_lookup(q), q=q)

@app.route("/documentation")
def documentation():
    return render_template('documentation.html')
