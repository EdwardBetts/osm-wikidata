#!/usr/bin/python3

from flask import Flask, render_template, request, Response, redirect, url_for
from .utils import cache_filename, load_from_cache, cache_dir
from lxml import etree
from .relation import Relation
from . import db, database, nominatim, wikidata, matcher
from .model import Place, Item, PlaceItem, ItemCandidate
from .wikipedia import page_category_iter

import psycopg2.extras
import requests
import os.path

app = Flask(__name__)

@app.route("/overpass/<int:osm_id>", methods=["POST"])
def post_overpass(osm_id):
    place = Place.query.get(osm_id)
    place.save_overpass(request.data)
    place.state = 'postgis'
    database.session.commit()
    return Response('done', mimetype='text/plain')

@app.route('/export/wikidata_<int:osm_id>_<name>.osm')
def export_osm(osm_id, name):
    place = Place.query.get(osm_id)
    items = place.items_with_candidates()

    items = list(matcher.filter_candidates_more(items))

    lookup = {}
    for item in items:
        osm = item.candidates.one()
        lookup[(osm.osm_type, osm.osm_id)] = item

    filename = cache_filename('{}_overpass_export.xml'.format(osm_id))
    if os.path.exists(filename):
        overpass_xml = open(filename, 'rb').read()
    else:
        union = ''
        for item in items:
            osm = item.candidates.one()
            union += '{}({});\n'.format(osm.osm_type, osm.osm_id)

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
        tag = etree.Element('tag', k='wikidata', v=item.qid)
        e.append(tag)

    xml = etree.tostring(root, pretty_print=True)
    return Response(xml, mimetype='text/xml')

def redirect_to_matcher(osm_id):
    return redirect(url_for('matcher_progress', osm_id=osm_id))

@app.route('/candidates/<int:osm_id>')
def candidates(osm_id):
    place = Place.query.get(osm_id)
    multiple_only = bool(request.args.get('multiple'))

    if place.state != 'ready':
        return redirect_to_matcher(osm_id)

    if place.state == 'overpass_error':
        error = open(place.overpass_filename).read()
        return render_template('candidates.html',
                               overpass_error=error,
                               place=place)

    full_count = place.items_with_candidates().count()
    multiple_match_count = place.items_with_multiple_candidates().count()

    if multiple_only:
        item_ids = [i[0] for i in place.items_with_multiple_candidates()]
        items = Item.query.filter(Item.item_id.in_(item_ids))
    else:
        items = place.items_with_candidates()

    items_without_matches = place.items_without_candidates()

    return render_template('candidates.html',
                           place=place,
                           osm_id=osm_id,
                           items_without_matches=items_without_matches,
                           multiple_only=multiple_only,
                           full_count=full_count,
                           multiple_match_count=multiple_match_count,
                           candidates=items)

def wbgetentities(p):
    q = p.items.filter(Item.tags != '{}')
    items = {i.qid: i for i in q}

    for qid, entity in wikidata.entity_iter(items.keys()):
        item = items[qid]
        item.entity = entity
        database.session.add(item)
    database.session.commit()

@app.route('/load/<int:osm_id>/wbgetentities', methods=['POST'])
def load_wikidata(osm_id):
    place = Place.query.get(osm_id)
    if place.state != 'tags':
        return 'done'
    wbgetentities(place)
    place.state = 'wbgetentities'
    database.session.commit()
    return 'done'

@app.route('/load/<int:osm_id>/checkover_pass', methods=['POST'])
def check_overpass(osm_id):
    place = Place.query.get(osm_id)
    reply = 'got' if place.overpass_done else 'get'
    return Response(reply, mimetype='text/plain')

@app.route('/load/<int:osm_id>/postgis', methods=['POST'])
def load_postgis(osm_id):
    place = Place.query.get(osm_id)
    tables = db.create_database(place.dbname)

    expect = {'spatial_ref_sys', 'geography_columns', 'geometry_columns',
              'raster_overviews', 'planet_osm_roads', 'raster_columns',
              'planet_osm_line', 'planet_osm_point', 'planet_osm_polygon'}
    if tables == expect:
        place.state = 'osm2pgsql'
        reply = 'skip osm2pgsql'
    else:
        place.state = 'postgis'
        reply = 'need osm2pgsql'
    database.session.commit()

    return Response(reply, mimetype='text/plain')

@app.route('/load/<int:osm_id>/osm2pgsql', methods=['POST'])
def load_osm2pgsql(osm_id):
    place = Place.query.get(osm_id)
    error = place.load_into_pgsql()
    if not error:
        place.state = 'osm2pgsql'
        database.session.commit()
    return Response(error or 'done', mimetype='text/plain')


@app.route('/load/<int:osm_id>/match', methods=['POST'])
def load_match(osm_id):
    place = Place.query.get(osm_id)

    conn = db.db_connect(place.dbname)
    psycopg2.extras.register_hstore(conn)
    cur = conn.cursor()

    q = place.items.filter(Item.entity.isnot(None)).order_by(Item.item_id)
    for item in q:
        candidates = matcher.find_item_matches(cur, item)
        for i in (candidates or []):
            c = ItemCandidate.query.get((item.item_id, i['osm_id'], i['osm_type']))
            if not c:
                c = ItemCandidate(**i, item=item)
                database.session.add(c)
    place.state = 'ready'
    database.session.commit()

    conn.close()
    return Response('done', mimetype='text/plain')

@app.route('/matcher/<int:osm_id>')
def matcher_progress(osm_id):
    place = Place.query.get(osm_id)

    if not place.state:
        items = {i['enwiki']: i for i in place.items_from_wikidata()}

        for title, cats in page_category_iter(items.keys()):
            print(title, cats)
            items[title]['categories'] = cats

        for enwiki, i in items.items():
            item = Item.query.get(i['id'])
            if not item:
                item = Item(item_id=i['id'],
                            enwiki=enwiki,
                            location=i['location'],
                            categories=i.get('categories'))
                database.session.add(item)
            place_item = PlaceItem.query.get((item.item_id, place.osm_id))
            if not place_item:
                database.session.add(PlaceItem(item=item, place=place))
        place.state = 'wikipedia'
        database.session.commit()
    if place.state == 'wikipedia':
        place.add_tags_to_items()

    return render_template('wikidata_items.html', place=place)

def get_existing():
    return Place.query.filter(Place.state.isnot(None))

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

    results = nominatim.lookup(q)
    for hit in results:
        p = Place.from_nominatim(hit)
        if p:
            database.session.merge(p)
    database.session.commit()

    for hit in results:
        if hit['osm_type'] == 'relation':
            hit['place'] = Place.query.get(hit['osm_id'])

    return render_template('index.html', results=results, q=q)

@app.route("/documentation")
def documentation():
    return render_template('documentation.html')
