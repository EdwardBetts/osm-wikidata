from . import database, nominatim
from .place import Place
from flask import session, url_for
import re

re_place_identifier = re.compile(r'^(node|way|relation)/(\d+)$')
re_qid = re.compile(r'^(Q\d+)$')

def update_search_results(results):
    need_commit = False
    for hit in results:
        if not ('osm_type' in hit and 'osm_id' in hit and 'geotext' in hit):
            continue

        p = Place.query.get(hit['place_id'])
        if p and (p.osm_type != hit['osm_type'] or p.osm_id != hit['osm_id']):
            need_commit = True
            db_place_hit = nominatim.reverse(p.osm_type, p.osm_id)
            if 'error' in db_place_hit or 'place_id' not in db_place_hit:
                # place deleted from OSM
                if p.osm_type == 'node':
                    database.session.delete(p)
                # FIXME: mail admin if place isn't a node on OSM
            else:
                p.place_id = db_place_hit['place_id']

        p = Place.query.filter_by(osm_type=hit['osm_type'],
                                  osm_id=hit['osm_id']).one_or_none()
        if p and p.place_id != hit['place_id']:
            p.update_from_nominatim(hit)
            need_commit = True
        elif not p:
            p = Place.query.get(hit['place_id'])
            if p:
                p.update_from_nominatim(hit)
            else:
                p = Place.from_nominatim(hit)
                database.session.add(p)
            need_commit = True
    if need_commit:
        database.session.commit()

def check_for_place_identifier(q):
    q = q.strip()
    m = re_place_identifier.match(q)
    if not m:
        return
    osm_type, osm_id = m.groups()
    p = Place.from_osm(osm_type, int(osm_id))
    if not p:
        return

    return p.candidates_url() if p.state == 'ready' else p.matcher_progress_url()

def check_for_search_identifier(q):
    q = q.strip()
    # if searching for a Wikidata QID then redirect to the item page for that QID
    m = re_qid.match(q)
    if m:
        return url_for('item_page', wikidata_id=m.group(1)[1:])

    return check_for_place_identifier(q)

def handle_redirect_on_single(results):
    if not session.get('redirect_on_single', False):
        return

    session['redirect_on_single'] = False
    hits = [hit for hit in results if hit['osm_type'] != 'node']
    if len(hits) != 1:
        return

    hit = hits[0]
    place = Place.get_or_abort(hit['osm_type'], hit['osm_id'])
    if place:
        return place.redirect_to_matcher()

def check_for_city_node_in_results(q, results):
    for hit_num, hit in enumerate(results):
        if hit['osm_type'] != 'node':
            continue
        name_parts = hit['display_name'].split(', ')
        node, area = name_parts[:2]
        if area not in (f'{node} City', f'City of {node}'):
            continue
        city_q = ', '.join(name_parts[1:])
        city_results = nominatim.lookup(city_q)
        if len(city_results) == 1:
            results[hit_num] = city_results[0]


