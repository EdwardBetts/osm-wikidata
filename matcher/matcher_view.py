from flask import Blueprint, abort, redirect, render_template, g, Response, jsonify, request
from . import database, wikidata, matcher, mail
from .model import Item, ItemCandidate
from .place import Place

import requests

matcher_blueprint = Blueprint('matcher', __name__)

@matcher_blueprint.route('/matcher/<osm_type>/<int:osm_id>')
def matcher_progress(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)
    if place.state == 'ready':
        return redirect(place.candidates_url())

    if osm_type != 'node' and place.area and place.area_in_sq_km > 90000:
        message = '{}: area is too large for matcher'.format(place.name)
        return render_template('error_page.html', message=message)

    if not place.state or place.state == 'refresh':
        try:
            place.load_items()
        except wikidata.QueryError as e:
            return render_template('wikidata_query_error.html',
                                   query=e.query,
                                   place=place,
                                   reply=e.r.text)

        place.state = 'wikipedia'

    if place.state == 'wikipedia':
        place.add_tags_to_items()
        place.state = 'tags'
        database.session.commit()

    if g.user.is_authenticated:
        user = g.user.username
        subject = 'matcher: {} (user: {})'.format(place.name, user)
    else:
        user = 'not authenticated'
        subject = 'matcher: {} (no auth)'.format(place.name)

    template = '''
user: {}
name: {}
page: {}
area: {}
'''

    body = template.format(user,
                           place.display_name,
                           place.candidates_url(_external=True),
                           mail.get_area(place))
    mail.send_mail(subject, body)

    return render_template('wikidata_items.html', place=place)

@matcher_blueprint.route('/load/<int:place_id>/wbgetentities', methods=['POST'])
def load_wikidata(place_id):
    place = Place.query.get(place_id)
    if place.state != 'tags':
        oql = place.get_oql()
        return jsonify(success=True, item_list=place.item_list(), oql=oql)
    try:
        place.wbgetentities()
    except requests.exceptions.HTTPError as e:
        error = e.response.text
        mail.place_error(place, 'wikidata', error)
        lc_error = error.lower()
        if 'timeout' in lc_error or 'time out' in lc_error:
            error = 'wikidata query timeout'
        return jsonify(success=False, error=e.r.text)

    place.load_extracts()
    place.state = 'wbgetentities'
    database.session.commit()
    oql = place.get_oql()
    return jsonify(success=True, item_list=place.item_list(), oql=oql)

@matcher_blueprint.route('/load/<int:place_id>/check_overpass', methods=['POST'])
def check_overpass(place_id):
    place = Place.query.get(place_id)
    reply = 'got' if place.overpass_done else 'get'
    return Response(reply, mimetype='text/plain')

@matcher_blueprint.route('/load/<int:place_id>/overpass_error', methods=['POST'])
def overpass_error(place_id):
    place = Place.query.get(place_id)
    if not place:
        abort(404)
    place.state = 'overpass_error'
    database.session.commit()

    error = request.form['error']
    mail.place_error(place, 'overpass', error)

    return Response('noted', mimetype='text/plain')

@matcher_blueprint.route('/load/<int:place_id>/overpass_timeout', methods=['POST'])
def overpass_timeout(place_id):
    place = Place.query.get(place_id)
    place.state = 'overpass_timeout'
    database.session.commit()

    mail.place_error(place, 'overpass', 'timeout')

    return Response('timeout noted', mimetype='text/plain')

@matcher_blueprint.route('/load/<int:place_id>/osm2pgsql', methods=['POST', 'GET'])
def load_osm2pgsql(place_id):
    place = Place.query.get(place_id)
    if not place:
        abort(404)
    expect = [place.prefix + '_' + t for t in ('line', 'point', 'polygon')]
    tables = database.get_tables()
    if not all(t in tables for t in expect):
        error = place.load_into_pgsql()
        if error:
            mail.place_error(place, 'osm2pgl', error)
            return Response(error, mimetype='text/plain')
    place.state = 'osm2pgsql'
    database.session.commit()
    return Response('done', mimetype='text/plain')

@matcher_blueprint.route('/load/<int:place_id>/match/Q<int:item_id>', methods=['POST', 'GET'])
def load_individual_match(place_id, item_id):
    global cat_to_ending

    place = Place.query.get(place_id)
    if not place:
        abort(404)

    conn = database.session.bind.raw_connection()
    cur = conn.cursor()

    item = Item.query.get(item_id)
    candidates = matcher.find_item_matches(cur, item, place.prefix, debug=False)
    confirmed = set()
    for i in (candidates or []):
        c = ItemCandidate.query.get((item.item_id, i['osm_id'], i['osm_type']))
        if not c:
            c = ItemCandidate(**i, item=item)
            database.session.add(c)
        confirmed.add((c.item_id, c.osm_id, c.osm_type))
    for c in item.candidates:
        if ((c.item_id, c.osm_id, c.osm_type) not in confirmed and
                not c.bad_matches.count()):
            database.session.delete(c)
    database.session.commit()

    conn.close()
    return Response('done', mimetype='text/plain')

@matcher_blueprint.route('/load/<int:place_id>/ready', methods=['POST', 'GET'])
def load_ready(place_id):
    place = Place.query.get(place_id)
    if not place:
        abort(404)

    place.state = 'ready'
    place.item_count = place.items.count()
    place.candidate_count = place.items_with_candidates_count()
    database.session.commit()
    return Response('done', mimetype='text/plain')

@matcher_blueprint.route('/overpass/<int:place_id>', methods=['POST'])
def post_overpass(place_id):
    place = Place.query.get(place_id)
    place.save_overpass(request.data)
    place.state = 'overpass'
    database.session.commit()
    return Response('done', mimetype='text/plain')
