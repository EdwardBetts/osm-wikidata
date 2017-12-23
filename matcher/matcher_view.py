from flask import Blueprint, abort, redirect, render_template, g, Response, jsonify, request
from . import database, matcher, mail, utils
from .model import Item
from .place import Place
import requests
import re

re_point = re.compile('^Point\((-?[0-9.]+) (-?[0-9.]+)\)$')

matcher_blueprint = Blueprint('matcher', __name__)

def announce_matcher_progress(place):
    ''' Send mail to announce somebody is trying the matcher. '''
    if g.user.is_authenticated:
        user = g.user.username
        subject = 'matcher: {} (user: {})'.format(place.name, user)
    elif utils.is_bot():
        return  # don't announce bots
    else:
        user = 'not authenticated'
        subject = 'matcher: {} (no auth)'.format(place.name)

    user_agent = request.headers.get('User-Agent', '[header missing]')
    template = '''
user: {}
IP: {}
agent: {}
name: {}
page: {}
area: {}
'''

    body = template.format(user,
                           request.remote_addr,
                           user_agent,
                           place.display_name,
                           place.candidates_url(_external=True),
                           mail.get_area(place))
    mail.send_mail(subject, body)

@matcher_blueprint.route('/matcher/<osm_type>/<int:osm_id>')
def matcher_progress(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)
    if place.state == 'ready':
        return redirect(place.candidates_url())

    announce_matcher_progress(place)
    replay_log = bool(utils.find_log_file(place))

    return render_template('matcher.html',
                           place=place,
                           replay_log=replay_log)

@matcher_blueprint.route('/matcher/<osm_type>/<int:osm_id>/query_wikidata')
def query_wikidata(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)

    wikidata_items = place.items_from_wikidata(place.bbox)
    items = []
    for qid, v in wikidata_items.items():
        v['qid'] = qid
        label = v.pop('query_label')
        enwiki = v.get('enwiki')
        if enwiki:
            del v['enwiki']
            if not enwiki.startswith(label + ','):
                label = enwiki
        v['label'] = label
        location = v.pop('location')
        lon, lat = re_point.match(location).groups()
        v['lat'] = lat
        v['lon'] = lon
        if 'tags' in v:
            v['tags'] = list(v['tags'])
        items.append(v)
    return jsonify(items=items)

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

    item = Item.query.get(item_id)
    candidates = matcher.run_individual_match(place, item)
    matcher.save_individual_matches(place, item, candidates)
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
