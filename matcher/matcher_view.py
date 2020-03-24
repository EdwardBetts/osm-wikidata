from flask import Blueprint, redirect, render_template, g, request, flash, current_app, session, url_for
from . import database, mail, utils
from .place import Place
import json
import re

re_point = re.compile(r'^Point\((-?[0-9.]+) (-?[0-9.]+)\)$')

matcher_blueprint = Blueprint('matcher', __name__)

def announce_matcher_progress(place):
    ''' Send mail to announce when somebody runs the matcher. '''
    if current_app.env == 'development':
        return
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

def confirm_matcher(place):

    wikidata_chunk_size = 22
    # size = place.wikidata_chunk_size(wikidata_chunk_size)
    wikidata_chunks = list(place.polygon_chunk(size=wikidata_chunk_size))
    wikidata_counk_count = len(wikidata_chunks)

    # recent_search = session.get('recent_search')
    # FIXME: if the user comes from the browse page then cancel should return
    # to the browse page
    cancel_url = session.get('cancel_match') or url_for('index')

    overpass_chunk_size = 22
    overpass_chunks = place.get_chunks(chunk_size=overpass_chunk_size)

    return render_template('confirm_matcher.html',
                           place=place,
                           cancel_url=cancel_url,
                           wikidata_chunk_size=wikidata_chunk_size,
                           wikidata_chunk_count=wikidata_counk_count,
                           overpass_chunk_size=overpass_chunk_size,
                           overpass_chunk_count=len(overpass_chunks))

@matcher_blueprint.route('/chunk/<osm_type>/<int:osm_id>.json')
def chunk_size_json(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)

    reply = {}

    if 'wikidata_chunk_size' in request.args:
        wikidata_chunk_size = int(request.args['wikidata_chunk_size'])
        # size = place.wikidata_chunk_size(wikidata_chunk_size)
        wikidata_chunks = list(place.polygon_chunk(size=wikidata_chunk_size))
        reply['wikidata_chunk_size'] = wikidata_chunk_size
        reply['wikidata_chunk_count'] = len(wikidata_chunks)

    if 'overpass_chunk_size' in request.args:
        overpass_chunk_size = int(request.args['overpass_chunk_size'])
        overpass_chunks = place.get_chunks(chunk_size=overpass_chunk_size)
        reply['overpass_chunk_size'] = overpass_chunk_size
        reply['overpass_chunk_count'] = len(overpass_chunks)
        if False:
            reply['chunk_geojson'] = [json.loads(chunk) for chunk
                                      in place.geojson_chunks(overpass_chunk_size)]

    return jsonify(reply)

@matcher_blueprint.route('/matcher/<osm_type>/<int:osm_id>', methods=['GET', 'POST'])
def matcher_progress(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)
    confirmed_key = f'confirmed/{osm_type}/{osm_id}'
    session_key = f'match_params/{osm_type}/{osm_id}'

    if place.state == 'ready':
        return redirect(place.candidates_url())

    if request.method == 'POST':
        keys = 'wikidata_chunk_size', 'overpass_chunk_size'
        session[session_key] = {key: int(request.form[key]) for key in keys}
        form_want_isa = request.form['want_isa']
        want_isa = form_want_isa.split(',') if form_want_isa else []
        session[session_key]['want_isa'] = want_isa
        session[confirmed_key] = 'yes'

        return redirect(request.url)

    if place.too_big or place.too_complex:
        return render_template('too_big.html', place=place)

    confirmed = session.get(confirmed_key) == 'yes'
    match_params = session.get(session_key)

    if not confirmed:
        return confirm_matcher(place)

    del session[confirmed_key]
    is_refresh = place.state == 'refresh'

    announce_matcher_progress(place)
    replay_log = place.state == 'ready' and bool(utils.find_log_file(place))

    url_scheme = request.environ.get('wsgi.url_scheme')
    ws_scheme = 'wss' if url_scheme == 'https' else 'ws'

    return render_template('matcher.html',
                           place=place,
                           is_refresh=is_refresh,
                           ws_scheme=ws_scheme,
                           replay_log=replay_log,
                           match_params=match_params)

@matcher_blueprint.route('/matcher/<osm_type>/<int:osm_id>/done')
def matcher_done(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)
    if place.too_big:
        return render_template('too_big.html', place=place)

    if place.state != 'ready':
        place.state = 'ready'
        database.session.commit()

    flash('The matcher has finished.')
    return redirect(place.candidates_url())

@matcher_blueprint.route('/replay/<osm_type>/<int:osm_id>')
def replay(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)

    replay_log = True
    url_scheme = request.environ.get('wsgi.url_scheme')
    ws_scheme = 'wss' if url_scheme == 'https' else 'ws'

    return render_template('matcher.html',
                           place=place,
                           ws_scheme=ws_scheme,
                           replay_log=replay_log)
