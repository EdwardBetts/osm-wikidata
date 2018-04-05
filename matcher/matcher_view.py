from flask import Blueprint, abort, redirect, render_template, g, Response, jsonify, request, flash
from . import database, matcher, mail, utils
from .model import Item
from .place import Place, PlaceMatcher
import requests
import re

re_point = re.compile('^Point\((-?[0-9.]+) (-?[0-9.]+)\)$')

matcher_blueprint = Blueprint('matcher', __name__)

def announce_matcher_progress(place):
    ''' Send mail to announce when somebody runs the matcher. '''
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

    if place.too_big:
        return render_template('too_big.html', place=place)

    is_refresh = place.state == 'refresh'

    announce_matcher_progress(place)
    replay_log = place.state == 'ready' and bool(utils.find_log_file(place))

    start = None
    if not replay_log:
        user_agent = request.headers.get('User-Agent')
        user = g.user if g.user.is_authenticated else None
        run = PlaceMatcher(place=place,
                           user=user,
                           remote_addr=request.remote_addr,
                           user_agent=user_agent,
                           is_refresh=is_refresh)
        database.session.add(run)
        database.session.commit()
        start = run.start

    url_scheme = request.environ.get('wsgi.url_scheme')
    ws_scheme = 'wss' if url_scheme == 'https' else 'ws'

    return render_template('matcher.html',
                           place=place,
                           is_refresh=is_refresh,
                           ws_scheme=ws_scheme,
                           replay_log=replay_log,
                           start=start)

@matcher_blueprint.route('/matcher/<osm_type>/<int:osm_id>/done')
def matcher_done(osm_type, osm_id):
    refresh = request.args.get('refresh') == '1'
    start = request.args.get('start')
    place = Place.get_or_abort(osm_type, osm_id)
    if place.state != 'ready':
        if place.too_big:
            return render_template('too_big.html', place=place)

        # place isn't ready so redirect to matcher progress
        return redirect(place.matcher_progress_url())

    if start:
        q = place.matcher_runs.filter(PlaceMatcher.start == start)
        run = q.one_or_none()
        if run:
            run.complete()

    if refresh:
        msg = 'Matcher refresh complete.'
    else:
        msg = 'The matcher has finished.'
    flash(msg)
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
