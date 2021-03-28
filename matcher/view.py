from . import (database, wikidata, wikidata_api, matcher, match, commons,
               export, user_agent_headers, overpass, mail, browse, edit, utils,
               osm_oauth, search)
from .utils import get_int_arg
from .model import (Item, ItemCandidate, User, Category, Changeset, ItemTag, BadMatch,
                    Timing, get_bad, Language, EditMatchReject, InProgress, IsA,
                    ItemIsA, SiteBanner)
from .place import Place
from .taginfo import get_taginfo
from .pager import Pagination, init_pager
from .forms import AccountSettingsForm
from .isa_facets import get_isa_facets, get_isa_facets2
from collections import Counter

from flask import (Flask, render_template, request, Response, redirect, url_for, g,
                   jsonify, flash, abort, make_response, session)
from lxml import etree
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy import func, distinct
from werkzeug.exceptions import InternalServerError
from jinja2 import evalcontextfilter, Markup, escape
from time import time, sleep
from werkzeug.debug.tbtools import get_current_traceback
from requests_oauthlib import OAuth1Session

from .matcher_view import matcher_blueprint
from .admin_view import admin_blueprint
from .api_view import api_blueprint
from .websocket import ws

from flask_sockets import Sockets

import flask_login
import json
import inspect
import requests
import re
import random

_paragraph_re = re.compile(r'(?:\r\n|\r|\n){2,}')

app = Flask(__name__)
app.register_blueprint(matcher_blueprint)
app.register_blueprint(admin_blueprint)
app.register_blueprint(api_blueprint)
sockets = Sockets(app)
sockets.register_blueprint(ws)
init_pager(app)
login_manager = flask_login.LoginManager(app)
login_manager.login_view = 'login_route'

cat_to_ending = None
osm_api_base = 'https://api.openstreetmap.org/api/0.6'
really_save = True

hide_trams = {
    ('relation', 186579),  # Portland, OR
    ('relation', 111968),  # San Francisco, CA
}  # matcher doesn't handle tram stops properly, so hide them for these places

navbar_pages = {
    'criteria_page': 'Criteria',
    'tag_list': 'Search tags',
    'documentation': 'Documentation',
    'changesets': 'Recent changes',
    'random_city': 'Random',
}

tab_pages = [
    {'route': 'candidates', 'label': 'Match candidates'},
    {'route': 'already_tagged', 'label': 'Already tagged'},
    {'route': 'no_match', 'label': 'No match'},
]

disabled_tab_pages = [
    {'route': 'overpass_query', 'label': 'Overpass query'}
]

@app.template_global()
def set_url_args(**new_args):
    args = request.view_args.copy()
    args.update(request.args)
    args.update(new_args)
    args = {k: v for k, v in args.items() if v is not None}
    return url_for(request.endpoint, **args)

@app.template_filter()
@evalcontextfilter
def newline_br(eval_ctx, value):
    result = u'\n\n'.join(u'<p>%s</p>' % p.replace('\n', '<br>\n') \
        for p in _paragraph_re.split(escape(value)))
    if eval_ctx.autoescape:
        result = Markup(result)
    return result

def demo_mode():
    return session.get('demo_mode', False) or request.args.get('demo')

@app.before_request
def global_user():
    if demo_mode():
        g.user = User.query.get(1)
    else:
        g.user = flask_login.current_user._get_current_object()

@app.before_request
def site_banner():
    g.banner = SiteBanner.query.filter(SiteBanner.end.is_(None)).one_or_none()

@app.before_request
def slow_crawl():
    if utils.is_bot():
        sleep(5)

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(user_id)

@app.context_processor
def navbar():
    try:
        return dict(navbar_pages=navbar_pages,
                    active=request.endpoint)
    except RuntimeError:
        return {}  # maybe we don't care

@app.route('/login')
def login_route():
    if app.env == 'development':
        users = User.query
        return render_template('dev/login.html',
                               users=users,
                               next=request.args.get('next'))

    return redirect(url_for('start_oauth',
                            next=request.args.get('next')))

@app.route('/dev/login', methods=['POST'])
def dev_login():
    if app.env != 'development':
        abort(403)
    dest = request.form.get('next') or url_for('index')
    user_id = request.form['user_id']
    user = User.query.filter_by(id=user_id).one_or_none()

    flask_login.login_user(user)

    return redirect(dest)

@app.route('/login/openstreetmap/')
def login_openstreetmap():
    return redirect(url_for('start_oauth',
                            next=request.args.get('next')))

@app.route('/logout')
def logout():
    next_url = request.args.get('next') or url_for('index')
    flask_login.logout_user()
    flash('you are logged out')
    return redirect(next_url)

@app.route('/done/')
def done():
    flash('login successful')
    return redirect(url_for('index'))

@app.route('/oauth/start')
def start_oauth():
    next_page = request.args.get('next')
    if next_page:
        session['next'] = next_page

    client_key = app.config['CLIENT_KEY']
    client_secret = app.config['CLIENT_SECRET']

    request_token_url = 'https://www.openstreetmap.org/oauth/request_token'

    callback = url_for('oauth_callback', _external=True)

    oauth = OAuth1Session(client_key,
                          client_secret=client_secret,
                          callback_uri=callback)
    fetch_response = oauth.fetch_request_token(request_token_url)

    session['owner_key'] = fetch_response.get('oauth_token')
    session['owner_secret'] = fetch_response.get('oauth_token_secret')

    base_authorization_url = 'https://www.openstreetmap.org/oauth/authorize'
    authorization_url = oauth.authorization_url(base_authorization_url,
                                                oauth_consumer_key=client_key)
    return redirect(authorization_url)

@app.route("/oauth/callback", methods=["GET"])
def oauth_callback():
    client_key = app.config['CLIENT_KEY']
    client_secret = app.config['CLIENT_SECRET']

    oauth = OAuth1Session(client_key,
                          client_secret=client_secret,
                          resource_owner_key=session['owner_key'],
                          resource_owner_secret=session['owner_secret'])

    oauth_response = oauth.parse_authorization_response(request.url)
    verifier = oauth_response.get('oauth_verifier')
    access_token_url = 'https://www.openstreetmap.org/oauth/access_token'
    oauth = OAuth1Session(client_key,
                          client_secret=client_secret,
                          resource_owner_key=session['owner_key'],
                          resource_owner_secret=session['owner_secret'],
                          verifier=verifier)

    oauth_tokens = oauth.fetch_access_token(access_token_url)
    session['owner_key'] = oauth_tokens.get('oauth_token')
    session['owner_secret'] = oauth_tokens.get('oauth_token_secret')

    r = oauth.get(osm_api_base + '/user/details')
    info = osm_oauth.parse_userinfo_call(r.content)

    user = User.query.filter_by(osm_id=info['id']).one_or_none()

    if user:
        user.osm_oauth_token = oauth_tokens.get('oauth_token')
        user.osm_oauth_token_secret = oauth_tokens.get('oauth_token_secret')
    else:
        user = User(
            username=info['username'],
            description=info['description'],
            img=info['img'],
            osm_id=info['id'],
            osm_account_created=info['account_created'],
        )
        database.session.add(user)
    database.session.commit()
    flask_login.login_user(user)

    next_page = session.get('next') or url_for('index')
    return redirect(next_page)

def reraise(tp, value, tb=None):
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value

@app.errorhandler(requests.exceptions.HTTPError)
def requests_exception(e):
    if request.endpoint != 'social.auth':
        raise e
    return 'OSM token request failed.'

@app.errorhandler(wikidata_api.QueryError)
def query_error(e):
    tb = get_current_traceback()
    return render_template('show_query_error.html', e=e, tb=tb), 500

@app.errorhandler(InternalServerError)
def exception_handler(e):
    tb = get_current_traceback()
    last_frame = tb.frames[-1]
    last_frame_args = inspect.getargs(last_frame.code)
    return render_template('show_error.html',
                           tb=tb,
                           last_frame=last_frame,
                           last_frame_args=last_frame_args), 500

def get_osm_object(osm_type, osm_id, attempts=5):
    url = '{}/{}/{}'.format(osm_api_base, osm_type, osm_id)
    for attempt in range(attempts):
        try:
            r = requests.get(url, headers=user_agent_headers())
            return etree.fromstring(r.content)
        except etree.XMLSyntaxError:
            if attempt == attempts - 1:
                mail.error_mail('error requesting element', url, r)
                raise
            sleep(1)

@app.route('/add_wikidata_tag', methods=['POST'])
def add_wikidata_tag():
    '''Add wikidata tags for a single item'''
    wikidata_id = request.form['wikidata']
    osm = request.form.get('osm')
    if not osm:
        flash('no candidate selected')
        return redirect(url_for('item_page', wikidata_id=wikidata_id[1:]))

    osm_type, _, osm_id = osm.partition('/')

    user = g.user
    assert user.is_authenticated

    root = get_osm_object(osm_type, osm_id)

    if root.find('.//tag[@k="wikidata"]') is not None:
        flash('no edit needed: OSM element already had wikidata tag')
        return redirect(url_for('item_page', wikidata_id=wikidata_id[1:]))

    comment = request.form.get('comment', 'add wikidata tag')
    changeset = edit.new_changeset(comment)
    r = edit.create_changeset(changeset)
    changeset_id = r.text.strip()

    tag = etree.Element('tag', k='wikidata', v=wikidata_id)
    root[0].set('changeset', changeset_id)
    root[0].append(tag)

    element_data = etree.tostring(root)

    try:
        edit.save_element(osm_type, osm_id, element_data)
    except requests.exceptions.HTTPError as e:
        r = e.response
        mail.error_mail('error saving element', element_data, r)

        return render_template('error_page.html',
                message="The OSM API returned an error when saving your edit: {}: " + r.text)

    for c in ItemCandidate.query.filter_by(osm_id=osm_id, osm_type=osm_type):
        c.tags['wikidata'] = wikidata_id
        flag_modified(c, 'tags')

    edit.record_changeset(id=changeset_id,
                          comment=comment,
                          item_id=wikidata_id[1:],
                          update_count=1)

    edit.close_changeset(changeset_id)
    flash('wikidata tag saved in OpenStreetMap')

    return redirect(url_for('item_page', wikidata_id=wikidata_id[1:]))

@app.route('/wikidata/<osm_type>/<int:osm_id>')
def wikidata_page(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)

    full_count = place.items_with_candidates_count()

    return render_template('wikidata_query.html',
                           place=place,
                           tab_pages=tab_pages,
                           osm_id=osm_id,
                           full_count=full_count)

@app.route('/overpass/<osm_type>/<int:osm_id>')
def overpass_query(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)

    full_count = place.items_with_candidates_count()

    return render_template('overpass.html',
                           place=place,
                           tab_pages=tab_pages,
                           osm_id=osm_id,
                           full_count=full_count)

def save_timing(name, t0):
    timing = Timing(start=t0,
                    path=request.full_path,
                    name=name,
                    seconds=time() - t0)
    database.session.add(timing)

@app.route('/update_tags/<osm_type>/<int:osm_id>', methods=['POST'])
def update_tags(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)

    candidates = []
    for item in place.items_with_candidates():
        candidates += item.candidates.all()

    elements = overpass.get_tags(candidates)

    for e in elements:
        for c in ItemCandidate.query.filter_by(osm_id=e['id'],
                                               osm_type=e['type']):
            if 'tags' in e:  # FIXME do something clever like delete the OSM candidate
                c.tags = e['tags']
    database.session.commit()

    flash('tags updated')

    return redirect(place.candidates_url())

def add_tags_post(osm_type, osm_id):
    include = request.form.getlist('include')
    items = Item.query.filter(Item.item_id.in_([i[1:] for i in include])).all()

    hits = matcher.filter_candidates_more(items,
                                          bad=get_bad(items),
                                          ignore_existing=demo_mode())
    table = [(item, match['candidate'])
             for item, match in hits if 'candidate' in match]

    candidates = {
        'isa_filter': request.form.getlist('isa') or [],
        'list': [{'qid': i.qid, 'osm_type': c.osm_type, 'osm_id': c.osm_id}
                 for i, c in table]
    }

    existing = InProgress.query.get((g.user.id, osm_type, osm_id))
    if existing:
        existing.candidates = candidates

    else:
        in_progress = InProgress(user_id=g.user.id,
                                 osm_type=osm_type,
                                 osm_id=osm_id,
                                 candidates=candidates)

        database.session.add(in_progress)
    database.session.commit()

    # switch method from POST to GET
    return redirect(url_for(request.endpoint, **request.view_args))

@app.route('/add_tags/<osm_type>/<int:osm_id>', methods=['GET', 'POST'])
def add_tags(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)

    if request.method == 'POST':
        return add_tags_post(osm_type, osm_id)

    check_still_auth()
    if not g.user.is_authenticated:
        return redirect(url_for('login_route', next=request.url))

    in_progress = InProgress.query.get((g.user.id, osm_type, osm_id))
    if in_progress is None:
        # FIXME: tell the user than something went wrong with a link back to
        # the candidates page
        return redirect(place.candidates_url())
    table = []
    for c in in_progress.candidates['list']:
        item_id = int(c['qid'][1:])
        candidate = ItemCandidate.query.get((item_id, c['osm_id'], c['osm_type']))
        table.append((candidate.item, candidate))

    g.country_code = place.country_code
    languages_with_counts = get_place_language_with_counts(place)
    languages = [l['lang'] for l in languages_with_counts if l['lang']]

    items = []
    add_wikipedia_tags = getattr(g.user, 'wikipedia_tag', False)
    for i, c in table:
        description = '{} {}: adding wikidata={}'.format(c.osm_type, c.osm_id, i.qid)
        item = {
            'qid': i.qid,
            'osm_type': c.osm_type,
            'osm_id': c.osm_id,
            'description': description,
        }
        if add_wikipedia_tags:
            wiki_lang, wiki_title = c.new_wikipedia_tag(languages)
            if wiki_lang:
                item['wiki_lang'] = wiki_lang
                item['wiki_title'] = wiki_title

        items.append(item)

    url_scheme = request.environ.get('wsgi.url_scheme')
    ws_scheme = 'wss' if url_scheme == 'https' else 'ws'

    isa_filter = in_progress.candidates.get('isa_filter') or []
    isa_labels = [IsA.query.get(isa[1:]).label_best_language(languages, plural=True)
                  for isa in isa_filter]

    return render_template('add_tags.html',
                           place=place,
                           osm_id=osm_id,
                           ws_scheme=ws_scheme,
                           isa_labels=isa_labels,
                           items=items,
                           table=table,
                           languages=languages,
                           add_wikipedia_tags=add_wikipedia_tags)

@app.route('/places/<name>')
def place_redirect(name):
    place = Place.query.filter(Place.state.in_('ready', 'complete'),
                               Place.display_name.ilike(name + '%')).first()
    if not place:
        abort(404)
    return redirect(place.candidates_url())

def get_bad_matches(place):
    q = (database.session
                 .query(ItemCandidate.item_id,
                        ItemCandidate.osm_type,
                        ItemCandidate.osm_id)
                 .join(BadMatch).distinct())

    return set(tuple(row) for row in q)

def get_wikidata_language(code):
    return (Language.query.filter_by(wikimedia_language_code=code)
                          .one_or_none())

def set_top_language(place, top):
    languages = place.languages()
    cookie_name = 'language_order'
    place_identifier = f'{place.osm_type}/{place.osm_id}'

    cookie = read_language_order()
    current_order = cookie.get(place_identifier) or [l['code'] for l in languages]
    current_order.remove(top)
    cookie[place_identifier] = [top] + current_order

    flash('language order updated')
    response = make_response(redirect(place.candidates_url()))
    response.set_cookie(cookie_name, json.dumps(cookie), samesite='Strict')
    return response

@app.route('/languages/<osm_type>/<int:osm_id>')
def switch_languages(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)

    top = request.args.get('top')
    if top:
        return set_top_language(place, top)

    languages = place.languages()
    for l in languages:
        l['lang'] = get_wikidata_language(l['code'])

    return render_template('switch_languages.html',
                           place=place,
                           languages=languages)

def read_language_order():
    cookie_name = 'language_order'
    cookie_json = request.cookies.get(cookie_name)
    return json.loads(cookie_json) if cookie_json else {}

def languages_in_user_order(user_language_order, languages):
    lookup = {l['code']: l for l in languages}
    return [lookup[code] for code in user_language_order if code in lookup]

def get_place_language_with_counts(place):
    g.default_languages = place.languages()
    languages = g.default_languages[:]

    cookie = read_language_order()
    if place.identifier in cookie:
        lookup = {l['code']: l for l in languages}
        languages = [lookup[code] for code in cookie[place.identifier]
                     if code in lookup]

    for l in languages:
        l['lang'] = get_wikidata_language(l['code'])

    return languages

def get_place_language(place):
    return [l['lang']
            for l in get_place_language_with_counts(place)
            if l['lang']]

@app.route('/save_language_order/<osm_type>/<int:osm_id>')
def save_language_order(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)
    order = request.args.get('order')
    if not order:
        flash('order parameter missing')
        url = place.place_url('switch_languages')
        return redirect(url)

    cookie_name = 'language_order'
    place_identifier = f'{osm_type}/{osm_id}'

    cookie = read_language_order()
    cookie[place_identifier] = order.split(';')

    flash('language order updated')
    response = make_response(redirect(place.candidates_url()))
    response.set_cookie(cookie_name, json.dumps(cookie))
    return response

def clear_languard_cookie():
    cookie_name = 'language_order'
    cookie = {}

    flash('language order cleared')
    response = make_response(redirect(url_for(request.endpoint)))
    response.set_cookie(cookie_name, json.dumps(cookie))
    return response

@app.route('/debug/languages', methods=['GET', 'POST'])
def debug_languages():
    if request.method == 'POST':
        if request.form.get('clear') == 'yes':
            return clear_languard_cookie()

        return redirect(url_for(request.endpoint))
    cookie = read_language_order()
    place_list = []
    all_codes = set(utils.flatten(cookie.values()))
    lookup = {}
    q = Language.query.filter(Language.wikimedia_language_code.in_(all_codes))
    for lang in q:
        lookup[lang.wikimedia_language_code] = lang

    for key, language_codes in cookie.items():
        osm_type, _, osm_id = key.partition('/')
        place = Place.get_by_osm(osm_type, osm_id)
        if not place:
            continue
        place_list.append((place, [lookup.get(code, code) for code in language_codes]))

    return render_template('debug/languages.html', place_list=place_list)

@app.route('/mobile/<osm_type>/<int:osm_id>')
def mobile(osm_type, osm_id):
    # FIXME: this is unfinished work
    place = Place.get_or_abort(osm_type, osm_id)
    items = place.get_candidate_items()

    filtered = {item.item_id: match
                for item, match in matcher.filter_candidates_more(items, bad=get_bad(items))}

    return render_template('mobile.html',
                           place=place,
                           osm_id=osm_id,
                           osm_type=osm_type,
                           filtered=filtered,
                           candidates=items)

def check_still_auth():
    if not g.user.is_authenticated:
        return
    r = osm_oauth.api_request('user/details')
    if r.status_code == 401:
        flash('Not authenticated with OpenStreetMap')
        flask_login.logout_user()

@app.route('/debug/user/details')
def debug_user_details():
    r = osm_oauth.api_request('user/details')
    content_type = r.headers.get('Content-Type') or 'text/plain'
    return r.text, r.status_code, {'Content-Type': content_type}

@app.route('/place/<osm_type>/<int:osm_id>.json')
def export_place(osm_type, osm_id):
    '''
    Export place, items and match candidates in JSON format.

    This is to make it possible to copy a place from production to a dev server.
    '''

    place = Place.get_or_abort(osm_type, osm_id)
    return jsonify(export.place_for_export(place))

def redirect_to_candidates(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)
    if not place:
        abort(404)

    if place.state in ('ready', 'complete'):
        return redirect(place.candidates_url())
    else:
        return place.redirect_to_matcher()

@app.route('/relation/<int:osm_id>')
def redirect_from_relation(osm_id):
    return redirect_to_candidates('relation', osm_id)

@app.route('/way/<int:osm_id>')
def redirect_from_way(osm_id):
    return redirect_to_candidates('way', osm_id)

@app.route('/profile/candidates/<osm_type>/<int:osm_id>')
def profile_candidates(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)

    t0 = time()

    profile = []

    def record_timing(name):
        prev = profile[-1][1] if profile else 0
        t = time() - t0
        profile.append((name, t, t - prev))

    demo_mode_active = demo_mode()

    items = place.get_candidate_items()

    record_timing('get candidate items')

    filter_iter = matcher.filter_candidates_more(items,
                                                 bad=get_bad(items),
                                                 ignore_existing=demo_mode_active)
    filtered = {item.item_id: match for item, match in filter_iter}

    record_timing('filter items')

    languages_with_counts = get_place_language_with_counts(place)
    languages = [l['lang'] for l in languages_with_counts if l['lang']]
    record_timing('language')

    if (osm_type, osm_id) in hide_trams:
        items = [item for item in items if not item.is_instance_of('Q2175765')]

    good_match = [i for i in items if filtered.get(i.item_id) and
                                      'candidate' in filtered[i.item_id] and
                                      'note' not in filtered[i.item_id]]
    get_isa_facets(good_match, languages=languages, min_count=3)
    record_timing('get facets')

    isa_filter = set(request.args.getlist('isa') or [])
    if isa_filter:
        items = [item for item in items if item.is_instance_of(isa_filter)]

    unsure_items = []
    ticked_items = []

    for item in items:
        picked = filtered[item.item_id].get('candidate')
        if picked and not picked.checkbox_ticked():
            unsure_items.append(item)
            if 'note' not in filtered[item.item_id]:
                max_dist = picked.get_max_dist()
                if max_dist < picked.dist:
                    note = ('distance between OSM and Wikidata is ' +
                            f'greater than {max_dist:,.0f}m')
                    filtered[item.item_id]['note'] = note
        else:
            ticked_items.append(item)

    record_timing('ticked')

    return render_template('profile_candidates.html',
                           place=place,
                           osm_id=osm_id,
                           profile=profile)

@app.route('/candidates/<osm_type>/<int:osm_id>')
def candidates(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)
    g.country_code = place.country_code

    if place.state not in ('ready', 'complete'):
        return place.redirect_to_matcher()

    candidates_json_url = url_for('candidates_json', osm_type=osm_type, osm_id=osm_id)

    return render_template('candidates.html',
                           place=place,
                           tab_pages=tab_pages,
                           osm_type=osm_type,
                           osm_id=osm_id,
                           candidates_json_url=candidates_json_url)

def language_dict(lang):
    return {
        'qid': lang.qid,
        'wikimedia_language_code': lang.wikimedia_language_code,
        'label': lang.label(with_code=False),
        'english_name': lang.english_name(),
        'self_name': lang.self_name(),
        'site_name': lang.site_name,
    }

def identifier_match(key, osm_value, identifiers):
    if not identifiers or key not in identifiers:
        return False

    assert osm_value

    values = identifiers[key]
    values = set(values) | {i.replace(' ', '') for i in values if ' ' in i}

    if osm_value in values:
        return True
    if ' ' in osm_value and osm_value.replace(' ', '') in values:
        return True
    if key == 'website' and match.any_url_match(osm_value, values):
        return True
    if (osm_value.isdigit() and
            any(v.isdigit() and int(osm_value) == int(v) for v in values)):
        return True

    return False

def candidate_tags(search_tags, identifiers, candidate):
    tags = []
    for k, v in candidate.tags.items():
        if k == 'way_area':
            continue
        tag = k + '=' + v
        match_found = None
        if candidate.name_match and k in candidate.name_match:
            match_found = f'{candidate.name_match_count(k)} name matches'
        if k in search_tags or tag in search_tags:
            match_found = 'item type tag matches'
        if identifier_match(k, v, identifiers):
            match_found = 'identifier matches'
        tags.append((k, v, match_found))
    return tags

@app.route('/candidates/<osm_type>/<int:osm_id>/languages', methods=['POST'])
def candidate_language_order(osm_type, osm_id):
    cookie_name = 'language_order'
    place_identifier = f'{osm_type}/{osm_id}'

    cookie = read_language_order()
    cookie[place_identifier] = request.json['languages']

    response = jsonify(savd=True)
    response.set_cookie(cookie_name, json.dumps(cookie), samesite='Strict')
    return response

@app.route('/candidates/<osm_type>/<int:osm_id>.json')
def candidates_json(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)
    g.country_code = place.country_code
    g.default_languages = place.languages()

    cookie = read_language_order()
    user_language_order = cookie.get(place.identifier) or []

    bad_matches = get_bad_matches(place)
    bad_match_items = {i[0] for i in bad_matches}

    cache = place.match_cache
    if cache:
        languages = cache.pop('languages')
        if user_language_order:
            languages = languages_in_user_order(user_language_order, languages)
        return jsonify(osm_type=osm_type,
                       osm_id=osm_id,
                       languages=languages,
                       **cache)

    languages = []
    langs = []
    for l in g.default_languages:
        lang = get_wikidata_language(l['code'])
        if not lang:
            continue
        langs.append(lang)
        l['lang'] = language_dict(lang)
        languages.append(l)

    osm_count = Counter()

    items = place.get_candidate_items()

    isa_facets = get_isa_facets2(items, languages=langs, min_count=2)

    for item in items:
        for c in item.candidates:
            osm_count[(c.osm_type, c.osm_id)] += 1

    if (osm_type, osm_id) in hide_trams:
        items = [item for item in items if not item.is_instance_of('Q2175765')]

    item_list = []
    isa_lookup = {}
    for item in items:
        candidates = item.candidates
        search_tags = item.tags
        identifiers = item.identifier_values()

        notes = []

        matched_candidates = matcher.reduce_candidates(item, candidates.all())

        if len(matched_candidates) == 1:
            matched_candidate = candidates[0]
            max_dist = matched_candidate.get_max_dist()
            ticked = matched_candidate.checkbox_ticked()
            upload_okay = True
            if not ticked and max_dist < matched_candidate.dist:
                # FIXME update to respect user setting for distance units
                notes.append('distance between OSM and Wikidata is ' +
                             f'greater than {max_dist:,.0f}m')
        else:
            max_dist, ticked = None, None
            upload_okay = False
            notes.append("more than one candidate found")
            matched_candidate = None

        if item.item_id in bad_match_items:
            notes.append('bad match reported')
            upload_okay = False

        lat, lon = item.get_lat_lon()

        isa_list = []
        isa_super_qids = []
        for isa in item.isa:
            if isa.qid not in isa_lookup:
                isa_lookup[isa.qid] = {'labels': isa.label_and_description_list(langs)}

            isa_list.append(isa.qid)

            try:
                super_list = [claim['mainsnak']['datavalue']['value']['id']
                              for claim in isa.entity['claims'].get('P279', [])]
            except TypeError:
                super_list = []

            isa_super_qids += [isa.qid] + super_list

        for candidate in candidates:
            housename = candidate.tags.get('addr:housename')
            if housename and housename.isdigit():
                notes.append('number as house name')
                upload_okay = False

            name = candidate.tags.get('name')
            if name and name.isdigit():
                notes.append('number as name')
                upload_okay = False

            if osm_count[(candidate.osm_type, candidate.osm_id)] > 1:
                notes.append('OSM candidate matches multiple Wikidata items')
                upload_okay = False

        item_list.append({
            'labels': item.label_and_description_list(langs),
            'qid': item.qid,
            'enwiki': item.enwiki,
            'lat': lat,
            'lon': lon,
            'first_paragraphs': item.first_paragraphs(langs),
            'street_addresses': item.get_street_addresses(),
            # 'search_tags': list(item.tags),
            'isa_list': isa_list,
            'isa_super_qids': isa_super_qids,
            'identifiers': [[list(values), label]
                            for values, label in item.identifiers()],
            'defunct_cats': item.defunct_cats(),
            'image_filenames': item.image_filenames(),
            'notes': notes,
            'ticked': ticked,
            'upload_okay': upload_okay,
            'sitelinks': item.sitelinks(),
            'candidates': [{
                'key': c.key,
                'is_best': (matched_candidate and c.key == matched_candidate.key),
                'bad_match_reported': (item.item_id, c.osm_type, c.osm_id) in bad_matches,
                # 'label': c.label_best_language(langs),
                'names': c.names(),
                # 'name': c.name,
                'dist': c.dist,
                'osm_type': c.osm_type,
                'osm_id': c.osm_id,
                'display_distance': c.display_distance(),
                'identifier_match': c.identifier_match,
                'name_match': c.name_match or None,
                'address_match': c.address_match,
                'tags': candidate_tags(search_tags, identifiers, c),
            } for c in candidates],
        })

    if user_language_order:
        languages = languages_in_user_order(user_language_order, languages)

    cache = dict(items=item_list,
                 languages=languages,
                 isa_facets=isa_facets,
                 isa=isa_lookup)

    place.match_cache = cache
    database.session.commit()

    return jsonify(osm_type=osm_type, osm_id=osm_id, **cache)

@app.route('/old/candidates/<osm_type>/<int:osm_id>')
def old_candidates(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)
    g.country_code = place.country_code

    if place.state not in ('ready', 'complete'):
        return place.redirect_to_matcher()

    demo_mode_active = demo_mode()

    if demo_mode_active:
        items = place.items_with_candidates().all()
    else:
        items = place.get_candidate_items()
        check_still_auth()

    filter_iter = matcher.filter_candidates_more(items,
                                                 bad=get_bad(items),
                                                 ignore_existing=demo_mode_active)
    filtered = {item.item_id: match for item, match in filter_iter}

    filter_okay = any('candidate' in m for m in filtered.values())
    upload_okay = any('candidate' in m for m in filtered.values()) and g.user.is_authenticated
    bad_matches = get_bad_matches(place)

    languages_with_counts = get_place_language_with_counts(place)
    languages = [l['lang'] for l in languages_with_counts if l['lang']]

    if (osm_type, osm_id) in hide_trams:
        items = [item for item in items if not item.is_instance_of('Q2175765')]

    good_match = [i for i in items if filtered.get(i.item_id) and
                                      'candidate' in filtered[i.item_id] and
                                      'note' not in filtered[i.item_id]]
    isa_facets = get_isa_facets(good_match, languages=languages, min_count=3)

    isa_filter = set(request.args.getlist('isa') or [])
    if isa_filter:
        items = [item for item in items if item.is_instance_of(isa_filter)]

    full_count = len(items)

    unsure_items = []
    ticked_items = []

    for item in items:
        picked = filtered[item.item_id].get('candidate')
        if picked and not picked.checkbox_ticked():
            unsure_items.append(item)
            if 'note' not in filtered[item.item_id]:
                max_dist = picked.get_max_dist()
                if max_dist < picked.dist:
                    note = f'distance between OSM and Wikidata is greater than {max_dist:,.0f}m'
                    filtered[item.item_id]['note'] = note
        else:
            ticked_items.append(item)

    return render_template('old_candidates.html',
                           place=place,
                           osm_id=osm_id,
                           isa_facets=isa_facets,
                           isa_filter=isa_filter,
                           filter_okay=filter_okay,
                           upload_okay=upload_okay,
                           tab_pages=tab_pages,
                           filtered=filtered,
                           bad_matches=bad_matches,
                           full_count=full_count,
                           candidates=items,
                           languages_with_counts=languages_with_counts,
                           languages=languages,
                           unsure_items=unsure_items,
                           ticked_items=ticked_items)

@app.route('/test_candidates/<osm_type>/<int:osm_id>')
def test_candidates(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)

    multiple_match_count = place.items_with_multiple_candidates().count()

    items = place.items_with_candidates()

    full_count = items.count()
    multiple_match_count = sum(1 for item in items if item.candidates.count() > 1)

    filtered = {item.item_id: match
                for item, match in matcher.filter_candidates_more(items, bad=get_bad(items))}

    filter_okay = any('candidate' in m for m in filtered.values())

    upload_okay = any('candidate' in m for m in filtered.values())
    bad_matches = get_bad_matches(place)

    return render_template('test_candidates.html',
                           place=place,
                           osm_id=osm_id,
                           filter_okay=filter_okay,
                           upload_okay=upload_okay,
                           tab_pages=tab_pages,
                           filtered=filtered,
                           bad_matches=bad_matches,
                           full_count=full_count,
                           multiple_match_count=multiple_match_count,
                           candidates=items)

def get_place(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)
    g.country_code = place.country_code

    if place.state == 'refresh_isa':
        place.load_isa()
        place.state = 'ready'
        database.session.commit()

    if place.state not in ('ready', 'complete'):
        return place.redirect_to_matcher()

    return place

@app.route('/no_match/<osm_type>/<int:osm_id>')
def no_match(osm_type, osm_id):
    place = get_place(osm_type, osm_id)
    if not isinstance(place, Place):
        return place

    full_count = place.items_with_candidates_count()

    items_without_matches = place.items_without_candidates()
    languages = get_place_language(place)

    return render_template('no_match.html',
                           place=place,
                           osm_id=osm_id,
                           tab_pages=tab_pages,
                           items_without_matches=items_without_matches,
                           full_count=full_count,
                           languages=languages)

@app.route('/already_tagged/<osm_type>/<int:osm_id>')
def already_tagged(osm_type, osm_id):
    place = get_place(osm_type, osm_id)
    if not isinstance(place, Place):
        return place

    items = [item for item in place.items_with_candidates()
             if any('wikidata' in c.tags for c in item.candidates)]

    languages = get_place_language(place)
    return render_template('already_tagged.html',
                           place=place,
                           osm_id=osm_id,
                           tab_pages=tab_pages,
                           items=items,
                           languages=languages)

# disable matcher for nodes, it isn't finished
# @app.route('/matcher/node/<int:osm_id>')
def node_is_in(osm_id):
    place = Place.query.filter_by(osm_type='node', osm_id=osm_id).one_or_none()
    if not place:
        abort(404)

    oql = '''[out:json][timeout:180];
node({});
is_in->.a;
(way(pivot.a); rel(pivot.a););
out bb tags;
'''.format(osm_id)

    reply = json.load(open('sample/node_is_in.json'))

    return render_template('node_is_in.html', place=place, oql=oql, reply=reply)

@app.route('/refresh/<osm_type>/<int:osm_id>', methods=['GET', 'POST'])
def refresh_place(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)
    if place.state not in ('ready', 'complete'):
        return place.redirect_to_matcher()

    if request.method != 'POST':  # confirm
        return render_template('refresh.html', place=place)

    place.state = 'refresh'
    place.language_count = None
    place.match_cache = None
    database.session.commit()

    return place.redirect_to_matcher()

def get_existing(sort, name_filter):
    q = Place.query.filter(Place.state.isnot(None), Place.osm_type != 'node')
    if name_filter:
        q = q.filter(Place.display_name.ilike('%' + name_filter + '%'))
    if sort == 'name':
        return q.order_by(Place.display_name)
    if sort == 'area':
        return q.order_by(Place.area)

    existing = q.all()
    if sort == 'match':
        return sorted(existing, key=lambda p: (p.items_with_candidates_count() or 0))
    if sort == 'ratio':
        return sorted(existing, key=lambda p: (p.match_ratio or 0))
    if sort == 'item':
        return sorted(existing, key=lambda p: p.items.count())

    return q

def sort_link(order):
    args = request.view_args.copy()
    args['sort'] = order
    return url_for(request.endpoint, **args)

def add_hit_place_detail(hit):
    if not ('osm_type' in hit and 'osm_id' in hit):
        return
    p = Place.query.filter_by(osm_type=hit['osm_type'],
                              osm_id=hit['osm_id']).one_or_none()
    if p:
        hit['place'] = p
        if p.area:
            hit['area'] = p.area_in_sq_km

@app.route("/random")
def random_city():
    cities = json.load(open('city_list.json'))
    city, country = random.choice(cities)
    q = city + ', ' + country
    return redirect(url_for('search_results', q=q))

@app.route("/search")
def search_results():
    ''' Search for OSM places using the nominatim API and show the results. '''
    q = request.args.get('q') or ''
    if not q:
        return render_template('results_page.html', results=[], q=q)

    q = q.strip()
    url = search.check_for_search_identifier(q)
    if url:
        return redirect(url)

    try:
        results = search.run(q)
    except search.SearchError:
        message = 'nominatim API search error'
        return render_template('error_page.html', message=message)

    search.update_search_results(results)

    dest = search.handle_redirect_on_single(results)
    if dest:
        return dest

    for hit in results:
        add_hit_place_detail(hit)

    results = search.convert_hits_to_objects(results)

    return render_template('results_page.html', results=results, q=q)

@app.route('/instance_of/Q<item_id>')
def instance_of_page(item_id):
    qid = f'Q{item_id}'
    entity = wikidata_api.get_entity(qid)

    en_label = entity['labels']['en']['value']

    query = wikidata.instance_of_query.replace('QID', qid)
    rows = wikidata.run_query(query)

    items = []
    for row in rows:
        item = {
            'label': row['itemLabel']['value'],
            'has_coords': bool(row.get('location', {}).get('value')),
            'country': row.get('countryLabel', {}).get('value'),
            'id': wikidata.wd_uri_to_id(row['item']['value']),
        }
        items.append(item)

    return render_template('instance_of.html',
                           qid=qid,
                           en_label=en_label,
                           entity=entity,
                           items=items)

@app.route('/')
def index():
    q = request.args.get('q')
    if q:
        return redirect(url_for('search_results', q=q))

    return render_template('index.html')

@app.route('/criteria')
def criteria_page():
    entity_types = matcher.load_entity_types()

    taginfo = get_taginfo(entity_types)

    for t in entity_types:
        t.setdefault('name', t['cats'][0].replace(' by country', ''))
        for tag in t['tags']:
            if '=' not in tag:
                continue
            image = taginfo.get(tag, {}).get('image')
            if image:
                t['image'] = image
                break

    entity_types.sort(key=lambda t: t['name'].lower())

    cat_counts = {cat.name: cat.page_count for cat in Category.query}

    return render_template('criteria.html',
                           entity_types=entity_types,
                           cat_counts=cat_counts,
                           taginfo=taginfo)

@app.route("/documentation")
def documentation():
    return redirect('https://github.com/EdwardBetts/osm-wikidata/blob/master/README.md')

@app.route('/changes')
def changesets():
    q = Changeset.query.filter(Changeset.update_count > 0).order_by(Changeset.id.desc())

    page = get_int_arg('page') or 1
    per_page = 100
    pager = Pagination(page, per_page, q.count())

    return render_template('changesets.html', objects=pager.slice(q), pager=pager)

@app.route('/browse/')
def browse_index():
    query = wikidata.continents_with_country_count_query
    rows = wikidata.run_query(query)
    items = []
    banner_filenames = []
    for row in rows:
        item = {
            'label': row['continentLabel']['value'],
            'description': row['continentDescription']['value'],
            'country_count': row['count']['value'],
            'qid': wikidata.wd_to_qid(row['continent']),
        }
        try:
            filename = commons.commons_uri_to_filename(row['banner']['value'])
            item['banner'] = filename
            banner_filenames.append(filename)
        except KeyError:
            pass
        items.append(item)
        row['item'] = item
    images = commons.image_detail(banner_filenames)
    for item in items:
        banner = item.get('banner')
        if not banner:
            continue
        item['banner_url'] = images[banner]['url']
    return render_template('browse_index.html', items=items)

@app.route('/browse/Q<int:item_id>')
def browse_page(item_id):
    timing = [('start', time())]
    timing.append(('get place done', time()))

    lang = request.args.get('lang')
    sort = request.args.get('sort')
    details = browse.get_details(item_id, timing, lang, sort)

    timing.append(('start get banner', time()))
    banner = wikidata.page_banner_from_entity(details['entity'])
    timing.append(('get banner done', time()))

    start = timing[0][1]
    timing = [(name, t - start) for name, t in timing]

    database.session.commit()
    return render_template('browse.html', banner=banner, timing=timing, **details)

@app.route('/matcher/Q<int:item_id>')
def matcher_wikidata(item_id):
    def get_search_string(qid):
        entity = wikidata_api.get_entity(qid)
        return browse.qid_to_search_string(qid, entity)

    qid = 'Q{}'.format(item_id)
    place = Place.get_by_wikidata(qid)
    if place:  # already in the database
        if place.osm_type == 'node':
            q = get_search_string(qid)
            session['redirect_on_single'] = True
            return redirect(url_for('search_results', q=q))
        return place.redirect_to_matcher()

    q = get_search_string(qid)
    place = browse.place_from_qid(qid, q=q)
    # search using wikidata query and nominatim
    if place and place.osm_type != 'node':
        return place.redirect_to_matcher()

    # give up and redirect to search page
    session['redirect_on_single'] = True
    return redirect(url_for('search_results', q=q))

def get_tag_list(sort):
    count = func.count(distinct(Item.item_id))
    order_by = ([count, ItemTag.tag_or_key] if sort == 'count' else [ItemTag.tag_or_key])
    q = (database.session.query(ItemTag.tag_or_key, func.count(distinct(Item.item_id)))
                         .join(Item)
                         .join(ItemCandidate)
                         # .filter(ItemTag.tag_or_key == sub.c.tag_or_key)
                         .group_by(ItemTag.tag_or_key)
                         .order_by(*order_by))

    return [(tag, num) for tag, num in q]

@app.route('/tags')
def tag_list():
    abort(404)
    q = get_tag_list(request.args.get('sort'))
    return render_template('tag_list.html', q=q)

@app.route('/tags/<tag_or_key>')
def tag_page(tag_or_key):
    abort(404)
    sub = (database.session.query(Item.item_id)
              .join(ItemTag)
              .join(ItemCandidate)
              .filter(ItemTag.tag_or_key == tag_or_key)
              .group_by(Item.item_id)
              .subquery())

    q = Item.query.filter(Item.item_id == sub.c.item_id)

    return render_template('tag_page.html', tag_or_key=tag_or_key, q=q)

@app.route('/bad_match/Q<int:item_id>/<osm_type>/<int:osm_id>', methods=['POST'])
def bad_match(item_id, osm_type, osm_id):
    comment = request.form.get('comment') or None

    bad = BadMatch(item_id=item_id,
                   osm_type=osm_type,
                   osm_id=osm_id,
                   comment=comment,
                   user=g.user)

    database.session.add(bad)
    database.session.commit()
    return Response('saved', mimetype='text/plain')

@app.route('/detail/Q<int:item_id>/<osm_type>/<int:osm_id>', methods=['GET', 'POST'])
def match_detail(item_id, osm_type, osm_id):
    osm = (ItemCandidate.query
                        .filter_by(item_id=item_id, osm_type=osm_type, osm_id=osm_id)
                        .one_or_none())
    if not osm:
        abort(404)

    item = osm.item
    item.set_country_code()

    qid = 'Q' + str(item_id)
    wikidata_names = dict(wikidata.names_from_entity(item.entity))
    lat, lon = item.coords()
    assert lat is not None and lon is not None

    return render_template('match_detail.html',
                           item=item,
                           osm=osm,
                           category_map=item.category_map,
                           qid=qid,
                           lat=lat,
                           lon=lon,
                           wikidata_names=wikidata_names,
                           entity=item.entity)

def build_item_page(wikidata_id, item):
    qid = 'Q' + str(wikidata_id)
    if item and item.entity:
        entity = wikidata.WikidataItem(qid, item.entity)
    else:
        entity = wikidata.WikidataItem.retrieve_item(qid)

    if not entity:
        abort(404)

    entity.report_broken_wikidata_osm_tags()

    osm_keys = entity.osm_keys
    wikidata_osm_tags = wikidata.parse_osm_keys(osm_keys)
    entity.report_broken_wikidata_osm_tags()
    if item:
        languages = [lang for lang in
                     (get_wikidata_language(l['code']) for l in item.place_languages())
                     if lang]
    else:
        languages = None

    criteria = entity.criteria()

    if item:  # add criteria from the Item object
        criteria |= item.criteria

    if item and item.candidates:
        filtered = {item.item_id: candidate
                    for item, candidate in matcher.filter_candidates_more([item])}
    else:
        filtered = {}

    is_proposed = item.is_proposed() if item else entity.is_proposed()

    if not entity.has_coords or not criteria or is_proposed:
        return render_template('item_page.html',
                               item=item,
                               entity=entity,
                               wikidata_query=entity.osm_key_query(),
                               wikidata_osm_tags=wikidata_osm_tags,
                               languages=languages,
                               criteria=criteria,
                               filtered=filtered,
                               qid=qid,
                               is_proposed=is_proposed)

    criteria = wikidata.flatten_criteria(criteria)

    radius = utils.get_radius()
    oql = entity.get_oql(criteria, radius)
    if item:
        overpass_reply = []
    else:
        try:
            overpass_reply = overpass.item_query(oql, qid, radius)
        except overpass.RateLimited:
            return render_template('error_page.html',
                                   message='Overpass rate limit exceeded')
        except overpass.Timeout:
            return render_template('error_page.html',
                                   message='Overpass timeout')

    found = entity.parse_item_query(criteria, overpass_reply)

    upload_option = False
    if g.user.is_authenticated:
        if item:
            upload_option = any(not c.wikidata_tag for c in item.candidates)
            q = (database.session.query(BadMatch.item_id)
                                 .filter(BadMatch.item_id == item.item_id))
            if q.count():
                upload_option = False
        elif found:
            upload_option = any('wikidata' not in c['tags'] for c, _ in found)

    return render_template('item_page.html',
                           item=item,
                           wikidata_query=entity.osm_key_query(),
                           entity=entity,
                           languages=languages,
                           wikidata_osm_tags=wikidata_osm_tags,
                           overpass_reply=overpass_reply,
                           upload_option=upload_option,
                           filtered=filtered,
                           oql=oql,
                           qid=qid,
                           found=found,
                           osm_keys=osm_keys)

@app.route('/Q<int:wikidata_id>')
def item_page(wikidata_id):
    check_still_auth()
    item = Item.query.get(wikidata_id)
    if item:
        item.set_country_code()
    try:
        return build_item_page(wikidata_id, item)
    except (requests.exceptions.SSLError,
            requests.exceptions.ConnectionError,
            wikidata_api.QueryError):
        return render_template('error_page.html',
                               message="query.wikidata.org isn't working")

@app.route('/reports/edit_match')
def reports_view():
    timestamp = request.args.get('timestamp')
    if timestamp:
        q = EditMatchReject.query.filter_by(report_timestamp=timestamp)
        return render_template('reports/edit_match.html', q=q)
    q = (database.session.query(EditMatchReject.report_timestamp, func.count())
                         .group_by(EditMatchReject.report_timestamp))
    hide = request.args.get('hide')
    if hide == 'farmhouse':
        q = q.filter(~EditMatchReject.edit.candidate.item.query_label.like('%farm%house%'))
    return render_template('reports/list.html', q=q)

@app.route('/reports/isa')
def isa_list_report():
    q = (database.session.query(IsA.item_id, IsA.label, func.count())
                         .join(ItemIsA)
                         .group_by(IsA.item_id, IsA.label)
                         .order_by(func.count().desc())
                         .limit(100))
    return render_template('reports/isa_list.html', q=q)

@app.route('/reports/isa/Q<int:isa_id>')
def isa_item_report(isa_id):
    item = IsA.query.get(isa_id)
    return render_template('reports/isa_item.html', item=item, isa_id=isa_id)

@app.route('/reports/isa/Q<int:isa_id>/refresh', methods=['POST'])
def isa_item_refresh(isa_id):
    item = IsA.query.get(isa_id)
    qid = f'Q{isa_id}'
    item.entity = wikidata_api.get_entity(qid)
    database.session.commit()
    flash('IsA item refreshed')
    return redirect(url_for('isa_item_report', isa_id=isa_id))

@app.route('/report/old_places')
@flask_login.login_required
def old_places():
    rows = database.get_old_place_list()
    items = [{
        'place_id': place_id,
        'size': size,
        'added': added,
        'candidates_url': url_for('candidates', osm_type=osm_type, osm_id=osm_id),
        'display_name': display_name,
        'state': state,
        'changesets': changeset_count,
        'recent': recent,
    } for place_id, osm_type, osm_id, added, size, display_name, state, changeset_count, recent in rows]

    free_space = utils.get_free_space(app.config)

    return render_template('space.html', items=items, free_space=free_space)

@app.route('/delete/<int:place_id>', methods=['POST', 'DELETE'])
@flask_login.login_required
def delete_place(place_id):
    place = Place.query.get(place_id)
    place.clean_up()

    flash('{} deleted'.format(place.display_name))
    to_next = request.args.get('next', 'space')
    return redirect(url_for(to_next))

@app.route('/delete', methods=['POST', 'DELETE'])
@flask_login.login_required
def delete_places():
    place_list = request.form.getlist('place')
    for place_id in place_list:
        place = Place.query.get(place_id)
        place.clean_up()

    flash(f'{len(place_list)} places deleted')
    to_next = request.form.get('next', 'db_space')
    return redirect(url_for(to_next))

@app.route('/account')
@flask_login.login_required
def account_page():
    return render_template('user/account.html', user=g.user)

@app.route('/account/settings', methods=['GET', 'POST'])
@flask_login.login_required
def account_settings_page():
    form = AccountSettingsForm()
    if request.method == 'GET':
        if g.user.single:
            form.single.data = g.user.single
        if g.user.multi:
            form.multi.data = g.user.multi
        if g.user.units:
            form.units.data = g.user.units

        form.wikipedia_tag.data = g.user.wikipedia_tag

    if form.validate_on_submit():
        form.populate_obj(g.user)
        database.session.commit()
        flash('Account settings saved.')
        return redirect(url_for(request.endpoint))
    return render_template('user/settings.html', form=form)

@app.route('/item_candidate/Q<int:item_id>.json')
def item_candidate_json(item_id):
    item = Item.query.get(item_id)
    if item is None:
        return jsonify(qid=f'Q{item_id}', candidates=[])

    candidates = [{
        'osm_id': c.osm_id,
        'osm_type': c.osm_type,
        'geojson': json.loads(c.geojson),
        'key': c.key,
    } for c in item.candidates if c.geojson]

    return jsonify(qid=item.qid, candidates=candidates)

@app.route('/debug/<osm_type>/<int:osm_id>/Q<int:item_id>')
def single_item_match(osm_type, osm_id, item_id):
    place = get_place(osm_type, osm_id)
    if not isinstance(place, Place):
        return place

    # qid = f'Q{item_id}'
    item = Item.query.get(item_id)

    tables = database.get_tables()
    ready = all(f'osm_{place.place_id}_{i}' in tables
                for i in ('line', 'point', 'polygon'))

    if not ready:
        return render_template('place_not_ready.html', item=item, place=place)

    endings = matcher.get_ending_from_criteria(item.tags)
    endings |= item.more_endings_from_isa()

    conn = database.session.bind.raw_connection()
    cur = conn.cursor()

    candidates = matcher.find_item_matches(cur, item, place.prefix, debug=False)

    for c in candidates:
        del c['geom']
        if 'way_area' in c['tags']:
            del c['tags']['way_area']

    return render_template('single_item_match.html',
                           item=item,
                           endings=endings,
                           place=place,
                           dict=dict,
                           candidates=candidates)
