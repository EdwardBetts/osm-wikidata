from . import (database, nominatim, wikidata, matcher, user_agent_headers,
               overpass, mail, browse, edit)
from .utils import cache_filename, get_radius, get_int_arg, is_bot
from .model import Item, ItemCandidate, User, Category, Changeset, ItemTag, BadMatch, Timing, get_bad, Language
from .place import Place, get_top_existing
from .taginfo import get_taginfo
from .match import check_for_match
from .pager import Pagination, init_pager
# from .forms import AccountSettingsForm

from flask import Flask, render_template, request, Response, redirect, url_for, g, jsonify, flash, abort, make_response
from flask_login import current_user, logout_user, LoginManager, login_required
from lxml import etree
from social.apps.flask_app.routes import social_auth
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy.orm import load_only
from sqlalchemy import func, distinct
from werkzeug.exceptions import InternalServerError
from geopy.distance import distance
from jinja2 import evalcontextfilter, Markup, escape
from time import time, sleep
from dogpile.cache import make_region
from dukpy.webassets import BabelJS
from werkzeug.debug.tbtools import get_current_traceback

from .matcher_view import matcher_blueprint
from .websocket import ws

from flask_sockets import Sockets

import json
import flask_assets
import webassets.filter
import operator
import sys
import requests
import os.path
import re
import random

_paragraph_re = re.compile(r'(?:\r\n|\r|\n){2,}')

re_qid = re.compile('^(Q\d+)$')

app = Flask(__name__)
app.register_blueprint(matcher_blueprint)
sockets = Sockets(app)
sockets.register_blueprint(ws)
init_pager(app)
env = flask_assets.Environment(app)
app.register_blueprint(social_auth)
login_manager = LoginManager(app)
login_manager.login_view = 'login_route'

cat_to_ending = None
osm_api_base = 'https://api.openstreetmap.org/api/0.6'
really_save = True

region = make_region().configure(
    'dogpile.cache.pylibmc',
    expiration_time=3600,
    arguments={'url': ["127.0.0.1"]}
)

navbar_pages = {
    'criteria_page': 'Criteria',
    'saved_places': 'Places',
    'tag_list': 'Search tags',
    'documentation': 'Documentation',
    'changesets': 'Recent changes',
    'random_city': 'Random',
}

tab_pages = [
    {'route': 'candidates', 'label': 'Match candidates'},
    {'route': 'already_tagged', 'label': 'Already tagged'},
    {'route': 'no_match', 'label': 'No match'},
    {'route': 'wikidata_page', 'label': 'Wikidata query'},
    {'route': 'overpass_query', 'label': 'Overpass query'},
]

webassets.filter.register_filter(BabelJS)
js_lib = webassets.Bundle('jquery/jquery.js',
                          'bootstrap4/js/bootstrap.js',
                          filters='jsmin')
js_app = webassets.Bundle('js/app.js',
                          filters='babeljs')

env.register('js', js_lib, js_app, output='gen/pack.js')

env.register('style', 'css/style.css', 'bootstrap4/css/bootstrap.css',
             filters='cssmin', output='gen/pack.css')

env.register('add_tags', 'js/add_tags.js',
             filters='babeljs', output='gen/add_tags.js')
env.register('matcher', 'js/matcher.js',
             filters='babeljs', output='gen/matcher.js')
env.register('node_is_in', 'js/node_is_in.js',
             filters='babeljs', output='gen/node_is_in.js')

@app.template_filter()
@evalcontextfilter
def newline_br(eval_ctx, value):
    result = u'\n\n'.join(u'<p>%s</p>' % p.replace('\n', '<br>\n') \
        for p in _paragraph_re.split(escape(value)))
    if eval_ctx.autoescape:
        result = Markup(result)
    return result

@app.context_processor
def filter_urls():
    name_filter = g.get('filter')
    try:
        if name_filter:
            url = url_for('saved_with_filter',
                          name_filter=name_filter.replace(' ', '_'))
        else:
            url = url_for('saved_places')
    except RuntimeError:
        return {}  # maybe we don't care
    return dict(url_for_saved=url)

@app.before_request
def global_user():
    g.user = current_user._get_current_object()

@app.before_request
def slow_crawl():
    if is_bot():
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
    return redirect(url_for('social.auth',
                            backend='openstreetmap',
                            next=request.args.get('next')))

@app.route('/logout')
@login_required
def logout():
    next_url = request.args.get('next') or url_for('index')
    logout_user()
    flash('you are logged out')
    return redirect(next_url)

@app.route('/done/')
def done():
    flash('login successful')
    return redirect(url_for('index'))

def reraise(tp, value, tb=None):
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value

@app.errorhandler(InternalServerError)
def exception_handler(e):
    tb = get_current_traceback()
    return render_template('show_error.html', tb=tb), 500

    exc_type, exc_value, tb = sys.exc_info()

    if exc_value is e:
        reraise(exc_type, exc_value, tb)
    else:
        raise e

@app.route('/add_wikidata_tag', methods=['POST'])
def add_wikidata_tag():
    '''Add wikidata tags for a single item'''
    wikidata_id = request.form['wikidata']
    osm = request.form.get('osm')
    if osm:
        osm_type, _, osm_id = osm.partition('/')
    elif 'osm_id' in request.form and 'osm_type' in request.form:
        osm_id = request.form['osm_id']  # old form paramters
        osm_type = request.form['osm_type']
    else:
        flash('no candidate selected')
        return redirect(url_for('item_page', wikidata_id=wikidata_id[1:]))

    user = g.user
    assert user.is_authenticated

    url = '{}/{}/{}'.format(osm_api_base, osm_type, osm_id)
    r = requests.get(url, headers=user_agent_headers())
    root = etree.fromstring(r.content)

    if root.find('.//tag[@k="wikidata"]'):
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

@app.route('/export/wikidata_<osm_type>_<int:osm_id>_<name>.osm')
def export_osm(osm_type, osm_id, name):
    place = Place.get_or_abort(osm_type, osm_id)
    items = place.items_with_candidates()

    items = list(matcher.filter_candidates_more(items, bad=get_bad(items)))

    if not any('candidate' in match for _, match in items):
        abort(404)

    items = [(item, match['candidate']) for item, match in items if 'candidate' in match]

    lookup = {}
    for item, osm in items:
        lookup[(osm.osm_type, osm.osm_id)] = item

    filename = cache_filename('{}_{}_overpass_export.xml'.format(osm_type, osm_id))
    if os.path.exists(filename):
        overpass_xml = open(filename, 'rb').read()
    else:
        overpass_xml = overpass.items_as_xml(items)
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
        e.attrib['action'] = 'modify'
        tag = etree.Element('tag', k='wikidata', v=item.qid)
        e.append(tag)

    xml = etree.tostring(root, pretty_print=True)
    return Response(xml, mimetype='text/xml')

def redirect_to_matcher(place):
    return redirect(place.matcher_progress_url())

@app.route('/filtered/<name_filter>/candidates/<osm_type>/<int:osm_id>')
def candidates_with_filter(name_filter, osm_type, osm_id):
    g.filter = name_filter.replace('_', ' ')
    return candidates(osm_type, osm_id)

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

@app.route('/add_tags/<osm_type>/<int:osm_id>', methods=['POST'])
def add_tags(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)

    include = request.form.getlist('include')
    items = Item.query.filter(Item.item_id.in_([i[1:] for i in include])).all()

    table = [(item, match['candidate'])
             for item, match in matcher.filter_candidates_more(items, bad=get_bad(items))
             if 'candidate' in match]

    items = [{'qid': i.qid,
              'osm_type': c.osm_type,
              'osm_id': c.osm_id,
              'description': '{} {}: adding wikidata={}'.format(c.osm_type, c.osm_id, i.qid)}
            for i, c in table]

    url_scheme = request.environ.get('wsgi.url_scheme')
    ws_scheme = 'wss' if url_scheme == 'https' else 'ws'

    return render_template('add_tags.html',
                           place=place,
                           osm_id=osm_id,
                           ws_scheme=ws_scheme,
                           items=items,
                           table=table)

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

@app.route('/languages/<osm_type>/<int:osm_id>')
def switch_languages(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)

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

def get_place_language_with_counts(place):
    languages = place.languages()

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

@app.route('/candidates/<osm_type>/<int:osm_id>')
def candidates(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)
    multiple_only = bool(request.args.get('multiple'))

    if place.state == 'overpass_error':
        error = open(place.overpass_filename).read()
        return render_template('candidates.html',
                               overpass_error=error,
                               place=place)

    if place.state not in ('ready', 'complete'):
        return redirect_to_matcher(place)

    multiple_match_count = place.items_with_multiple_candidates().count()

    if multiple_only:
        item_ids = [i[0] for i in place.items_with_multiple_candidates()]
        if not item_ids:
            items = Item.query.filter(0 == 1)
        else:
            items = Item.query.filter(Item.item_id.in_(item_ids))
    else:
        items = place.items_with_candidates()

    items = [item for item in items
             if all('wikidata' not in c.tags for c in item.candidates)]

    full_count = len(items)
    multiple_match_count = sum(1 for item in items if item.candidates.count() > 1)

    filtered = {item.item_id: match
                for item, match in matcher.filter_candidates_more(items, bad=get_bad(items))}

    filter_okay = any('candidate' in m for m in filtered.values())

    upload_okay = any('candidate' in m for m in filtered.values()) and g.user.is_authenticated
    bad_matches = get_bad_matches(place)

    languages_with_counts = get_place_language_with_counts(place)
    languages = [l['lang'] for l in languages_with_counts if l['lang']]

    return render_template('candidates.html',
                           place=place,
                           osm_id=osm_id,
                           filter_okay=filter_okay,
                           upload_okay=upload_okay,
                           tab_pages=tab_pages,
                           multiple_only=multiple_only,
                           filtered=filtered,
                           bad_matches=bad_matches,
                           full_count=full_count,
                           multiple_match_count=multiple_match_count,
                           candidates=items,
                           languages_with_counts=languages_with_counts,
                           languages=languages)

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

    if place.state not in ('ready', 'complete'):
        return redirect_to_matcher(place)

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
        return redirect_to_matcher(place)

    if request.method != 'POST':  # confirm
        return render_template('refresh.html', place=place)

    refresh_type = request.form['type']

    place.reset_all_items_to_not_done()

    if refresh_type == 'matcher':
        place.state = 'osm2pgsql'
        database.session.commit()
        return redirect_to_matcher(place)

    assert refresh_type == 'full'
    place.delete_overpass()
    place.state = 'refresh'

    engine = database.session.bind
    for t in database.get_tables():
        if not t.startswith(place.prefix):
            continue
        engine.execute('drop table if exists {}'.format(t))
    engine.execute('commit')
    database.session.commit()

    expect = [place.prefix + '_' + t for t in ('line', 'point', 'polygon')]
    tables = database.get_tables()
    assert not any(t in tables for t in expect)

    place.refresh_nominatim()
    database.session.commit()
    return redirect_to_matcher(place)

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

def update_search_results(results):
    need_commit = False
    for hit in results:
        if not ('osm_type' in hit and 'osm_id' in hit and 'geotext' in hit):
            continue
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
    q = request.args.get('q') or ''
    if not q:
        return render_template('results_page.html', results=[], q=q)

    m = re_qid.match(q.strip())
    if m:
        return redirect(url_for('item_page', wikidata_id=m.group(1)[1:]))

    try:
        results = nominatim.lookup(q)
    except nominatim.SearchError:
        message = 'nominatim API search error'
        return render_template('error_page.html', message=message)

    update_search_results(results)

    for hit in results:
        add_hit_place_detail(hit)

    return render_template('results_page.html', results=results, q=q)

@region.cache_on_arguments()
def get_place_cards():
    return render_template('top_places.html', existing=get_top_existing())

@app.route('/refresh_index')
def refresh_index():
    get_place_cards.refresh()
    flash('Top place cards refreshed.')
    return redirect(url_for('index'))

@app.route('/instance_of/Q<item_id>')
def instance_of_page(item_id):
    qid = f'Q{item_id}'
    entity = wikidata.get_entity(qid)

    en_label = entity['labels']['en']['value']

    return render_template('instance_of.html',
                           qid=qid,
                           en_label=en_label,
                           entity=entity)

@app.route('/is_in/way/<way_id>')
def is_in_way(way_id):

    oql = f'''
[out:json][timeout:5];
way({way_id});>;
is_in->.a;
(way(pivot.a); rel(pivot.a););
out tags;
'''
    elements = overpass.get_elements(oql)
    osm_type = 'way'

    name_by_admin_level = {}
    for e in elements:
        tags = e['tags']
        name = tags.get('name')
        admin_level = tags.get('admin_level')
        if name and admin_level:
            name_by_admin_level[int(admin_level)] = name

    q = ', '.join(v for k, v in sorted(name_by_admin_level.items(), reverse=True))
    return redirect(url_for('search_results', q=q))

@app.route('/')
def index():
    q = request.args.get('q')
    if q:
        return redirect(url_for('search_results', q=q))

    if 'filter' in request.args:
        arg_filter = request.args['filter'].strip().replace(' ', '_')
        if arg_filter:
            return redirect(url_for('saved_with_filter', name_filter=arg_filter))
        else:
            return redirect(url_for('saved_places'))

    return render_template('index.html', place_cards=get_place_cards())

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

@app.route('/filtered/<name_filter>')
def saved_with_filter(name_filter):
    g.filter = name_filter.replace('_', ' ')
    return saved_places()

@region.cache_on_arguments()
def get_place_tbody(sort):
    return render_template('place_tbody.html', existing=get_existing(sort, None))

@app.route('/places')
def saved_places():
    abort(404)
    if 'filter' in request.args:
        arg_filter = request.args['filter'].strip().replace(' ', '_')
        if arg_filter:
            return redirect(url_for('saved_with_filter', name_filter=arg_filter))
        else:
            return redirect(url_for('saved_places'))

    sort = request.args.get('sort') or 'name'
    name_filter = g.get('filter') or None

    if name_filter:
        place_tbody = render_template('place_tbody.html',
                                      existing=get_existing(sort, name_filter))
    else:
        place_tbody = get_place_tbody(sort)

    return render_template('saved.html', place_tbody=place_tbody, sort_link=sort_link)

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

def api_overpass_error(data, error):
    data['error'] = error
    data['response'] = 'error'
    response = jsonify(data)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

def api_osm_list(existing, found):
    osm = []
    osm_lookup = {}
    for i in existing:
        index = (i['type'], i['id'])
        i['existing'] = True
        i['match'] = False
        osm.append(i)
        osm_lookup[index] = i
    for i in found:
        index = (i['type'], i['id'])
        if index in osm_lookup:
            osm_lookup[index]['match'] = True
            continue
        i['match'] = True
        i['existing'] = False
        osm.append(i)
    return osm

def api_get(wikidata_id):
    qid = 'Q' + str(wikidata_id)
    entity = wikidata.WikidataItem.retrieve_item(qid)
    if not entity:
        abort(404)

    entity.remove_badges()  # don't need badges in API response

    wikidata_names = entity.names
    entity.trim_location_from_names(wikidata_names)
    entity.report_broken_wikidata_osm_tags()

    criteria = entity.criteria()

    item = Item.query.get(wikidata_id)
    if item:  # add criteria from the Item object
        criteria |= item.criteria

    criteria = wikidata.flatten_criteria(criteria)

    radius = get_radius()
    data = {
        'wikidata': {
            'item': qid,
            'labels': entity.labels,
            'aliases': entity.aliases,
            'sitelinks': entity.sitelinks,
        },
        'search': {
            'radius': radius,
            'criteria': sorted(criteria),
        },
        'found_matches': False,
    }

    if not entity.has_coords:
        return api_overpass_error(data, 'no coordinates')

    lat, lon = entity.coords
    data['wikidata']['lat'] = lat
    data['wikidata']['lon'] = lon

    oql = entity.get_oql(criteria, radius)

    try:
        existing = overpass.get_existing(qid)
    except overpass.RateLimited:
        return api_overpass_error(data, 'overpass rate limited')
    except overpass.Timeout:
        return api_overpass_error(data, 'overpass timeout')

    found = []
    if criteria:
        try:
            overpass_reply = overpass.item_query(oql, qid, radius)
        except overpass.RateLimited:
            return api_overpass_error(data, 'overpass rate limited')
        except overpass.Timeout:
            return api_overpass_error(data, 'overpass timeout')

        endings = matcher.get_ending_from_criteria({i.partition(':')[2] for i in criteria})
        found = [element for element in overpass_reply
                 if check_for_match(element['tags'], wikidata_names, endings=endings)]

    osm = api_osm_list(existing, found)

    for i in osm:
        coords = operator.itemgetter('lat', 'lon')(i.get('center', i))
        i['distance'] = int(distance(coords, (lat, lon)).m);

    data['response'] = 'ok'
    data['found_matches'] = bool(found)
    data['osm'] = osm

    return data

@app.route('/api/1/item/Q<int:wikidata_id>')
def api_item_match(wikidata_id):
    '''API call: find matches for Wikidata item

    Optional parameter: radius (in metres)
    '''

    data = api_get(wikidata_id)

    response = jsonify(data)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@app.route('/api/1/names/Q<int:wikidata_id>')
def api_item_names(wikidata_id):
    api_data = api_get(wikidata_id)

    def json_data(**kwargs):
        response = jsonify(**kwargs)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response

    if not api_data.get('osm'):
        return json_data(found=False,
                         labels=False,
                         message='item not found in OSM')

    for osm in api_data['osm']:
        osm['names'] = {k[5:]: v for k, v in osm['tags'].items()
                        if k.startswith('name:')}
        osm['name_count'] = len(osm['names'])

    picked = max(api_data['osm'], key=lambda osm: osm['name_count'])
    osm_type, osm_id = picked['type'], picked['id']
    url = f'https://www.openstreetmap.org/{osm_type}/{osm_id}'

    data = {}
    if picked['name_count']:
        data['names'] = picked['names']
        data['labels'] = True
    else:
        data['message'] = 'no labels found in OSM'
        data['labels'] = False

    return json_data(found=True,
                     osm_type=osm_type,
                     osm_id=osm_id,
                     osm_url=url,
                     **data)

@app.route('/browse/Q<int:item_id>')
def browse_page(item_id):
    qid = 'Q{}'.format(item_id)
    sort = request.args.get('sort')

    place = Place.get_by_wikidata(qid)
    entity = wikidata.get_entity(qid)

    if not place:
        place = browse.place_from_qid(qid, entity=entity)
        if not place:
            name = entity['labels']['en']['value']
    if place:
        name = place.name

    rows = wikidata.next_level_places(qid, entity)

    if sort and sort in {'area', 'population'}:
        rows.sort(key=lambda i: i[sort] if i[sort] else 0)

    return render_template('browse.html',
                           qid=qid,
                           place=place,
                           name=name,
                           rows=rows)

@app.route('/matcher/Q<int:item_id>')
def matcher_wikidata(item_id):
    qid = 'Q{}'.format(item_id)
    place = Place.get_by_wikidata(qid)
    if place:  # already in the database
        return redirect(place.matcher_progress_url())

    entity = wikidata.get_entity(qid)
    q = browse.qid_to_search_string(qid, entity)
    place = browse.place_from_qid(qid, q=q)
    # search using wikidata query and nominatim
    if place and place.osm_type != 'node':
        return redirect(place.matcher_progress_url())

    # give up and redirect to search page
    return redirect(url_for('search_results', q=q))

@region.cache_on_arguments()
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
                               criteria=criteria,
                               filtered=filtered,
                               qid=qid,
                               is_proposed=is_proposed)

    criteria = wikidata.flatten_criteria(criteria)

    radius = get_radius()
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
    item = Item.query.get(wikidata_id)
    try:
        return build_item_page(wikidata_id, item)
    except wikidata.QueryError:
        return render_template('error_page.html',
                               message="query.wikidata.org isn't working")

@app.route('/space')
def space():
    overpass_dir = app.config['OVERPASS_DIR']
    files = [{'file': f, 'size': f.stat().st_size} for f in os.scandir(overpass_dir) if '_' not in f.name and f.name.endswith('.xml')]
    files.sort(key=lambda f: f['size'], reverse=True)
    files = files[:200]

    place_lookup = {int(f['file'].name[:-4]): f for f in files}
    # q = Place.query.outerjoin(Changeset).filter(Place.place_id.in_(place_lookup.keys())).add_columns(func.count(Changeset.id))
    q = (database.session.query(Place, func.count(Changeset.id))
                         .outerjoin(Changeset)
                         .filter(Place.place_id.in_(place_lookup.keys()))
                         .options(load_only(Place.place_id, Place.display_name, Place.state))
                         .group_by(Place.place_id, Place.display_name, Place.state))
    for place, num in q:
        place_id = place.place_id
        place_lookup[place_id]['place'] = place
        place_lookup[place_id]['changesets'] = num

    return render_template('space.html', files=files)

@app.route('/db_space')
def db_space():
    rows = database.get_big_table_list()
    items = [{
        'place_id': place_id,
        'size': size,
        'display_name': display_name,
        'state': state,
        'changesets': changeset_count
    } for place_id, size, display_name, state, changeset_count in rows]

    return render_template('db_space.html', items=items)

@app.route('/delete/<int:place_id>', methods=['POST', 'DELETE'])
@login_required
def delete_place(place_id):
    place = Place.query.get(place_id)
    place.clean_up()

    flash('{} deleted'.format(place.display_name))
    to_next = request.args.get('next', 'space')
    return redirect(url_for(to_next))

@app.route('/user/<path:username>')
def user_page(username):
    user = User.query.filter(User.username.ilike(username)).one_or_none()
    if not user:
        abort(404)

    return render_template('user_page.html', user=user)

@app.route('/account')
@login_required
def account_page():
    return render_template('user/account.html', user=g.user)

@app.route('/account/settings', methods=['GET', 'POST'])
@login_required
def account_settings_page():
    return render_template('user/settings.html')

@app.route('/item_candidate/Q<int:item_id>.json')
def item_candidate_json(item_id):
    item = Item.query.get(item_id)
    candidates = [{
        'osm_id': c.osm_id,
        'osm_type': c.osm_type,
        'geojson': json.loads(c.geojson),
        'key': c.key,
    } for c in item.candidates if c.geojson]

    return jsonify(qid=item.qid,
                   candidates=candidates)
