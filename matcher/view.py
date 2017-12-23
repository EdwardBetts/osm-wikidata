from . import database, nominatim, wikidata, matcher, user_agent_headers, overpass, mail
from .utils import cache_filename, get_radius, get_int_arg, is_bot
from .model import Item, ItemCandidate, User, Category, Changeset, ItemTag, BadMatch, Timing, get_bad
from .place import Place, get_top_existing
from .taginfo import get_taginfo
from .match import check_for_match
from .pager import Pagination, init_pager

from flask import Flask, render_template, request, Response, redirect, url_for, g, jsonify, flash, abort
from flask_login import current_user, logout_user, LoginManager, login_required
from lxml import etree
from social.apps.flask_app.routes import social_auth
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.orm import load_only
from sqlalchemy import func, distinct
from werkzeug.exceptions import InternalServerError
from geopy.distance import distance
from jinja2 import evalcontextfilter, Markup, escape
from time import time, sleep
from dogpile.cache import make_region
from dukpy.webassets import BabelJS

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
login_manager.login_view = 'login'

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
                          'js/tether.js',
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
    exc_type, exc_value, tb = sys.exc_info()

    if exc_value is e:
        reraise(exc_type, exc_value, tb)
    else:
        raise e

def new_changeset(comment):
    return '''
<osm>
  <changeset>
    <tag k="created_by" v="https://osm.wikidata.link/"/>
    <tag k="comment" v="{}"/>
  </changeset>
</osm>'''.format(comment)

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

    osm_backend, auth = get_backend_and_auth()

    url = '{}/{}/{}'.format(osm_api_base, osm_type, osm_id)
    r = requests.get(url, headers=user_agent_headers())
    root = etree.fromstring(r.content)

    if root.find('.//tag[@k="wikidata"]'):
        flash('no edit needed: OSM element already had wikidata tag')
        return redirect(url_for('item_page', wikidata_id=wikidata_id[1:]))

    comment = request.form.get('comment', 'add wikidata tag')
    changeset = new_changeset(comment)

    r = osm_backend.request(osm_api_base + '/changeset/create',
                            method='PUT',
                            data=changeset,
                            auth=auth,
                            headers=user_agent_headers())
    changeset_id = r.text.strip()

    tag = etree.Element('tag', k='wikidata', v=wikidata_id)
    root[0].set('changeset', changeset_id)
    root[0].append(tag)

    element_data = etree.tostring(root).decode('utf-8')

    try:
        r = osm_backend.request(url,
                                method='PUT',
                                data=element_data,
                                auth=auth,
                                headers=user_agent_headers())
    except requests.exceptions.HTTPError as e:
        r = e.response
        mail.error_mail('error saving element', element_data, r)

        return render_template('error_page.html',
                message="The OSM API returned an error when saving your edit: {}: " + r.text)

    assert(r.text.strip().isdigit())
    for c in ItemCandidate.query.filter_by(osm_id=osm_id, osm_type=osm_type):
        c.tags['wikidata'] = wikidata_id
        flag_modified(c, 'tags')

    change = Changeset(id=changeset_id,
                       item_id=wikidata_id[1:],
                       created=func.now(),
                       comment=comment,
                       update_count=1,
                       user=g.user)

    database.session.add(change)
    database.session.commit()

    r = osm_backend.request(osm_api_base + '/changeset/{}/close'.format(changeset_id),
                            method='PUT',
                            auth=auth,
                            headers=user_agent_headers())

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

@app.route('/close_changeset/<osm_type>/<int:osm_id>', methods=['POST'])
def close_changeset(osm_type, osm_id):
    Place.get_or_abort(osm_type, osm_id)

    osm_backend, auth = get_backend_and_auth()

    changeset_id = request.form['changeset_id']
    update_count = request.form['update_count']

    if really_save:
        osm_backend.request(osm_api_base + '/changeset/{}/close'.format(changeset_id),
                            method='PUT',
                            auth=auth,
                            headers=user_agent_headers())

        change = Changeset.query.get(changeset_id)
        change.update_count = update_count

        database.session.commit()

        # mail.announce_change(change)

    return Response('done', mimetype='text/plain')

@app.route('/open_changeset/<osm_type>/<int:osm_id>', methods=['POST'])
def open_changeset(osm_type, osm_id):
    place = Place.get_or_abort(osm_type, osm_id)

    osm_backend, auth = get_backend_and_auth()

    comment = request.form['comment']
    changeset = new_changeset(comment)

    if not really_save:
        return Response(changeset.id, mimetype='text/plain')

    try:
        r = osm_backend.request(osm_api_base + '/changeset/create',
                                method='PUT',
                                data=changeset.encode('utf-8'),
                                auth=auth,
                                headers=user_agent_headers())
    except requests.exceptions.HTTPError as e:
        mail.error_mail('error creating changeset: ' + place.name, changeset, e.response)
        return Response('error', mimetype='text/plain')
    changeset_id = r.text.strip()
    if not changeset_id.isdigit():
        mail.open_changeset_error(place, changeset, r)
        return Response('error', mimetype='text/plain')

    change = Changeset(id=changeset_id,
                       place=place,
                       created=func.now(),
                       comment=comment,
                       update_count=0,
                       user=g.user)

    database.session.add(change)
    database.session.commit()

    return Response(changeset_id, mimetype='text/plain')

def get_backend_and_auth():
    if not really_save:
        return None, None

    user = g.user
    assert user.is_authenticated

    social_user = user.social_auth.one()
    osm_backend = social_user.get_backend_instance()
    auth = osm_backend.oauth_auth(social_user.access_token)

    return osm_backend, auth

def save_timing(name, t0):
    timing = Timing(start=t0,
                    path=request.full_path,
                    name=name,
                    seconds=time() - t0)
    database.session.add(timing)

@app.route('/post_tag/<osm_type>/<int:osm_id>/Q<int:item_id>', methods=['POST'])
def post_tag(osm_type, osm_id, item_id):
    changeset_id = request.form['changeset_id']

    t0 = time()
    osm_backend, auth = get_backend_and_auth()
    save_timing('backend and auth', t0)

    wikidata_id = 'Q{:d}'.format(item_id)

    t0 = time()
    osm = ItemCandidate.query.filter_by(item_id=item_id, osm_type=osm_type, osm_id=osm_id).one_or_none()
    save_timing('get candidate', t0)

    if not osm:
        database.session.commit()
        return Response('not found', mimetype='text/plain')

    url = '{}/{}/{}'.format(osm_api_base, osm_type, osm_id)
    t0 = time()
    r = requests.get(url, headers=user_agent_headers())
    content = r.content
    save_timing('OSM API get', t0)
    if b'wikidata' in content:
        root = etree.fromstring(content)
        existing = root.find('.//tag[@k="wikidata"]')
        if existing is not None and really_save:
            osm.tags['wikidata'] = existing.get('v')
            flag_modified(osm, 'tags')
        database.session.commit()
        return Response('already tagged', mimetype='text/plain')

    if r.status_code == 410 or r.content == b'':
        database.session.commit()
        return Response('deleted', mimetype='text/plain')

    t0 = time()
    root = etree.fromstring(r.content)
    tag = etree.Element('tag', k='wikidata', v=wikidata_id)
    root[0].set('changeset', changeset_id)
    root[0].append(tag)
    save_timing('build tree', t0)

    element_data = etree.tostring(root).decode('utf-8')
    if really_save:
        t0 = time()
        try:
            r = osm_backend.request(url,
                                    method='PUT',
                                    data=element_data,
                                    auth=auth,
                                    headers=user_agent_headers())
        except requests.exceptions.HTTPError as e:
            mail.error_mail('error saving element', element_data, e.response)
            database.session.commit()
            return Response('save error', mimetype='text/plain')
        if not r.text.strip().isdigit():
            database.session.commit()
            return Response('save error', mimetype='text/plain')
        save_timing('add tag via OSM API', t0)

    t0 = time()
    if really_save:
        osm.tags['wikidata'] = wikidata_id
        flag_modified(osm, 'tags')

    if changeset_id:
        change = Changeset.query.get(changeset_id)
        change.update_count = change.update_count + 1
    save_timing('update database', t0)

    database.session.commit()

    return Response('done', mimetype='text/plain')

def do_add_tags(place, table):
    osm_backend, auth = get_backend_and_auth()

    comment = request.form['comment']
    changeset = new_changeset(comment)

    r = osm_backend.request(osm_api_base + '/changeset/create',
                            method='PUT',
                            data=changeset,
                            auth=auth,
                            headers=user_agent_headers())
    changeset_id = r.text.strip()
    update_count = 0

    for item, osm in table:
        wikidata_id = 'Q{:d}'.format(item.item_id)
        url = '{}/{}/{}'.format(osm_api_base, osm.osm_type, osm.osm_id)
        r = requests.get(url, headers=user_agent_headers())
        if 'wikidata' in r.text:  # done already
            print('skip:', wikidata_id)
            continue

        if r.status_code == 410 or r.content == b'':
            continue  # element has been deleted

        root = etree.fromstring(r.content)
        tag = etree.Element('tag', k='wikidata', v=wikidata_id)
        root[0].set('changeset', changeset_id)
        root[0].append(tag)

        element_data = etree.tostring(root).decode('utf-8')
        r = osm_backend.request(url,
                                method='PUT',
                                data=element_data,
                                auth=auth,
                                headers=user_agent_headers())
        assert(r.text.strip().isdigit())

        osm.tags['wikidata'] = wikidata_id
        flag_modified(osm, 'tags')
        database.session.commit()
        database.session.expire(osm)
        assert osm.tags['wikidata'] == wikidata_id
        update_count += 1

    osm_backend.request(osm_api_base + '/changeset/{}/close'.format(changeset_id),
                        method='PUT',
                        auth=auth,
                        headers=user_agent_headers())

    change = Changeset(id=changeset_id,
                       place=place,
                       created=func.now(),
                       comment=comment,
                       update_count=update_count,
                       user=g.user)

    database.session.add(change)
    database.session.commit()

    mail.announce_change(change)

    return update_count

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

    items = [{'row_id': '{:s}-{:s}-{:d}'.format(i.qid, c.osm_type, c.osm_id),
              'qid': i.qid,
              'osm_type': c.osm_type,
              'osm_id': c.osm_id,
              'description': '{} {}: adding wikidata={}'.format(c.osm_type, c.osm_id, i.qid),
              'post_tag_url': url_for('.post_tag',
                                      item_id=i.item_id,
                                      osm_id=c.osm_id,
                                      osm_type=c.osm_type)} for i, c in table]

    if False and request.form.get('confirm') == 'yes':
        update_count = do_add_tags(place, table)
        flash('{:,d} wikidata tags added to OpenStreetMap'.format(update_count))
        return redirect(place.candidates_url())

    return render_template('add_tags.html',
                           place=place,
                           osm_id=osm_id,
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

    return render_template('no_match.html',
                           place=place,
                           osm_id=osm_id,
                           tab_pages=tab_pages,
                           items_without_matches=items_without_matches,
                           full_count=full_count)

@app.route('/already_tagged/<osm_type>/<int:osm_id>')
def already_tagged(osm_type, osm_id):
    place = get_place(osm_type, osm_id)
    if not isinstance(place, Place):
        return place

    items = [item for item in place.items_with_candidates()
             if any('wikidata' in c.tags for c in item.candidates)]

    return render_template('already_tagged.html',
                           place=place,
                           osm_id=osm_id,
                           tab_pages=tab_pages,
                           items=items)

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

    place.move_overpass_to_backup()
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
    per_page = 50
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

@app.route('/api/1/item/Q<int:wikidata_id>')
def api_item_match(wikidata_id):
    '''API call: find matches for Wikidata item

    Optional parameter: radius (in metres)
    '''

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

    response = jsonify(data)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@app.route('/browse/Q<int:item_id>')
def browse_page(item_id):
    qid = 'Q{}'.format(item_id)

    return render_template('browse.html',
                           qid=qid,
                           rows=wikidata.next_level_places(qid))

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
    q = get_tag_list(request.args.get('sort'))
    return render_template('tag_list.html', q=q)

@app.route('/tags/<tag_or_key>')
def tag_page(tag_or_key):
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

    if not entity.has_coords or not criteria:
        return render_template('item_page.html',
                               item=item,
                               entity=entity,
                               wikidata_query=entity.osm_key_query(),
                               wikidata_osm_tags=wikidata_osm_tags,
                               criteria=criteria,
                               filtered=filtered,
                               qid=qid)

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
