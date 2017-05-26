#!/usr/bin/python3

from flask import Flask, render_template, request, Response, redirect, url_for, g, jsonify, flash, abort
from flask_login import login_user, current_user, logout_user, LoginManager, login_required
from .utils import cache_filename
from lxml import etree
from . import database, nominatim, wikidata, matcher, user_agent_headers, overpass
from .model import Place, Item, PlaceItem, ItemCandidate, User, Category
from .wikipedia import page_category_iter
from .taginfo import get_taginfo
from .match import check_for_match
from social.apps.flask_app.routes import social_auth

import requests
import os.path

app = Flask(__name__)
app.register_blueprint(social_auth)
login_manager = LoginManager(app)
login_manager.login_view = 'login'

cat_to_ending = None

navbar_pages = [
    {'name': 'criteria_page', 'label': 'Criteria'},
    {'name': 'saved_places', 'label': 'Saved'},
    {'name': 'documentation', 'label': 'Documentation'},
]

tab_pages = [
    {'route': 'candidates', 'label': 'Match candidates'},
    {'route': 'already_tagged', 'label': 'Already tagged'},
    {'route': 'no_match', 'label': 'No match'},
    {'route': 'wikidata_page', 'label': 'Wikidata query'},
    {'route': 'overpass_query', 'label': 'Overpass query'},
]

extra_keys = {
    'Q1021290': 'Tag:amenity=college'  # music school
}

@app.context_processor
def filter_urls():
    name_filter = g.get('filter')
    try:
        if name_filter:
            url = url_for('saved_with_filter', name_filter=name_filter.replace(' ', '_'))
        else:
            url = url_for('saved_places')
    except RuntimeError:
        return {}  # maybe we don't care
    return dict(url_for_saved=url)

@app.before_request
def global_user():
    g.user = current_user

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(user_id)

@app.context_processor
def navbar():
    try:
        return dict(navbar_pages=navbar_pages, active=request.endpoint)
    except RuntimeError:
        return {}  # maybe we don't care

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('you are logged out')
    return redirect(url_for('index'))

@app.route('/done/')
def done():
    flash('login successful')
    return redirect(url_for('index'))

@app.route('/add_wikidata_tag', methods=['POST'])
def add_wikidata_tag():
    wikidata_id = request.form['wikidata']
    flash('wikidata tag saved in OpenStreetMap')
    osm_id = request.form['osm_id']
    osm_type = request.form['osm_type']

    user = g.user._get_current_object()
    assert user.is_authenticated

    social_user = user.social_auth.one()
    osm_backend = social_user.get_backend_instance()
    auth = osm_backend.oauth_auth(social_user.access_token)

    base = 'https://api.openstreetmap.org/api/0.6'

    changeset = '''
<osm>
  <changeset>
    <tag k="created_by" v="https://osm.wikidata.link/"/>
    <tag k="comment" v="add wikidata tag"/>
  </changeset>
</osm>
'''

    r = osm_backend.request(base + '/changeset/create', method='PUT', data=changeset, auth=auth)
    changeset_id = r.text.strip()

    url = '{}/{}/{}'.format(base, osm_type, osm_id)
    r = requests.get(url, params=social_user.access_token)

    print(r.url)
    print(r.text)
    root = etree.fromstring(r.content)
    tag = etree.Element('tag', k='wikidata', v=wikidata_id)
    root[0].set('changeset', changeset_id)
    root[0].append(tag)

    element_data = etree.tostring(root).decode('utf-8')
    print(element_data)

    r = osm_backend.request(url, method='PUT', data=element_data, auth=auth)

    print(r.text)

    r = osm_backend.request(base + '/changeset/{}/close'.format(changeset_id),
                            method='PUT',
                            auth=auth)

    print(repr(r.text))

    return redirect(url_for('item_page', wikidata_id=wikidata_id[1:]))

@app.route('/overpass/<int:osm_id>', methods=['POST'])
def post_overpass(osm_id):
    place = Place.query.get(osm_id)
    place.save_overpass(request.data)
    place.state = 'overpass'
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
        r = requests.post(overpass_url, data=oql, headers=user_agent_headers())
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
        e.attrib['action'] = 'modify'
        tag = etree.Element('tag', k='wikidata', v=item.qid)
        e.append(tag)

    xml = etree.tostring(root, pretty_print=True)
    return Response(xml, mimetype='text/xml')

def redirect_to_matcher(osm_id):
    return redirect(url_for('matcher_progress', osm_id=osm_id))

@app.route('/filtered/<name_filter>/candidates/<int:osm_id>')
def candidates_with_filter(name_filter, osm_id):
    g.filter = name_filter.replace('_', ' ')
    return candidates(osm_id)

@app.route('/wikidata/<int:osm_id>')
def wikidata_page(osm_id):
    place = Place.query.get(osm_id)

    full_count = place.items_with_candidates_count()

    return render_template('wikidata_query.html',
                           place=place,
                           tab_pages=tab_pages,
                           osm_id=osm_id,
                           full_count=full_count)

@app.route('/overpass/<int:osm_id>')
def overpass_query(osm_id):
    place = Place.query.get(osm_id)

    full_count = place.items_with_candidates_count()

    return render_template('overpass.html',
                           place=place,
                           tab_pages=tab_pages,
                           osm_id=osm_id,
                           full_count=full_count)

def do_add_tags(place, table):
    user = g.user._get_current_object()
    assert user.is_authenticated

    social_user = user.social_auth.one()
    osm_backend = social_user.get_backend_instance()
    auth = osm_backend.oauth_auth(social_user.access_token)

    base = 'https://api.openstreetmap.org/api/0.6'

    changeset = '''
<osm>
  <changeset>
    <tag k="created_by" v="https://osm.wikidata.link/"/>
    <tag k="comment" v="{comment}"/>
  </changeset>
</osm>
'''.format(comment=request.form['comment'])

    r = osm_backend.request(base + '/changeset/create', method='PUT', data=changeset, auth=auth)
    changeset_id = r.text.strip()

    for item, osm in table:
        wikidata_id = 'Q{:d}'.format(item.item_id)
        url = '{}/{}/{}'.format(base, osm['osm_type'], osm['osm_id'])
        r = requests.get(url, params=social_user.access_token)
        if 'wikidata' in r.text:  # done already
            continue

        root = etree.fromstring(r.content)
        tag = etree.Element('tag', k='wikidata', v=wikidata_id)
        root[0].set('changeset', changeset_id)
        root[0].append(tag)

        element_data = etree.tostring(root).decode('utf-8')
        r = osm_backend.request(url, method='PUT', data=element_data, auth=auth)
        print(r.text)

    r = osm_backend.request(base + '/changeset/{}/close'.format(changeset_id),
                            method='PUT',
                            auth=auth)

    print(repr(r.text))

@app.route('/add_tags/<int:osm_id>', methods=['POST'])
def add_tags(osm_id):
    place = Place.query.get(osm_id)
    if not place:
        abort(404)

    include = request.form.getlist('include')
    items = Item.query.filter(Item.item_id.in_([i[1:] for i in include])).all()

    table = [(item, candidate)
             for item, candidate in matcher.filter_candidates_more(items)]

    if request.form.get('confirm') == 'yes':
        do_add_tags(place, table)
        return 'done'

    return render_template('add_tags.html',
                           place=place,
                           osm_id=osm_id,
                           table=table)

@app.route('/candidates/<int:osm_id>')
def candidates(osm_id):
    place = Place.query.get(osm_id)
    if not place:
        abort(404)
    multiple_only = bool(request.args.get('multiple'))

    if place.state != 'ready':
        return redirect_to_matcher(osm_id)

    if place.state == 'overpass_error':
        error = open(place.overpass_filename).read()
        return render_template('candidates.html',
                               overpass_error=error,
                               place=place)

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

    filtered = {item.item_id: candidate
                for item, candidate in matcher.filter_candidates_more(items)}

    return render_template('candidates.html',
                           place=place,
                           osm_id=osm_id,
                           tab_pages=tab_pages,
                           multiple_only=multiple_only,
                           filtered=filtered,
                           full_count=full_count,
                           multiple_match_count=multiple_match_count,
                           candidates=items)

@app.route('/no_match/<int:osm_id>')
def no_match(osm_id):
    place = Place.query.get(osm_id)

    if place.state != 'ready':
        return redirect_to_matcher(osm_id)

    if place.state == 'overpass_error':
        error = open(place.overpass_filename).read()
        return render_template('candidates.html',
                               overpass_error=error,
                               place=place)

    full_count = place.items_with_candidates_count()

    items_without_matches = place.items_without_candidates()

    return render_template('no_match.html',
                           place=place,
                           osm_id=osm_id,
                           tab_pages=tab_pages,
                           items_without_matches=items_without_matches,
                           full_count=full_count)

@app.route('/already_tagged/<int:osm_id>')
def already_tagged(osm_id):
    place = Place.query.get(osm_id)

    if place.state != 'ready':
        return redirect_to_matcher(osm_id)

    if place.state == 'overpass_error':
        error = open(place.overpass_filename).read()
        return render_template('candidates.html',
                               overpass_error=error,
                               place=place)

    items = [item for item in place.items_with_candidates()
             if any('wikidata' in c.tags for c in item.candidates)]

    return render_template('already_tagged.html',
                           place=place,
                           osm_id=osm_id,
                           tab_pages=tab_pages,
                           items=items)

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
        return jsonify(item_list=place.item_list())
    wbgetentities(place)
    place.state = 'wbgetentities'
    database.session.commit()
    return jsonify(item_list=place.item_list())

@app.route('/load/<int:osm_id>/check_overpass', methods=['POST'])
def check_overpass(osm_id):
    place = Place.query.get(osm_id)
    reply = 'got' if place.overpass_done else 'get'
    return Response(reply, mimetype='text/plain')

@app.route('/load/<int:osm_id>/overpass_timeout', methods=['POST'])
def overpass_timeout(osm_id):
    place = Place.query.get(osm_id)
    place.state = 'overpass_timeout'
    database.session.commit()
    return Response('timeout noted', mimetype='text/plain')

@app.route('/load/<int:osm_id>/osm2pgsql', methods=['POST', 'GET'])
def load_osm2pgsql(osm_id):
    place = Place.query.get(osm_id)
    expect = [place.prefix + '_' + t for t in ('line', 'point', 'polygon')]
    tables = database.get_tables()
    if not all(t in tables for t in expect):
        error = place.load_into_pgsql()
        if error:
            return Response(error, mimetype='text/plain')
    place.state = 'osm2pgsql'
    database.session.commit()
    return Response('done', mimetype='text/plain')

@app.route('/load/<int:osm_id>/match/Q<int:item_id>', methods=['POST', 'GET'])
def load_individual_match(osm_id, item_id):
    global cat_to_ending

    place = Place.query.get(osm_id)

    conn = database.session.bind.raw_connection()
    cur = conn.cursor()

    if cat_to_ending is None:
        cat_to_ending = matcher.build_cat_to_ending()

    item = Item.query.get(item_id)
    candidates = matcher.find_item_matches(cur, item, cat_to_ending, place.prefix)
    for i in (candidates or []):
        c = ItemCandidate.query.get((item.item_id, i['osm_id'], i['osm_type']))
        if not c:
            c = ItemCandidate(**i, item=item)
            database.session.add(c)
    database.session.commit()

    conn.close()
    return Response('done', mimetype='text/plain')

@app.route('/load/<int:osm_id>/ready', methods=['POST', 'GET'])
def load_ready(osm_id):
    place = Place.query.get(osm_id)
    place.state = 'ready'
    place.item_count = place.items.count()
    place.candidate_count = place.items_with_candidates_count()
    database.session.commit()
    return Response('done', mimetype='text/plain')

@app.route('/load/<int:osm_id>/match', methods=['POST', 'GET'])
def load_match(osm_id):
    place = Place.query.get(osm_id)

    conn = database.session.bind.raw_connection()
    cur = conn.cursor()

    cat_to_ending = matcher.build_cat_to_ending()

    q = place.items.filter(Item.entity.isnot(None)).order_by(Item.item_id)
    for item in q:
        candidates = matcher.find_item_matches(cur, item, cat_to_ending, place.prefix)
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
    sort = request.args.get('sort') or 'name'
    name_filter = g.get('filter')

    q = Place.query.filter(Place.state.isnot(None))
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

def get_top_existing():
    q = (Place.query.filter(Place.state == 'ready')
                    .order_by((Place.item_count / Place.area).desc()))
    return q

def sort_link(order):
    args = request.view_args.copy()
    args['sort'] = order
    return url_for(request.endpoint, **args)

@app.route("/search")
def search_results():
    q = request.args.get('q')
    results = nominatim.lookup(q)
    for hit in results:
        p = Place.from_nominatim(hit)
        if p:
            database.session.merge(p)
    database.session.commit()

    for hit in results:
        if hit.get('osm_type') == 'relation':
            hit['place'] = Place.query.get(hit['osm_id'])

    return render_template('results_page.html', results=results, q=q)

@app.route("/")
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

    return render_template('index.html', existing=get_top_existing())


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

@app.route('/saved')
def saved_places():
    if 'filter' in request.args:
        arg_filter = request.args['filter'].strip().replace(' ', '_')
        if arg_filter:
            return redirect(url_for('saved_with_filter', name_filter=arg_filter))
        else:
            return redirect(url_for('saved_places'))

    return render_template('saved.html',
                           existing=get_existing(),
                           sort_link=sort_link)

@app.route("/documentation")
def documentation():
    return redirect('https://github.com/EdwardBetts/osm-wikidata/blob/master/README.md')

def get_isa(entity):
    return [isa['mainsnak']['datavalue']['value']['id']
            for isa in entity['claims'].get('P31', [])]

def get_radius(default=1000):
    arg_radius = request.args.get('radius')
    return int(arg_radius) if arg_radius and arg_radius.isdigit() else default

def get_entity_coords(entity):
    coords = entity['claims']['P625'][0]['mainsnak']['datavalue']['value']
    return coords['latitude'], coords['longitude']

def get_entity_oql(entity, criteria, radius=None):
    if radius is None:
        radius = get_radius()
    lat, lon = get_entity_coords(entity)

    osm_filter = 'around:{},{},{}'.format(radius, lat, lon)

    union = []
    for tag_or_key in criteria:
        union += overpass.oql_from_wikidata_tag_or_key(tag_or_key, osm_filter)

    oql = ('[timeout:300][out:json];\n' +
           '({}\n);\n' +
           'out qt center tags;').format(''.join(union))

    return oql

@app.route('/api/1/item/Q<int:wikidata_id>')
def api_item_match(wikidata_id):
    radius = get_radius()
    qid = 'Q' + str(wikidata_id)
    entity = wikidata.get_entity(qid)
    wikidata_names = dict(wikidata.names_from_entity(entity))
    wikidata_query = wikidata.osm_key_query(qid)

    criteria = {row['tag']['value'] for row in wikidata.get_osm_keys(wikidata_query)}
    criteria |= {extra_keys[isa] for isa in get_isa(entity) if isa in extra_keys}

    oql = get_entity_oql(entity, criteria, radius=radius)

    existing = overpass.get_existing(qid)

    overpass_reply = overpass.item_query(oql, qid, radius, refresh=True)

    found = [element for element in overpass_reply
             if check_for_match(element['tags'], wikidata_names)]

    lat, lon = get_entity_coords(entity)

    return jsonify({
        'item_id': qid,
        'radius': radius,
        'matches': found,
        'lat': lat,
        'lon': lon,
        'criteria': sorted(criteria),
        'found_matches': bool(found),
        'existing': existing,
    })


@app.route('/Q<int:wikidata_id>', methods=['GET', 'POST'])
def item_page(wikidata_id):
    radius = get_radius()
    qid = 'Q' + str(wikidata_id)
    filename = overpass.item_filename(qid, radius)
    if request.method == 'POST':
        if os.path.exists(filename):
            os.remove(filename)
        return redirect(url_for(request.endpoint, **request.view_args, **request.args))

    entity = wikidata.get_entity(qid)
    labels = entity['labels']
    wikidata_names = dict(wikidata.names_from_entity(entity))

    if 'en' in labels:
        label = labels['en']['value']
    else:
        label = list(labels.values())[0]['value']

    lat, lon = get_entity_coords(entity)

    wikidata_query = wikidata.osm_key_query(qid)
    osm_keys = wikidata.get_osm_keys(wikidata_query)

    extra = {extra_keys[isa] for isa in get_isa(entity) if isa in extra_keys}

    if not osm_keys and not extra:

        return render_template('item_page.html',
                               entity=entity,
                               wikidata_names=wikidata_names,
                               wikidata_query=wikidata_query,
                               qid=qid,
                               lat=lat,
                               lon=lon,
                               osm_keys=osm_keys,
                               label=label,
                               labels=labels)

    osm_filter = 'around:{},{},{}'.format(radius, lat, lon)

    union = []
    for row in osm_keys:
        # if row['item']['value'] == 'http://www.wikidata.org/entity/Q41176':
        #     continue  # building
        tag_or_key = row['tag']['value']
        union += overpass.oql_from_wikidata_tag_or_key(tag_or_key, osm_filter)

    for tag_or_key in extra:
        union += overpass.oql_from_wikidata_tag_or_key(tag_or_key, osm_filter)

    oql = ('[timeout:300][out:json];\n' +
           '({}\n);\n' +
           'out qt tags;').format(''.join(union))

    # FIXME extend oql to also check is_in
    # like this:
    #
    # is_in(48.856089,2.29789);
    # area._[admin_level];
    # out tags;

    overpass_reply = overpass.item_query(oql, qid, radius)

    found = []
    for element in overpass_reply:
        m = check_for_match(element['tags'], wikidata_names)
        if m:
            found.append((element, m))

    return render_template('item_page.html',
                           entity=entity,
                           wikidata_names=wikidata_names,
                           wikidata_query=wikidata_query,
                           overpass_reply=overpass_reply,
                           oql=oql,
                           qid=qid,
                           lat=lat,
                           lon=lon,
                           found=found,
                           osm_keys=osm_keys,
                           label=label,
                           labels=labels)
