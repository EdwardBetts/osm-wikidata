from flask import render_template
from .view import app, get_top_existing, get_existing
from .model import Item, Changeset, get_bad, Base, ItemCandidate, Language, LanguageLabel, PlaceItem, OsmCandidate
from .place import Place
from . import database, mail, matcher, nominatim, utils, netstring, wikidata
from social.apps.flask_app.default.models import UserSocialAuth, Nonce, Association
from datetime import datetime, timedelta
from tabulate import tabulate
from sqlalchemy import inspect, func, cast
from geoalchemy2 import Geometry, Geography
from time import time, sleep
from pprint import pprint
from sqlalchemy.types import Enum
from sqlalchemy.schema import CreateTable, CreateIndex
from sqlalchemy.dialects.postgresql.base import CreateEnumType
import math
import os.path
import re
import json
import click
import socket

@app.cli.command()
def create_db():
    app.config.from_object('config.default')
    database.init_app(app)

    Base.metadata.create_all(database.session.get_bind())

def get_place(place_identifier):
    app.config.from_object('config.default')
    database.init_app(app)

    if place_identifier.isdigit():
        return Place.query.get(place_identifier)
    else:
        osm_type, osm_id = place_identifier.split('/')
        return Place.get_by_osm(osm_type, osm_id)

@app.cli.command()
def mail_recent():
    app.config.from_object('config.default')
    database.init_app(app)

    app.config['SERVER_NAME'] = 'osm.wikidata.link'
    ctx = app.test_request_context()
    ctx.push()  # to make url_for work

    # this works if run from cron once per hour
    # better to report new items since last run
    since = datetime.now() - timedelta(hours=1)

    q = (Changeset.query.filter(Changeset.update_count > 0,
                                Changeset.created > since)
                        .order_by(Changeset.id.desc()))

    template = '''
user: {change.user.username}
name: {name}
page: {url}
items: {change.update_count}
comment: {change.comment}

https://www.openstreetmap.org/changeset/{change.id}
'''

    total_items = 0
    body = ''
    for change in q:
        place = change.place
        if not place:
            continue
        url = place.candidates_url(_external=True, _scheme='https')
        body += template.format(name=place.display_name, url=url, change=change)
        total_items += change.update_count

    if total_items > 0:
        subject = 'tags added: {} changesets / {} objects'
        mail.send_mail(subject.format(q.count(), total_items), body)

    ctx.pop()

@app.cli.command()
def show_big_tables():
    app.config.from_object('config.default')
    database.init_app(app)
    for row in database.get_big_table_list():
        click.echo(row)

@app.cli.command()
def recent():
    app.config.from_object('config.default')
    database.init_app(app)
    q = (Changeset.query.filter(Changeset.update_count > 0)
                        .order_by(Changeset.id.desc()))

    rows = [(obj.created.strftime('%F %T'),
             obj.user.username,
             obj.update_count,
             obj.place.name_for_changeset) for obj in q.limit(25)]
    click.echo(tabulate(rows,
                        headers=['when', 'who', '#', 'where'],
                        tablefmt='simple'))

@app.cli.command()
def top():
    app.config.from_object('config.default')
    database.init_app(app)
    top_places = get_top_existing()
    headers = ['id', 'name', 'candidates', 'items', 'changesets']

    places = []
    for p, changeset_count in top_places:
        name = p.display_name
        if len(name) > 60:
            name = name[:56] + ' ...'
        places.append((p.place_id,
                       name,
                       p.candidate_count,
                       p.item_count,
                       changeset_count))

    click.echo(tabulate(places,
                        headers=headers,
                        tablefmt='simple'))

def object_as_dict(obj):
    return {c.key: getattr(obj, c.key) for c in inspect(obj).mapper.column_attrs}

@app.cli.command()
def dump():
    app.config.from_object('config.default')
    database.init_app(app)

    place_ids = [int(line[:-1]) for line in open('top_place_ids')]

    q = Place.query.filter(Place.place_id.in_(place_ids)).add_columns(func.ST_AsText(Place.geom))

    for place, geom in q:
        d = object_as_dict(place)
        d['geom'] = geom
        click.echo(d)

@app.cli.command()
@click.argument('place_identifier')
def place(place_identifier):
    place = get_place(place_identifier)

    fields = ['place_id', 'osm_type', 'osm_id', 'display_name',
              'category', 'type', 'place_rank', 'icon', 'south', 'west',
              'north', 'east', 'extratags',
              'item_count', 'candidate_count', 'state', 'override_name',
              'lat', 'lon', 'added']

    t0 = time()
    items = place.items_with_candidates()
    items = [item for item in items
             if all('wikidata' not in c.tags for c in item.candidates)]

    filtered = {item.item_id: match
                for item, match in matcher.filter_candidates_more(items, bad=get_bad(items))}

    max_field_len = max(len(f) for f in fields)

    for f in fields:
        click.echo('{:{}s}  {}'.format(f + ':', max_field_len + 1, getattr(place, f)))

    click.echo()
    click.echo('filtered:', len(filtered))

    click.echo('{:1f}'.format(time() - t0))

@app.cli.command()
def mark_as_complete():
    app.config.from_object('config.default')
    database.init_app(app)

    q = (Place.query.join(Changeset)
                    .filter(Place.state == 'ready', Place.candidate_count > 4)
                    .order_by((Place.item_count / Place.area).desc())
                    .limit(100))

    for p in q:
        items = p.items_with_candidates()
        items = [item for item in items
                 if all('wikidata' not in c.tags for c in item.candidates)]

        filtered = {item.item_id: match
                    for item, match in matcher.filter_candidates_more(items, bad=get_bad(items))}

        if len(filtered) == 0:
            p.state = 'complete'
            database.session.commit()
            click.echo(len(filtered), p.display_name, '(updated)')
        else:
            click.echo(len(filtered), p.display_name)

@app.cli.command()
@click.argument('q')
def nominatim_lookup(q):
    app.config.from_object('config.default')  # need the admin email address
    # result = nominatim.lookup_with_params(q=q, polygon_text=0)
    result = nominatim.lookup_with_params(q=q)
    click.echo(json.dumps(result, indent=2))

@app.cli.command()
def refresh_address():
    app.config.from_object('config.default')  # need the admin email address
    database.init_app(app)

    for place in Place.query.filter(Place.state == 'ready'):
        if isinstance(place.address, list):
            continue

        if not place.address.get('country'):
            click.echo('country missing:', place.display_name)
            continue

        click.echo(place.place_id, place.display_name)
        continue
        click.echo('http://nominatim.openstreetmap.org/details.php?osmtype={}&osmid={}'.format(place.osm_type[0].upper(), place.osm_id))
        first_parts = place.display_name.split(', ', 1)[:-1]
        click.echo(place.address)
        # q = ', '.join(first_parts + [place.address['country']])
        q = ', '.join(first_parts)
        click.echo(q)
        # print()

        try:
            results = nominatim.lookup(q=q)
        except nominatim.SearchError as e:
            click.echo(e.text)
            raise
        for hit in results:
            place_id = hit['place_id']
            place = Place.query.get(place_id)
            if 'osm_type' not in hit or 'osm_id' not in hit:
                continue

            if not place:
                place = (Place.query
                              .filter_by(osm_type=hit['osm_type'], osm_id=hit['osm_id'])
                              .one_or_none())
            if not place:
                click.echo('not found: {hit[place_id]}  {hit[display_name]}'.format(hit=hit))
                continue
            place.update_from_nominatim(hit)

            click.echo(hit['place_id'], list(hit['address'].items()))
        database.session.commit()

        click.echo()
        sleep(10)

@app.cli.command()
@click.argument('place_identifier')
def run_matcher(place_identifier):
    place = get_place(place_identifier)

    click.echo(place.display_name)
    click.echo(place.state)

    click.echo('do match')
    place.do_match()
    click.echo(place.state, place.display_name)
    click.echo('https://osm.wikidata.link/candidates/{place.osm_type}/{place.osm_id}'.format(place=place))

@app.cli.command()
@click.argument('place_identifier')
@click.argument('qid')
def individual_match(place_identifier, qid):
    app.config.from_object('config.default')
    database.init_app(app)

    if place_identifier.isdigit():
        place = Place.query.get(place_identifier)
    else:
        osm_type, osm_id = place_identifier.split('/')
        place = Place.query.filter_by(osm_type=osm_type, osm_id=osm_id).one()

    item = Item.get_by_qid(qid)
    # entity = wikidata.WikidataItem(qid, item.entity)

    candidates = matcher.run_individual_match(place, item)
    pprint(candidates)

@app.cli.command()
@click.argument('since')
def match_since(since):
    app.config.from_object('config.default')
    database.init_app(app)

    q = Place.query.filter(~Place.state.is_(None), Place.added > since)
    print(q.count(), 'places')
    for place in q:
        print(place.state, place.display_name)
        place.do_match()
        print(place.state, place.display_name)
        print('https://osm.wikidata.link/candidates/{place.osm_type}/{place.osm_id}'.format(place=place))
        print()

@app.cli.command()
def place_page():
    app.config.from_object('config.default')
    database.init_app(app)

    with app.test_request_context('/'):
        sort = 'name'
        t0 = time()
        existing = get_existing(sort, None)
        tbody = render_template('place_tbody.html', existing=existing)
        seconds = time() - t0
        print('took: {:.0f} seconds'.format(seconds))
        # open('place_tbody.html', 'w').write(tbody)

@app.cli.command()
@click.argument('place_identifier')
def polygons(place_identifier):
    place = get_place(place_identifier)

    chunk_size = utils.calc_chunk_size(place.area_in_sq_km)
    place_geojson = (database.session.query(func.ST_AsGeoJSON(Place.geom, 4))
                                     .filter(Place.place_id == place.place_id)
                                     .scalar())
    # print(place_geojson)
    for chunk in place.chunk_n(chunk_size):
        print(', '.join('{:.3f}'.format(i) for i in chunk))

        (ymin, ymax, xmin, xmax) = chunk

        clip = func.ST_Intersection(Place.geom,
                                    func.ST_MakeEnvelope(xmin, ymin, xmax, ymax))

        chunk_geojson = (database.session
                                 .query(func.ST_AsGeoJSON(clip, 4))
                                 .filter(Place.place_id == place.place_id)
                                 .scalar())

        print(chunk_geojson)

@app.cli.command()
@click.argument('place_identifier')
def srid(place_identifier):
    click.echo(get_place(place_identifier).srid)

@app.cli.command()
@click.argument('place_identifier')
def add_to_queue(place_identifier):
    place = get_place(place_identifier)

    host, port = 'localhost', 6020
    sock = socket.create_connection((host, port))
    sock.setblocking(True)

    chunks = place.get_chunks()

    fields = ['place_id', 'osm_id', 'osm_type']
    msg = {
        'place': {f: getattr(place, f) for f in fields},
        'chunks': chunks,
    }

    netstring.write(sock, json.dumps(msg))
    reply = netstring.read(sock)
    print(reply)
    while True:
        from_network = netstring.read(sock)
        print('from network:', from_network)
        if from_network is None:
            break
        netstring.write(sock, 'ack')
    print('socket closed')

@app.cli.command()
def queue_sample_items():

    host, port = 'localhost', 6020
    sock = socket.create_connection((host, port))
    sock.setblocking(True)

    chunks = list(range(5))
    msg = {'place': {}, 'chunks': chunks, 'sample': True}

    netstring.write(sock, json.dumps(msg))

@app.cli.command()
@click.argument('place_identifier')
def get_items_from_wikidata(place_identifier):
    place = get_place(place_identifier)

    items = place.items_from_wikidata()

    click.echo(len(items))

def get_class(class_name):
    return globals()[class_name]

@app.cli.command()
@click.argument('tables', nargs=-1)
def print_create_table(tables):
    app.config.from_object('config.default')
    database.init_app(app)

    engine = database.session.get_bind()

    for class_name in tables:
        cls = get_class(class_name)

        for c in cls.__table__.columns:
            if not isinstance(c.type, Enum):
                continue
            t = c.type
            sql = str(CreateEnumType(t).compile(engine))
            click.echo(sql.strip() + ';')

        for index in cls.__table__.indexes:
            sql = str(CreateIndex(index).compile(engine))
            click.echo(sql.strip() + ';')

        sql = str(CreateTable(cls.__table__).compile(engine))
        click.echo(sql.strip() + ';')

@app.cli.command()
@click.argument('qid')
def hstore_query(qid):
    app.config.from_object('config.default')
    database.init_app(app)
    print(Item.query.get(int(qid[1:])).hstore_query())

@app.cli.command()
@click.argument('place_identifier')
@click.argument('qid')
def find_item_matches(place_identifier, qid):
    place = get_place(place_identifier)
    print(place.name_for_changeset)

    conn = database.session.bind.raw_connection()
    cur = conn.cursor()
    item_id = int(qid[1:])
    item = Item.query.get(item_id)
    item.refresh_extract_names()
    database.session.commit()
    print('label:', item.label())
    print('tags:', item.tags)
    print('extra:', item.get_extra_tags())
    print('hstore:', item.hstore_query())
    for k, v in item.names().items():
        print((k, v))
    print('NRHP:', item.ref_nrhp())
    candidates = matcher.find_item_matches(cur, item, place.prefix, debug=False)
    print('candidate count:', len(candidates))

    for c in candidates:
        pprint(c)
        print()

@app.cli.command()
@click.argument('place_identifier')
def area(place_identifier):
    place = get_place(place_identifier)
    print(place.name_for_changeset)
    print('{:,.0f} km²'.format(place.area_in_sq_km))

    chunk_size = utils.calc_chunk_size(place.area_in_sq_km, size=64)
    print(chunk_size)

    bbox_chunks = place.chunk_n(chunk_size)
    for num, chunk in enumerate(bbox_chunks):
        print(num, chunk)

@app.cli.command()
def find_ceb():
    app.config.from_object('config.default')
    database.init_app(app)

    q = Item.query.filter(Item.entity.isnot(None))
    for item in q:
        sitelinks = item.sitelinks()
        if not sitelinks or 'cebwiki' not in sitelinks or 'enwiki' in sitelinks:
            continue
        click.echo(item.qid, item.label())
        for k, v in sitelinks.items():
            click.echo('  ', (k, v['title']))

@app.cli.command()
@click.argument('place_identifier')
def place_oql(place_identifier):
    print(get_place(place_identifier).get_oql())

@app.cli.command()
@click.argument('place_identifier')
def latest_matcher_run(place_identifier):
    place = get_place(place_identifier)
    print(place.latest_matcher_run().start)

@app.cli.command()
def add_place_wikidata():
    app.config.from_object('config.default')
    database.init_app(app)

    need_commit = False
    for place in Place.query:
        qid = place.extratags.get('wikidata')
        if not qid:
            continue
        need_commit = True
        print(qid, place.display_name)
        place.wikidata = qid

    if need_commit:
        database.session.commit()

@app.cli.command()
@click.argument('qid')
def next_level(qid):
    app.config.from_object('config.default')
    entity = wikidata.get_entity(qid)
    rows = wikidata.next_level_places(qid, entity)
    isa = {i['mainsnak']['datavalue']['value']['id']
           for i in entity.get('claims', {}).get('P31', [])}

    print(qid, entity['labels']['en']['value'], isa)

    for row in rows:
        print(row)

@app.cli.command()
def geojson_chunks():
    app.config.from_object('config.default')
    database.init_app(app)

    q = Place.query.filter(Place.area > (1000 * 1000 * 1000),
                           Place.area < (1000 * 1000 * 5000))

    empty_json = '{"type":"GeometryCollection","geometries":[]}'

    for place in q:
        chunk_size = utils.calc_chunk_size(place.area_in_sq_km)
        geo = place.geojson_chunks()
        empty_count = geo.count(empty_json)
        if empty_count == 0:
            continue

        print(f'{chunk_size ** 2:3d}  {empty_count:3d}  ' +
              f'{place.area_in_sq_km:>10.0f}  ' +
              f'{place.osm_type}/{place.osm_id} ' +
              f'{place.display_name}')
        continue
        for chunk in place.geojson_chunks():
            print(len(chunk), chunk[:100])

@app.cli.command()
@click.argument('place_identifier')
def place_chunks(place_identifier):
    place = get_place(place_identifier)

    print(f'{place.chunk_count():3d}  ' +
          f'{place.area_in_sq_km:>10.0f}  ' +
          f'{place.osm_type}/{place.osm_id} ' +
          f'{place.display_name}')

    for chunk in place.geojson_chunks():
        print(len(chunk), chunk[:100])

@app.cli.command()
def nominatim_refresh():
    app.config.from_object('config.default')
    database.init_app(app)

    q = Place.query.filter_by(state='ready')
    for place in q:
        print(place.display_name)
        place.refresh_nominatim()
        sleep(10)

@app.cli.command()
def hide_top_places_from_index():
    app.config.from_object('config.default')
    database.init_app(app)

    top_places = get_top_existing()
    for p in top_places:
        print((p.osm_type, p.osm_id, p.display_name))
        p.index_hide = True

    database.session.commit()

def wikidata_chunk_size(area):
    return 1 if area < 10000 else utils.calc_chunk_size(area, size=32)

def chunk_n(bbox, n):
    n = max(1, n)
    (south, north, west, east) = bbox
    ns = (north - south) / n
    ew = (east - west) / n

    chunks = []
    for row in range(n):
        for col in range(n):
            chunk = (south + ns * row, south + ns * (row + 1),
                    west + ew * col, west + ew * (col + 1))
            chunks.append(chunk)
    return chunks


@app.cli.command()
@click.argument('place_identifier')
def show_polygons(place_identifier):
    place = get_place(place_identifier)
    num = 0
    for chunk in place.polygon_chunk(size=64):
        num += 1
        print(chunk)

    print()
    print(num)

    return
    num = '(-?[0-9.]+)'
    re_box = re.compile(f'^BOX\({num} {num},{num} {num}\)$')

    # select ST_Dump(geom::geometry) as poly from place where osm_id=1543125
    stmt = (database.session.query(func.ST_Dump(Place.geom.cast(Geometry())).label('x'))
                            .filter_by(place_id=place.place_id)
                            .subquery())

    q = database.session.query(stmt.c.x.path[1],
                               func.ST_Area(stmt.c.x.geom.cast(Geography)) / (1000 * 1000),
                               func.Box2D(stmt.c.x.geom))
    print(q)

    for num, area, box2d in q:
        # west, south, east, noth
        # BOX(135.8536855 20.2145811,136.3224209 20.6291059)

        size = wikidata_chunk_size(area)
        west, south, east, north = map(float, re_box.match(box2d).groups())
        bbox = (south, north, west, east)

        # print((num, area, size, box2d))

        for chunk in chunk_n(bbox, size):
            print(chunk)

@app.cli.command()
def operator():
    app.config.from_object('config.default')
    database.init_app(app)

    q = ItemCandidate.query.filter(ItemCandidate.tags['operator'] != None)

    for c in q:
        tags = c.tags
        print((c.osm_type, c.osm_id))
        pprint(tags)
        print()

@app.cli.command()
def load_languages():
    app.config.from_object('config.default')
    database.init_app(app)

    filename = 'data/languages.json'

    if not os.path.exists(filename):

        query = '''
    SELECT ?lang WHERE {
      ?lang wdt:P424 ?code .
      ?lang wdt:P218 ?iso .
    }'''

        ids = [wikidata.wd_uri_to_qid(row['lang']['value'])
               for row in wikidata.run_query(query)]

        print(len(ids), 'languages')

        out = open('languages', 'w')
        for qid, entity in wikidata.entity_iter(ids):
            # item_id = int(qid[1:])
            print((qid, entity['labels'].get('en')))
            print((qid, entity), file=out)
        out.close()

    property_map = {
        'P218': 'iso_639_1',
        'P219': 'iso_639_2',
        'P220': 'iso_639_3',
        'P424': 'wikimedia_language_code',
    }

    known_lang = set()
    for line in open(filename):
        qid, entity = eval(line)
        if qid == 'Q9129':
            continue
        item_id = int(qid[1:])
        claims = entity['claims']
        print(entity['labels']['en']['value'])

        item = Language(item_id=item_id)

        for property_key, field in property_map.items():
            values = claims.get(property_key)
            if not values:
                continue
            # print(field, len(values))
            # French ISO 639-2 codes: fre and fra
            if len(values) != 1:
                continue
            v = values[0]['mainsnak']['datavalue']['value']
            setattr(item, field, v)
        known_lang.add(item.wikimedia_language_code)
        # database.session.add(item)
    # database.session.commit()

    print()
    for line in open(filename):
        qid, entity = eval(line)
        if qid == 'Q9129':
            continue
        print(entity['labels']['en']['value'])
        item_id = int(qid[1:])
        for k, v in entity['labels'].items():
            if k not in known_lang:
                continue
            label = LanguageLabel(item_id=item_id,
                                  wikimedia_language_code=k,
                                  label=v['value'])
            database.session.add(label)

        database.session.commit()

@app.cli.command()
@click.argument('place_identifier')
def place_languages(place_identifier):
    place = get_place(place_identifier)

    for lang in place.languages():
        print(lang)

@app.cli.command()
def populate_osm_candidate_table():
    app.config.from_object('config.default')
    database.init_app(app)
    total = ItemCandidate.query.count()
    print(f'total: {total}')
    for num, ic in enumerate(ItemCandidate.query):
        print(f'{num}/{total} {num/total:.2%}  {ic.name}')

        fields = ['osm_id', 'osm_type', 'name', 'tags']
        c = OsmCandidate(**{k: getattr(ic, k) for k in fields})
        database.session.merge(c)
    database.session.commit()

@app.cli.command()
@click.argument('qid')
def get_isa(qid):
    pass

@app.cli.command()
def load_item_candidate_geom():
    app.config.from_object('config.default')
    database.init_app(app)
    tables = database.get_tables()

    conn = database.session.bind.raw_connection()
    cur = conn.cursor()

    q = ItemCandidate.query.filter(ItemCandidate.geom.is_(None))
    total = q.count()

    for num, c in enumerate(q):
        for place in c.item.places:
            table = place.prefix + '_' + c.planet_table
            if table not in tables:
                continue
            sql = f'select ST_AsText(ST_Transform(way, 4326)) from {table} where osm_id={c.src_id}'
            cur.execute(sql)
            row = cur.fetchone()
            if row is None:
                continue
            geom = row[0]
            if len(geom) > 40_000:
                continue
            print(f'{num}/{total} ({num/total:.2%}) ', table, c.src_id, len(geom))
            c.geom = geom
            break
        if num % 100 == 0:
            database.session.commit()

    database.session.commit()

@app.cli.command()
@click.argument('place_identifier')
def candidate_shapes(place_identifier):
    place = get_place(place_identifier)

    for item in place.items_with_candidates():
        for c in item.candidates:
            print(c.key, ' ', c.geojson)
