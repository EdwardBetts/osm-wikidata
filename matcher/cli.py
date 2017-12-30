from flask import render_template
from .view import app, get_top_existing, get_existing
from .model import Item, Changeset, get_bad
from .place import Place
from . import database, mail, matcher, nominatim, utils, netstring
from datetime import datetime, timedelta
from tabulate import tabulate
from sqlalchemy import inspect, func
from time import time, sleep
from pprint import pprint
from sqlalchemy.types import Enum
from sqlalchemy.schema import CreateTable, CreateIndex
from sqlalchemy.dialects.postgresql.base import CreateEnumType
import json
import click
import socket

def get_place(place_identifier):
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
        print(row)

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
    print(tabulate(rows,
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

    print(tabulate(places,
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
        print(d)

@app.cli.command()
@click.argument('place_identifier')
def place(place_identifier):
    app.config.from_object('config.default')
    database.init_app(app)

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
        print('{:{}s}  {}'.format(f + ':', max_field_len + 1, getattr(place, f)))

    print()
    print('filtered:', len(filtered))

    print('{:1f}'.format(time() - t0))

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
            print(len(filtered), p.display_name, '(updated)')
        else:
            print(len(filtered), p.display_name)

@app.cli.command()
@click.argument('q')
def nominatim_lookup(q):
    app.config.from_object('config.default')  # need the admin email address
    # result = nominatim.lookup_with_params(q=q, polygon_text=0)
    result = nominatim.lookup_with_params(q=q)
    print(json.dumps(result, indent=2))

@app.cli.command()
def refresh_address():
    app.config.from_object('config.default')  # need the admin email address
    database.init_app(app)

    for place in Place.query.filter(Place.state == 'ready'):
        if isinstance(place.address, list):
            continue

        if not place.address.get('country'):
            print('country missing:', place.display_name)
            continue

        print(place.place_id, place.display_name)
        continue
        print('http://nominatim.openstreetmap.org/details.php?osmtype={}&osmid={}'.format(place.osm_type[0].upper(), place.osm_id))
        first_parts = place.display_name.split(', ', 1)[:-1]
        print(place.address)
        # q = ', '.join(first_parts + [place.address['country']])
        q = ', '.join(first_parts)
        print(q)
        # print()

        try:
            results = nominatim.lookup(q=q)
        except nominatim.SearchError as e:
            print(e.text)
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
                print('not found: {hit[place_id]}  {hit[display_name]}'.format(hit=hit))
                continue
            place.update_from_nominatim(hit)

            print(hit['place_id'], list(hit['address'].items()))
        database.session.commit()

        print()
        sleep(10)

@app.cli.command()
@click.argument('place_identifier')
def run_matcher(place_identifier):
    app.config.from_object('config.default')
    database.init_app(app)

    print(place_identifier)
    place = get_place(place_identifier)

    print(place.display_name)
    print(place.state)

    print('do match')
    place.do_match()
    print(place.state, place.display_name)
    print('https://osm.wikidata.link/candidates/{place.osm_type}/{place.osm_id}'.format(place=place))

@app.cli.command()
@click.argument('place_identifier')
@click.argument('chunk_count')
def show_chunks(place_identifier, chunk_count):
    app.config.from_object('config.default')
    database.init_app(app)
    chunk_count = int(chunk_count)

    print(place_identifier)
    place = get_place(place_identifier)

    pprint(place.chunk_n(chunk_count))

    if chunk_count == 2:
        pprint([i['bbox'] for i in place.chunk4()])
    if chunk_count == 3:
        pprint([i['bbox'] for i in place.chunk9()])

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
    app.config.from_object('config.default')
    database.init_app(app)

    print(place_identifier)
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
    app.config.from_object('config.default')
    database.init_app(app)

    print(place_identifier)
    place = get_place(place_identifier)

    print(place.srid)

@app.cli.command()
@click.argument('place_identifier')
def add_to_queue(place_identifier):
    app.config.from_object('config.default')
    database.init_app(app)

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
    app.config.from_object('config.default')
    database.init_app(app)

    place = get_place(place_identifier)

    items = place.items_from_wikidata()

    print(len(items))

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
@click.argument('place_identifier')
@click.argument('qid')
def find_item_matches(place_identifier, qid):
    app.config.from_object('config.default')
    database.init_app(app)

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
    app.config.from_object('config.default')
    database.init_app(app)

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
        print(item.qid, item.label())
        for k, v in sitelinks.items():
            print('  ', (k, v['title']))
