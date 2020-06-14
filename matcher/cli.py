from flask import render_template
from .view import app, get_existing
from .model import (Item, Changeset, get_bad, Base, ItemCandidate, Language,
                    LanguageLabel, OsmCandidate, Extract, ChangesetEdit,
                    EditMatchReject, BadMatchFilter)
from .place import Place
from .isa_facets import get_isa_facets
from . import (database, mail, matcher, nominatim, utils, chat, wikidata, osm_api,
               wikidata_api, browse)
from datetime import datetime, timedelta
from tabulate import tabulate
from sqlalchemy import inspect, func
from geoalchemy2 import Geometry, Geography
from time import time, sleep
from pprint import pprint
from sqlalchemy.types import Enum
from sqlalchemy.schema import CreateTable, CreateIndex
from sqlalchemy.dialects.postgresql.base import CreateEnumType
import unicodedata
import sqlalchemy.exc
import os.path
import re
import json
import click
import sys

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
        return Place.from_osm(osm_type, osm_id)

@app.cli.command()
def mail_recent():
    app.config.from_object('config.default')
    database.init_app(app)

    app.config['SERVER_NAME'] = 'osm.wikidata.link'
    ctx = app.test_request_context()
    ctx.push()  # to make url_for work

    # this works if run from cron once per hour
    # better to report new items since last run
    since = datetime.utcnow() - timedelta(hours=1)

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
    click.echo((place.state, place.display_name))
    click.echo('https://osm.wikidata.link/candidates/{place.osm_type}/{place.osm_id}'.format(place=place))

@app.cli.command()
@click.argument('place_identifier')
@click.option('--debug', is_flag=True)
def place_match(place_identifier, debug):
    place = get_place(place_identifier)
    place_items = place.matcher_query()
    total = place_items.count()
    print('total:', total)

    place.run_matcher(debug=debug)

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
def show_chunks(place_identifier):
    place = get_place(place_identifier)
    for chunk in place.get_chunks():
        oql = chunk['oql']
        if oql:
            print(oql)

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
    # print('isa:', [isa.label_and_qid() for isa in item.isa])
    if item.categories:
        print('categories:', item.categories)
        print('tags from categories:', matcher.categories_to_tags(item.categories))
    print('label:', item.label())
    print('tags:', item.tags)
    print('extra:', item.get_extra_tags())
    # print('hstore:', item.hstore_query())
    for k, v in item.names().items():
        print((k, v))
    print('NRHP:', item.ref_nrhp())
    candidates = matcher.find_item_matches(cur, item, place.prefix, debug=True)
    print('candidate count:', len(candidates))

    for c in candidates:
        del c['geom']
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
    isa_list = set()
    isa = {i['mainsnak']['datavalue']['value']['id']
           for i in entity.get('claims', {}).get('P31', [])}

    print(qid, entity['labels']['en']['value'], isa)

    for row in rows:
        isa_list.update(row['isa'])
        print(row)

    print()
    for row in wikidata.get_item_labels(isa_list):
        print(row)

@app.cli.command()
@click.argument('qid')
def next_level_query(qid):
    app.config.from_object('config.default')
    entity = wikidata.get_entity(qid)
    query = wikidata.get_next_level_query(qid, entity)

    print(query)

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
        # west, south, east, north
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

        out = open(filename, 'w')
        for qid, entity in wikidata_api.entity_iter(ids):
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
    print('creating language objects')
    for line in open(filename):
        qid, entity = eval(line)
        # Skip Greek
        if qid == 'Q9129':
            continue
        item_id = int(qid[1:])
        claims = entity['claims']
        en_label = entity['labels']['en']['value']
        print(en_label or ('no English label:', qid))

        item = Language(item_id=item_id)

        for property_key, field in property_map.items():
            values = claims.get(property_key)
            if not values:
                continue
            # print(field, len(values))
            # French ISO 639-2 codes: fre and fra
            if len(values) != 1:
                continue
            mainsnak = values[0]['mainsnak']
            v = mainsnak['datavalue']['value'] if 'datavalue' in mainsnak else None
            setattr(item, field, v)
        known_lang.add(item.wikimedia_language_code)
        database.session.add(item)
    database.session.commit()

    print()
    print('adding language labels')
    for line in open(filename):
        qid, entity = eval(line)
        # Skip Greek
        if qid == 'Q9129':
            continue
        en_label = entity['labels']['en']['value']
        print(en_label or ('no English label:', qid))
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

@app.cli.command()
@click.argument('place_identifier')
def suggest_larger_areas(place_identifier):
    top = get_place(place_identifier)

    for place in top.go_bigger():
        area_in_sq_km = place.area_in_sq_km
        print(f'{area_in_sq_km:>10.1f} sq km  {place.name}')

    return

    for e in reversed(top.is_in()):
        # pprint(e)
        osm_type, osm_id = e['type'], e['id']
        if osm_type == top.osm_type and osm_id == top.osm_id:
            continue

        level = e['tags'].get('admin_level')

        # {'minlat': 49, 'minlon': -14, 'maxlat': 61.061, 'maxlon': 2}

        box = func.ST_MakeEnvelope(e['bounds']['minlon'],
                                   e['bounds']['minlat'],
                                   e['bounds']['maxlon'],
                                   e['bounds']['maxlat'], 4326)

        bbox_area = database.session.query(func.ST_Area(box.cast(Geography))).scalar()
        area_in_sq_km = bbox_area / (1000 * 1000)

        if area_in_sq_km > 20_000:
            continue
        place = Place.from_osm(osm_type, osm_id)
        area_in_sq_km = place.area_in_sq_km
        print(f'{area_in_sq_km:>10.1f} sq km', e['type'], f"{e['id']:10d}", level, e['tags']['name'])

        continue

        print(f'{place.area_in_sq_km:>10.1f} sq km  {place.name:30s}')
        continue

        hit = nominatim.reverse(e['type'], e['id'], polygon_text=0)
        pprint(hit)
        print()
        sleep(2)

@app.cli.command()
@click.argument('qid')
def find_isa(qid):
    app.config.from_object('config.default')
    database.init_app(app)

    items = [qid]
    cache_name = 'isa_' + ','.join(items)
    try:
        result = wikidata.get_isa(items, name=cache_name)
    except wikidata.QueryError as e:
        print(e.args[0])
        sys.exit(0)

    print(result)

@app.cli.command()
@click.argument('place_identifier')
def show_place_item_isa(place_identifier):
    place = get_place(place_identifier)
    items = {item.qid: item for item in place.items_with_instanceof()}

    name = 'isa_' + place_identifier.replace('/', '_')

    try:
        isa = wikidata.get_isa(items.keys(), name=name)
    except wikidata.QueryError as e:
        print(e.args[0])
        sys.exit(0)

    for k, v in isa.items():
        print(k, items[k].label(), v)

@app.cli.command()
@click.argument('place_identifier')
def load_isa(place_identifier):
    place = get_place(place_identifier)

    def progress(msg):
        print(msg)

    place.load_isa(progress)

@app.cli.command()
@click.argument('place_identifier')
def detect_language(place_identifier):
    place = get_place(place_identifier)
    for item in place.items_with_candidates():
        for c in item.candidates:
            if 'name' not in c.tags:
                continue
            name = c.tags['name']
            print(name)
            for c in name:
                print(c, unicodedata.name(c))

            continue
            name_tags = {k: v for k, v in c.tags.items() if k.startswith('name')}
            print(name_tags)

@app.cli.command()
def load_all_isa():
    app.config.from_object('config.default')
    database.init_app(app)

    q = Place.query.filter_by(state='load_isa')
    first = True
    for place in q:
        if not first:
            sleep(20)
            first = False
        print(place.display_name)
        place.load_isa()
        place.state = 'ready'
        database.session.commit()

    print('done')

@app.cli.command()
def identifier_match_only():
    app.config.from_object('config.default')
    database.init_app(app)

    q = ItemCandidate.query.filter(ItemCandidate.identifier_match.is_(True))
    for c in q:
        if c.name_match:
            continue
        print(c.tags, dict(c.item.names()))

@app.cli.command()
def add_missing_edits():
    app.config.from_object('config.default')
    database.init_app(app)

    q = Changeset.query.order_by(Changeset.id)
    print(q.count())
    for changeset in q:
        if changeset.edits.count():
            continue
        changeset_id = changeset.id
        root = osm_api.get_changeset(changeset_id)
        edits = osm_api.parse_osm_change(root)
        missing_count = 0
        for edit in edits:
            item_candidate = ItemCandidate.query.get((edit.item_id, edit.osm_id, edit.osm_type))
            if item_candidate:
                database.session.add(edit)
            else:
                missing_count += 1
        print(changeset_id, len(edits), missing_count)
        try:
            database.session.commit()
        except sqlalchemy.exc.IntegrityError:
            print('missing candidate')
            database.session.rollback()

@app.cli.command()
def check_saved_edits():
    app.config.from_object('config.default')
    database.init_app(app)

    q = ChangesetEdit.query.order_by(ChangesetEdit.saved.desc())
    total = q.count()
    report_timestamp = datetime.now()
    reject_count = 0
    stdout = click.get_text_stream('stdout')
    for num, edit in enumerate(q):
        ret = matcher.check_item_candidate(edit.candidate)
        item = edit.candidate.item
        if num % 100 == 0:
            status = f'{num:6,d}/{total:6,d}  {num/total:5.1%}'
            status += f'  bad: {reject_count:3d}  {item.qid:10s} {item.label()}'
            click.echo(status)
            stdout.flush()
        if 'reject' not in ret:
            continue

        for f in 'matching_tags', 'place_names':
            if f in ret and isinstance(ret[f], set):
                ret[f] = list(ret[f])

        try:
            reject = EditMatchReject(edit=edit,
                                     report_timestamp=report_timestamp,
                                     matcher_result=ret)
            database.session.add(reject)
            database.session.commit()
        except sqlalchemy.exc.StatementError:
            pprint(ret)
            raise
        reject_count += 1

        continue

        if len(ret) == 1:
            print((item.qid, item.label(), ret['reject']))
        else:
            print(item.qid, item.label())
            if 'place_names' in ret:
                print('place names:', ret.pop('place_names'))
            pprint(ret)
        if 'osm_tags' not in ret:
            pprint(edit.candidate.tags)

@app.cli.command()
def refresh_all_extracts():
    app.config.from_object('config.default')
    database.init_app(app)

    for place in Place.query:
        print(place.display_name)

        def progress(item):
            print('  ', item.label())

        place.load_extracts(progress=progress)
        print()

@app.cli.command()
def db_now_utc():
    app.config.from_object('config.default')
    database.init_app(app)

    q = database.session.query(func.timezone('utc', func.now()))
    print(q.scalar())

@app.cli.command()
@click.argument('filename')
def get_changeset_edits(filename):
    ids = sorted(int(line) for line in open(filename))

    for changeset_id in ids:
        print(changeset_id)
        osm_api.get_changeset(changeset_id)
        sleep(1)

@app.cli.command()
def dump_bad_match_filters():
    app.config.from_object('config.default')
    database.init_app(app)
    for item in BadMatchFilter.query:
        print(json.dumps({'wikidata': item.wikidata,
                          'osm': item.osm}))

@app.cli.command()
@click.argument('filename')
def load_bad_match_filters(filename):
    app.config.from_object('config.default')
    database.init_app(app)
    for line in open(filename):
        i = BadMatchFilter(**json.loads(line))
        database.session.add(i)
    database.session.commit()

@app.cli.command()
@click.argument('changeset_dir')
def parse_changesets(changeset_dir):
    for f in os.scandir(changeset_dir):
        changeset_id = f.name[:-4]
        root = osm_api.get_changeset(changeset_id)
        edits = osm_api.parse_osm_change(root)
        print(edits)

@app.cli.command()
def larger_areas():
    app.config.from_object('config.default')
    database.init_app(app)

    q = Place.query.filter(Place.state == 'ready',
                           Place.overpass_is_in.isnot(None))
    print(q.count())

    for p in q:
        print(p.name)
        for a in p.is_in():
            print(a['tags'].get('name:en', a['tags']['name']))
        print()

@app.cli.command()
def place_names():
    app.config.from_object('config.default')
    database.init_app(app)
    for place in Place.query:
        print((place.category, place.type))
        print(place.address)
        print(place.name_for_change_comment)
        print()


@app.cli.command()
@click.argument('qid')
def first_paragraph(qid):
    app.config.from_object('config.default')
    database.init_app(app)

    item = Item.query.get(qid[1:])

    fp = item.first_paragraph_language('enwiki')
    print(repr(fp))

def update_place(place, want_isa=None):
    if want_isa is None:
        want_isa = set()

    sock = chat.connect_to_queue()
    msg = {
        'type': 'match',
        'osm_type': place.osm_type,
        'osm_id': place.osm_id,
        'want_isa': want_isa,
    }
    chat.send_command(sock, 'match', **msg)

    while True:
        msg = chat.read_json_line(sock)
        if msg is None:
            break
        yield(msg)

    sock.close()

def place_from_qid(qid):
    def get_search_string(qid):
        entity = wikidata_api.get_entity(qid)
        return browse.qid_to_search_string(qid, entity)

    place = Place.get_by_wikidata(qid)
    if place:  # already in the database
        return place if place.osm_type != 'node' else None

    return browse.place_from_qid(qid, q=get_search_string(qid))

@app.cli.command()
@click.argument('qid')
def match_subregions(qid):
    app.config.from_object('config.default')
    database.init_app(app)

    item_id = int(qid[1:])

    details = browse.get_details(item_id)
    print(qid, details['name'])
    del details['entity']

    place_count = len(details['current_places'])
    for num, p in enumerate(details['current_places']):
        place = place_from_qid(p['qid'])
        if place:
            run = place.latest_matcher_run()
            delta = datetime.utcnow() - run.start if run else None
            if run and run.end and delta < timedelta(hours=1):
                print('fresh', p['qid'], p['label'])
            else:
                print('updating:', p['qid'], p['label'])
                if place.state == 'ready':
                    place.state = 'refresh'
                    database.session.commit()
                for msg in update_place(place):
                    print(f"{num + 1}/{place_count}  {p['qid']} {p['label']}", msg)

        else:
            print('not found:', p['qid'], p['label'])

def matcher_queue_send(cmd):
    sock = chat.connect_to_queue()
    chat.send_command(sock, cmd)

    while True:
        msg = chat.read_json_line(sock)
        if msg is None:
            break
        print(msg)

    sock.close()

@app.cli.command()
@click.argument('place_identifier')
def matcher_update_place(place_identifier):
    place = get_place(place_identifier)

    if place.state == 'ready':
        place.state = 'refresh'
        database.session.commit()
    for msg in update_place(place):
        print(msg)

@app.cli.command()
@click.argument('place_identifier')
@click.argument('want_isa')
def place_filter(place_identifier, want_isa):
    place = get_place(place_identifier)
    if place.state == 'ready':
        place.state = 'refresh'
        database.session.commit()

    for msg in update_place(place, want_isa=want_isa.split(',')):
        print(msg)

@app.cli.command()
@click.argument('place_identifier')
def candidate_filters(place_identifier):
    place = get_place(place_identifier)

    items = place.items_with_candidates().all()

    for isa in get_isa_facets(items):
        print(f"{isa['count']:3d}: {isa['label']} ({isa['qid']}) - {isa['description']}")

@app.cli.command()
def show_all_extract():
    app.config.from_object('config.default')
    database.init_app(app)

    q = Extract.query.filter(Extract.site == 'enwiki',
                             Extract.extract.ilike('%chain%'))
    for i in q:
        print(json.dumps(i.extract))
