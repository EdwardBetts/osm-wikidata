from .view import app, get_top_existing
from .model import Changeset, get_bad
from .place import Place
from . import database, mail, matcher, nominatim
from datetime import datetime, timedelta
from tabulate import tabulate
from sqlalchemy import inspect, func
from time import time, sleep
import json
import click

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

    if place_identifier.isdigit():
        place = Place.query.get(place_identifier)
    else:
        osm_type, osm_id = place_identifier.split('/')
        place = Place.query.filter_by(osm_type=osm_type, osm_id=osm_id).one()

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
    if place_identifier.isdigit():
        place = Place.query.get(place_identifier)
    else:
        osm_type, osm_id = place_identifier.split('/')
        place = Place.query.filter_by(osm_type=osm_type, osm_id=osm_id).one()

    print(place.display_name)
    print(place.state)

    print('do match')
    place.do_match()
    print(place.state, place.display_name)
    print('https://osm.wikidata.link/candidates/{place.osm_type}/{place.osm_id}'.format(place=place))
