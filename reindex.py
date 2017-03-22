#!/usr/bin/python3
from matcher.model import Place, Item, ItemCandidate
from matcher import database, user_agent_headers, db, matcher, wikidata
from matcher.view import app
from time import sleep
import requests
import psycopg2.extras
import sys

def wbgetentities(p):
    q = p.items.filter(Item.tags != '{}')
    items = {i.qid: i for i in q}

    for qid, entity in wikidata.entity_iter(items.keys()):
        item = items[qid]
        item.entity = entity
        database.session.add(item)
    database.session.commit()

def do_reindex(place):
    print(place.display_name)

    existing = {item.item_id: item.tags for item in place.items}
    all_tags = place.all_tags

    place.add_tags_to_items()
    print('tags updated')

    tag_change = False
    for item in place.items:
        old = existing[item.item_id]
        if item.tags == old:
            continue
        tag_change = True
        print(item.qid, item.enwiki)
        print('  old:', old)
        print('  new:', item.tags)

    if tag_change:
        print('no change')
        place.state = 'ready'
        database.session.commit()
        return

    wbgetentities(place)
    database.session.commit()

    print(place.all_tags)
    print(all_tags)
    sleep(10)

    if place.all_tags != all_tags:
        oql = place.get_oql()
        overpass_url = 'http://overpass-api.de/api/interpreter'

        print('running overpass query')
        r = requests.post(overpass_url, data=oql, headers=user_agent_headers())
        print('overpass done')

        place.save_overpass(r.content)
        place.state = 'postgis'
        database.session.commit()

        print('running osm2pgsql')
        place.load_into_pgsql(capture_stderr=False)
        place.state = 'osm2pgsql'
        database.session.commit()

    conn = db.db_connect(place.dbname)
    psycopg2.extras.register_hstore(conn)
    cur = conn.cursor()

    cat_to_ending = matcher.build_cat_to_ending()

    q = place.items.filter(Item.entity.isnot(None)).order_by(Item.item_id)
    for item in q:
        candidates = matcher.find_item_matches(cur, item, cat_to_ending)
        for i in (candidates or []):
            c = ItemCandidate.query.get((item.item_id, i['osm_id'], i['osm_type']))
            if not c:
                c = ItemCandidate(**i, item=item)
                database.session.add(c)
        print(len(candidates), item.enwiki)
    place.state = 'ready'
    database.session.commit()

    conn.close()

def reindex_all(skip_places=None):
    q = Place.query.filter(Place.state == 'ready')
    if skip_places:
        q = q.filter(~Place.osm_id.in_(skip_places))
    for place in Place.query.filter(Place.state == 'ready', ~Place.display_name.like('San Fran%')):
        if place.display_name.startswith('San Fran'):
            continue
        do_reindex(place)


app.config.from_object('config.default')
with app.app_context():
    database.init_app(app)
    if len(sys.argv) > 1:
        osm_id = sys.argv[1]
        place = Place.query.get(osm_id)
        do_reindex(place)
    else:
        reindex_all()
