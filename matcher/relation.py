from flask import current_app

from .wikipedia import get_items_with_cats
from . import wikidata, user_agent_headers
from .utils import cache_filename, load_from_cache
from .matcher import find_matches, find_tags, filter_candidates
from .db import db_connect
from . import matcher

import requests
import os.path
import subprocess
import json
import psycopg2.extras

def nominatim_lookup(q):
    url = 'http://nominatim.openstreetmap.org/search'

    params = {
        'q': q,
        'format': 'jsonv2',
        'addressdetails': 1,
        'email': current_app.config['ADMIN_EMAIL'],
        'extratags': 1,
        'limit': 20,
        'namedetails': 1,
        'accept-language': 'en',
        'polygon_text': 1,
    }
    r = requests.get(url, params=params, headers=user_agent_headers())
    results = []
    for hit in r.json():
        results.append(hit)
        if hit.get('osm_type') == 'relation':
            relation = Relation(hit['osm_id'])
            relation.save_nominatim(hit)
    return results

class Relation(object):
    def __init__(self, osm_id):
        self.osm_id = osm_id
        self.detail = None
        self.all_tags = None
        self.oql = None
        self.items_with_tags = None

    def item_detail(self):
        return self.detail if self.detail else self.get_detail()

    def get_detail(self):
        self.detail = load_from_cache('{}_nominatim.json'.format(self.osm_id))
        if 'namedetails' not in self.detail:
            nominatim_lookup(self.display_name)  # refresh
            self.detail = load_from_cache('{}_nominatim.json'.format(self.osm_id))
        return self.detail

    @property
    def bbox(self):
        return self.item_detail()['boundingbox']

    @property
    def display_name(self):
        return self.item_detail()['display_name']

    @property
    def namedetails(self):
        return self.item_detail()['namedetails']

    @property
    def name(self):
        nd = self.namedetails
        return nd.get('name:en') or nd['name']

    @property
    def export_name(self):
        return self.name.replace(':', '').replace(' ', '_')

    def get_items_with_tags(self):
        if self.items_with_tags:
            return self.items_with_tags

        items = self.items_with_cats()
        self.all_tags = matcher.find_tags(items)
        self.items_with_tags = items
        return items

    def clip_items_to_polygon(self):
        items = self.get_items_with_tags()
        # assumes that this relation was returned by overpass
        conn = db_connect(self.dbname)
        cur = conn.cursor()
        for enwiki, item in items.items():
            point = "ST_TRANSFORM(ST_SETSRID(ST_MAKEPOINT({}, {}),4326), 3857)".format(item['lon'], item['lat'])
            sql = 'select ST_Within({}, way) from planet_osm_polygon where osm_id={}'.format(point, -self.osm_id)
            cur.execute(sql)
            item['within_area'] = cur.fetchone()[0]
        conn.close()
        return items

    def get_oql(self):
        if self.oql:
            return self.oql
        if not self.all_tags:
            self.get_items_with_tags()

        (south, north, west, east) = self.bbox
        bbox = ','.join('{}'.format(i) for i in (south, west, north, east))
        union = ['rel({});'.format(self.osm_id)]
        # optimisation: we only expect route, type or site on relations
        for tag in self.all_tags:
            relation_only = tag == 'site'
            if '=' in tag:
                k, _, v = tag.partition('=')
                if k in {'site', 'type', 'route'}:
                    relation_only = True
                tag = '"{}"="{}"'.format(k, v)
            for t in ('rel',) if relation_only else ('node', 'way', 'rel'):
                union.append('\n    {}(area.a)[{}][~"^(addr:housenumber|.*name.*)$"~".",i];'.format(t, tag))
        area_id = 3600000000 + int(self.osm_id)
        self.oql = '''[timeout:600][out:xml][bbox:{}];
area({})->.a;
({});
(._;>;);
out qt;'''.format(bbox, area_id, ''.join(union))
        print(self.oql)
        return self.oql

    def wikidata_query(self):
        filename = cache_filename('{}_wikidata.json'.format(self.osm_id))
        if os.path.exists(filename):
            return json.load(open(filename))['results']['bindings']

        r = wikidata.run_query(*self.bbox)
        open(filename, 'wb').write(r.content)
        return r.json()['results']['bindings']

    def get_wikidata_query(self):
        return wikidata.get_query(*self.bbox)

    @property
    def dbname(self):
        return '{}{}'.format(current_app.config['DB_PREFIX'], self.osm_id)

    def load_into_pgsql(self):
        cmd = ['osm2pgsql', '--create', '--drop', '--slim',
                '--hstore-all', '--hstore-add-index',
                '--cache', '1000',
                '--multi-geometry',
                '--host', current_app.config['DB_HOST'],
                '--username', current_app.config['DB_USER'],
                '--database', self.dbname,
                self.overpass_filename]

        p = subprocess.run(cmd,
                           stderr=subprocess.PIPE,
                           env={'PGPASSWORD': current_app.config['DB_PASS']})
        if p.returncode != 0:
            if b'Out of memory' in p.stderr:
                return 'out of memory'
            else:
                return p.stderr
        return

    @property
    def overpass_filename(self):
        overpass_dir = current_app.config['OVERPASS_DIR']
        return os.path.join(overpass_dir, '{}.xml'.format(self.osm_id))

    @property
    def overpass_done(self):
        return os.path.exists(self.overpass_filename)

    @property
    def overpass_error(self):
        # read as bytes to avoid UnicodeDecodeError
        start = open(self.overpass_filename, 'rb').read(1000)
        return b'runtime error' in start or b'Gateway Timeout' in start

    def save_overpass(self, content):
        with open(self.overpass_filename, 'wb') as out:
            out.write(content)

    def save_nominatim(self, hit):
        name = cache_filename('{}_nominatim.json'.format(self.osm_id))
        with open(name, 'w') as f:
            json.dump(hit, f, indent=2)

    def get_candidates(self):
        filename = cache_filename('{}_candidates.json'.format(self.osm_id))
        return json.load(open(filename))

    def run_matcher(self):
        filename = cache_filename('{}_candidates.json'.format(self.osm_id))
        if os.path.exists(filename):
            candidates = json.load(open(filename))
            return candidates  # already filtered

        conn = db_connect(self.dbname)
        psycopg2.extras.register_hstore(conn)

        items = load_from_cache('{}_wbgetentities.json'.format(self.osm_id))
        candidates = find_matches(list(items.values()), conn)
        candidates = filter_candidates(candidates, conn)

        json.dump(candidates, open(filename, 'w'), indent=2)

        conn.close()
        return candidates

    def items_with_cats(self):
        filename = cache_filename('{}_items_with_cats.json'.format(self.osm_id))
        if os.path.exists(filename):
            return json.load(open(filename))

        items = wikidata.parse_query(self.wikidata_query())

        get_items_with_cats(items)
        with open(filename, 'w') as f:
            json.dump(items, f, indent=2)
        return items

    def wbgetentities(self):
        items = self.items_with_cats()
        self.all_tags = find_tags(items)

        filename = cache_filename('{}_wbgetentities.json'.format(self.osm_id))
        if os.path.exists(filename):
            return json.load(open(filename))

        wikidata.wbgetentities(items)

        json.dump(items, open(filename, 'w'), indent=2)
        return items
