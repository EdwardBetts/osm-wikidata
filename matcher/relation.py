from flask import current_app

from .wikipedia import get_items_with_cats
from . import wikidata
from .utils import cache_filename, load_from_cache
from .matcher import find_matches, find_tags, filter_candidates
from .db import db_connect

import os.path
import subprocess
import json
import psycopg2.extras

class Relation(object):
    def __init__(self, osm_id):
        self.osm_id = osm_id

    def item_detail(self):
        return load_from_cache('{}_nominatim.json'.format(self.osm_id))

    @property
    def bbox(self):
        return self.item_detail()['boundingbox']

    def oql(self, tags):
        (south, north, west, east) = self.bbox
        bbox = ','.join('{}'.format(i) for i in (south, west, north, east))
        union = []
        # optimisation: we only expect type=site or site=<something> on relations
        for tag in tags:
            relation_only = tag == 'site'
            if '=' in tag:
                k, _, v = tag.partition('=')
                if k == 'site' or tag == 'type=site':
                    relation_only = True
                tag = '"{}"="{}"'.format(k, v)
            for t in ('rel',) if relation_only else ('node', 'way', 'rel'):
                union.append('\n    {}(area.a)[{}][~"^(addr:housenumber|.*name.*)$"~".",i];'.format(t, tag))
        area_id = 3600000000 + int(self.osm_id)
        oql = '''[timeout:600][out:xml][bbox:{}];
area({})->.a;
({});
(._;>;);
out qt;'''.format(bbox, area_id, ''.join(union))
        return oql

    def wikidata_query(self):
        filename = cache_filename('{}_wikidata.json'.format(self.osm_id))
        if os.path.exists(filename):
            return json.load(open(filename))['results']['bindings']

        r = wikidata.run_query(*self.bbox)
        open(filename, 'wb').write(r.content)
        return r.json()['results']['bindings']

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

    def save_overpass(self, content):
        with open(self.overpass_filename, 'wb') as out:
            out.write(content)

    def save_nominatim(self, hit):
        name = cache_filename('{}_nominatim.json'.format(self.osm_id))
        with open(name, 'w') as f:
            json.dump(hit, f, indent=2)

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


