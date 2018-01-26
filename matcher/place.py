from flask import current_app, url_for, g, abort
from .model import Base, Item, ItemCandidate, PlaceItem, ItemTag, Changeset, osm_type_enum, get_bad
from sqlalchemy.types import BigInteger, Float, Integer, JSON, String, DateTime, Boolean
from sqlalchemy.schema import Column
from sqlalchemy import func, select, cast
from sqlalchemy.schema import ForeignKeyConstraint, ForeignKey, Column, UniqueConstraint
from sqlalchemy.orm import relationship, backref, column_property, object_session, deferred, load_only
from geoalchemy2 import Geography, Geometry
from sqlalchemy.ext.hybrid import hybrid_property
from .database import session, get_tables
from . import wikidata, matcher, wikipedia, overpass, utils
from collections import Counter
from .overpass import oql_from_tag
from time import time

import subprocess
import os.path
import re

overpass_types = {'way': 'way', 'relation': 'rel', 'node': 'node'}

skip_tags = {'route:road',
             'highway=primary',
             'highway=road',
             'highway=service',
             'highway=motorway',
             'highway=trunk',
             'highway=unclassified',
             'highway',
             'name',
             'website',
             'type=waterway',
             'waterway=river'
             'addr:street',
             'type=associatedStreet'}

def envelope(bbox):
    # note: different order for coordinates, xmin first, not ymin
    ymin, ymax, xmin, xmax = bbox
    return func.ST_MakeEnvelope(xmin, ymin, xmax, ymax, 4326)

class Place(Base):
    __tablename__ = 'place'
    place_id = Column(BigInteger, primary_key=True, autoincrement=False)
    osm_type = Column(osm_type_enum, nullable=False)
    osm_id = Column(BigInteger, nullable=False)
    radius = Column(Integer)  # only for nodes
    display_name = Column(String, nullable=False)
    category = Column(String, nullable=False)
    type = Column(String, nullable=False)
    place_rank = Column(Integer, nullable=False)
    icon = Column(String)
    geom = Column(Geography(spatial_index=True))
    south = Column(Float, nullable=False)
    west = Column(Float, nullable=False)
    north = Column(Float, nullable=False)
    east = Column(Float, nullable=False)
    extratags = deferred(Column(JSON))
    address = deferred(Column(JSON))
    namedetails = deferred(Column(JSON))
    item_count = Column(Integer)
    candidate_count = Column(Integer)
    state = Column(String, index=True)
    override_name = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    added = Column(DateTime, default=func.now())
    wikidata_query_timeout = Column(Boolean, default=False)

    area = column_property(func.ST_Area(geom))
    geojson = column_property(func.ST_AsGeoJSON(geom, 4), deferred=True)
    srid = column_property(func.ST_SRID(geom))
    # match_ratio = column_property(candidate_count / item_count)

    items = relationship('Item',
                         secondary='place_item',
                         lazy='dynamic',
                         backref=backref('places', lazy='dynamic'))

    __table_args__ = (
        UniqueConstraint('osm_type', 'osm_id'),
    )

    @classmethod
    def get_by_osm(cls, osm_type, osm_id):
        return cls.query.filter_by(osm_type=osm_type, osm_id=osm_id).one_or_none()

    def get_address_key(self, key):
        if isinstance(self.address, dict):
            return self.address.get(key)
        for line in self.address or []:
            if line['type'] == key:
                return line['name']

    @property
    def country_code(self):
        return self.get_address_key('country_code')

    @property
    def country(self):
        return self.get_address_key('country')

    @classmethod
    def get_or_abort(cls, osm_type, osm_id):
        if osm_type in {'way', 'relation'}:
            place = cls.get_by_osm(osm_type, osm_id)
            if place:
                return place
        abort(404)

    @hybrid_property
    def area_in_sq_km(self):
        return self.area / (1000 * 1000)

    @hybrid_property
    def too_big(self):
        return self.area_in_sq_km > 39000

    def update_from_nominatim(self, hit):
        keys = ('display_name', 'place_rank', 'category', 'type', 'icon',
                'extratags', 'namedetails')
        for n in keys:
            setattr(self, n, hit.get(n))
        self.address = [dict(name=n, type=t) for t, n in hit['address'].items()]

    def change_comment(self, item_count):
        if item_count == 1:
            return 'add wikidata tag'
        return 'add wikidata tags within ' + self.name_for_change_comment

    @property
    def name_for_changeset(self):
        address = self.address
        n = self.name
        if not address:
            return self.name
        if isinstance(address, list):
            d = {a['type']: a['name'] for a in address}
        elif isinstance(address, dict):
            d = address

        if d.get('country_code') == 'us':
            state = d.get('state')
            if state and n != state:
                return n + ', ' + state

        country = d.get('country')
        if country and self.name != country:
            return '{} ({})'.format(self.name, country)

        return self.name

    @property
    def name_for_change_comment(self):
        n = self.name

        if self.address:
            if isinstance(self.address, list):
                address = {a['type']: a['name'] for a in self.address}
            elif isinstance(self.address, dict):
                address = self.address

            if address.get('country_code') == 'us':
                state = address.get('state')
                if state and n != state:
                    return n + ', ' + state
        return 'the ' + n if ' of ' in n else n

    @classmethod
    def from_nominatim(cls, hit):
        keys = ('place_id', 'osm_type', 'osm_id', 'lat', 'lon', 'display_name',
                'place_rank', 'category', 'type', 'icon', 'extratags',
                'namedetails')
        n = {k: hit[k] for k in keys if k in hit}
        if hit['osm_type'] == 'node':
            n['radius'] = 1000   # 1km
        bbox = hit['boundingbox']
        (n['south'], n['north'], n['west'], n['east']) = bbox
        n['geom'] = hit['geotext']
        n['address'] = [dict(name=n, type=t) for t, n in hit['address'].items()]
        return cls(**n)

    @classmethod
    def get_or_add_place(cls, hit):
        place = cls.query.filter_by(osm_type=hit['osm_type'],
                                    osm_id=hit['osm_id']).one_or_none()

        if place and place.place_id != hit['place_id']:
            place.update_from_nominatim(hit)
        elif not place:
            place = Place.query.get(hit['place_id'])
            if place:
                place.update_from_nominatim(hit)
            else:
                place = cls.from_nominatim(hit)
                session.add(place)
        session.commit()
        return place

    @property
    def match_ratio(self):
        if self.item_count:
            return self.candidate_count / self.item_count

    @property
    def bbox(self):
        return (self.south, self.north, self.west, self.east)

    @property
    def display_area(self):
        return '{:.1f} km²'.format(self.area_in_sq_km)

    def get_wikidata_query(self):
        if self.osm_type == 'node':
            query = wikidata.get_point_query(self.lat, self.lon, self.radius)
        else:
            query = wikidata.get_enwiki_query(*self.bbox)
        return query

    def items_from_wikidata(self, bbox=None):
        if bbox is None:
            bbox = self.bbox

        q = wikidata.get_enwiki_query(*bbox)
        rows = wikidata.run_query(q)

        items = wikidata.parse_enwiki_query(rows)

        try:  # add items with the coordinates in the HQ field
            q = wikidata.get_enwiki_hq_query(*bbox)
            items.update(wikidata.parse_enwiki_query(rows))
        except wikidata.QueryError:
            pass  # HQ query timeout isn't fatal

        q = wikidata.get_item_tag_query(*bbox)
        rows = wikidata.run_query(q)
        wikidata.parse_item_tag_query(rows, items)

        try:  # add items with the coordinates in the HQ field
            q = wikidata.get_hq_item_tag_query(*bbox)
            rows = wikidata.run_query(q)
            wikidata.parse_item_tag_query(rows, items)
        except wikidata.QueryError:
            pass  # HQ query timeout isn't fatal

        # would be nice to include OSM chunk information with each
        # item not doing it at this point because it means lots
        # of queries easier once the items are loaded into the database
        return {k: v for k, v in items.items() if self.covers(v)}

    def covers(self, item):
        q = (select([func.ST_Covers(Place.geom, item['location'])])
                .where(Place.place_id == self.place_id))
        return object_session(self).scalar(q)

    def add_tags_to_items(self):
        for item in self.items.filter(Item.categories != '{}'):
            # if wikidata says this is a place then adding tags
            # from wikipedia can just confuse things
            if any(t.startswith('place') for t in item.tags):
                continue
            for t in matcher.categories_to_tags(item.categories):
                item.tags.add(t)

    @property
    def prefix(self):
        return 'osm_{}'.format(self.place_id)

    @property
    def overpass_filename(self):
        overpass_dir = current_app.config['OVERPASS_DIR']
        return os.path.join(overpass_dir, '{}.xml'.format(self.place_id))

    def is_overpass_filename(self, f):
        ''' Does the overpass filename belongs to this place. '''
        place_id = str(self.place_id)
        return f == place_id + '.xml' or f.startswith(place_id + '_')

    def delete_overpass(self):
        for f in os.scandir(current_app.config['OVERPASS_DIR']):
            if self.is_overpass_filename(f.name):
                os.remove(f.path)

    def clean_up(self):
        place_id = self.place_id

        engine = session.bind
        for t in get_tables():
            if not t.startswith(self.prefix):
                continue
            engine.execute('drop table if exists {}'.format(t))
        engine.execute('commit')

        overpass_dir = current_app.config['OVERPASS_DIR']
        for f in os.listdir(overpass_dir):
            if not any(f.startswith(str(place_id) + end) for end in ('_', '.')):
                continue
            os.remove(os.path.join(overpass_dir, f))

    @property
    def overpass_done(self):
        return os.path.exists(self.overpass_filename)

    def items_with_candidates(self):
        return self.items.join(ItemCandidate)

    def items_with_candidates_count(self):
        if self.state != 'ready':
            return
        return (session.query(Item.item_id)
                       .join(PlaceItem)
                       .join(Place)
                       .join(ItemCandidate)
                       .filter(Place.place_id == self.place_id)
                       .group_by(Item.item_id)
                       .count())

    def items_without_candidates(self):
        return self.items.outerjoin(ItemCandidate).filter(ItemCandidate.item_id.is_(None))

    def items_with_multiple_candidates(self):
        # select count(*) from (select 1 from item, item_candidate where item.item_id=item_candidate.item_id) x;
        q = (self.items.join(ItemCandidate)
                 .group_by(Item.item_id)
                 .having(func.count(Item.item_id) > 1)
                 .with_entities(Item.item_id))
        return q

    @property
    def name(self):
        if self.override_name:
            return self.override_name

        name = self.namedetails.get('name:en') or self.namedetails.get('name')
        display = self.display_name
        if not name:
            return display

        for short in ('City', '1st district'):
            start = len(short) + 2
            if name == short and display.startswith(short + ', ') and ', ' in display[start:]:
                name = display[:display.find(', ', start)]
                break

        return name

    @property
    def name_extra_detail(self):
        for n in 'name:en', 'name':
            if n not in self.namedetails:
                continue
            start = self.namedetails[n] + ', '
            if self.display_name.startswith(start):
                return self.display_name[len(start):]

    @property
    def export_name(self):
        return self.name.replace(':', '').replace(' ', '_')

    def osm2pgsql_cmd(self):
        return ['osm2pgsql', '--create', '--drop', '--slim',
                '--hstore-all', '--hstore-add-index',
                '--prefix', self.prefix,
                '--cache', '1000',
                '--multi-geometry',
                '--host', current_app.config['DB_HOST'],
                '--username', current_app.config['DB_USER'],
                '--database', current_app.config['DB_NAME'],
                self.overpass_filename]

    def load_into_pgsql(self, capture_stderr=True):
        if not os.path.exists(self.overpass_filename):
            return 'no data from overpass to load with osm2pgsql'

        if os.stat(self.overpass_filename).st_size == 0:
            return 'no data from overpass to load with osm2pgsql'

        cmd = self.osm2pgsql_cmd()

        if not capture_stderr:
            p = subprocess.run(cmd,
                               env={'PGPASSWORD': current_app.config['DB_PASS']})
            return
        p = subprocess.run(cmd,
                           stderr=subprocess.PIPE,
                           env={'PGPASSWORD': current_app.config['DB_PASS']})
        if p.returncode != 0:
            if b'Out of memory' in p.stderr:
                return 'out of memory'
            else:
                return p.stderr.decode('utf-8')

    def save_overpass(self, content):
        with open(self.overpass_filename, 'wb') as out:
            out.write(content)

    @property
    def all_tags(self):
        tags = set()
        for item in self.items:
            tags |= set(item.tags)
            tags |= item.disused_tags()
        tags.difference_update(skip_tags)
        return matcher.simplify_tags(tags)

    @property
    def overpass_type(self):
        return overpass_types[self.osm_type]

    @property
    def overpass_filter(self):
        return 'around:{0.radius},{0.lat},{0.lon}'.format(self)

    def building_names(self):
        re_paren = re.compile(r'\(.+\)')
        re_drop = re.compile(r'\b(the|and|at|of|de|le|la|les|von)\b')
        names = set()
        for building in (item for item in self.items if 'building' in item.tags):
            for n in building.names():
                if n[0].isdigit() and ',' in n:
                    continue
                n = n.lower()
                comma = n.rfind(', ')
                if comma != -1 and not n[0].isdigit():
                    n = n[:comma]

                n = re_paren.sub('', n).replace("'s", "('s)?")
                n = n.replace('(', '').replace(')', '').replace('.', r'\.')
                names.add(n)
                names.add(re_drop.sub('', n))

        names = sorted(n.replace(' ', '\W*') for n in names)
        if names:
            return '({})'.format('|'.join(names))

    def get_oql(self, buildings_special=False):
        assert self.osm_type != 'node'

        bbox = '{:f},{:f},{:f},{:f}'.format(self.south, self.west, self.north, self.east)

        tags = self.all_tags

        if buildings_special and 'building' in tags:
            buildings = self.building_names()
            tags.remove('building')
        else:
            buildings = None

        return overpass.oql_for_area(self.overpass_type,
                                     self.osm_id,
                                     tags,
                                     bbox,
                                     buildings)

        union = ['{}({});'.format(self.overpass_type, self.osm_id)]

        for tag in self.all_tags:
            u = (oql_from_tag(tag, filters=self.overpass_filter)
                 if self.osm_type == 'node'
                 else oql_from_tag(tag))
            if u:
                union += u

        if self.osm_type == 'node':
            oql = ('[timeout:300][out:xml];\n' +
                   '({});\n' +
                   '(._;>;);\n' +
                   'out qt;').format(''.join(union))
            return oql

        bbox = '{:f},{:f},{:f},{:f}'.format(self.south, self.west, self.north, self.east)
        offset = {'way': 2400000000, 'relation': 3600000000}
        area_id = offset[self.osm_type] + int(self.osm_id)

        oql = ('[timeout:300][out:xml][bbox:{}];\n' +
               'area({})->.a;\n' +
               '({});\n' +
               '(._;>;);\n' +
               'out qt;').format(bbox, area_id, ''.join(union))
        return oql

    def candidates_url(self, **kwargs):
        if g.get('filter'):
            kwargs['name_filter'] = g.filter
            endpoint = 'candidates_with_filter'
        else:
            endpoint = 'candidates'

        return url_for(endpoint,
                       osm_type=self.osm_type,
                       osm_id=self.osm_id,
                       **kwargs)

    def place_url(self, endpoint, **kwargs):
        return url_for(endpoint,
                       osm_type=self.osm_type,
                       osm_id=self.osm_id,
                       **kwargs)

    def matcher_progress_url(self):
        return self.place_url('matcher.matcher_progress')

    def matcher_done_url(self, start, refresh):
        kwargs = {'refresh': 1} if refresh else {}
        return self.place_url('matcher.matcher_done',
                              start=start, **kwargs)

    def item_list(self):
        lang = self.most_common_language() or 'en'
        q = self.items.filter(Item.entity.isnot(None)).order_by(Item.item_id)
        return [{'id': i.item_id, 'name': i.label(lang=lang)}
                for i in q]

    def save_items(self, items, debug=None):
        if debug is None:
            def debug(msg):
                pass
        debug('save items')
        seen = {}
        for qid, v in items.items():
            wikidata_id = int(qid[1:])
            item = Item.query.get(wikidata_id)

            if item:
                item.location = v['location']
            else:
                item = Item(item_id=wikidata_id, location=v['location'])
                session.add(item)
            for k in 'enwiki', 'categories', 'query_label':
                if k in v:
                    setattr(item, k, v[k])

            tags = set(v['tags'])
            # if wikidata says this is a place then adding tags
            # from wikipedia can just confuse things
            if 'categories' in v and not any(t.startswith('place') for t in tags):
                for t in matcher.categories_to_tags(v['categories']):
                    tags.add(t)

            if 'building' in tags and len(tags) > 1:
                tags.remove('building')

            item.tags = tags
            if qid in seen:
                continue

            seen[qid] = item

            existing = PlaceItem.query.filter_by(item=item, place=self).one_or_none()
            if not existing:
                place_item = PlaceItem(item=item, place=self)
                session.add(place_item)

        for item in self.items:
            if item.qid in seen:
                continue
            link = PlaceItem.query.filter_by(item=item, place=self).one()
            session.delete(link)
        debug('done')

        return seen

    def load_items(self, bbox=None, debug=False):
        if bbox is None:
            bbox = self.bbox

        items = self.items_from_wikidata(bbox)
        if debug:
            print('{:d} items'.format(len(items)))

        wikipedia.add_enwiki_categories(items)

        self.save_items(items)

        session.commit()

    def load_extracts(self, debug=False, progress=None):
        by_title = {item.enwiki: item for item in self.items if item.enwiki}

        for title, extract in wikipedia.get_extracts(by_title.keys()):
            item = by_title[title]
            if debug:
                print(title)
            item.extract = extract
            item.extract_names = wikipedia.html_names(extract)
            progress(item)

    def wbgetentities(self, debug=False):
        sub = (session.query(Item.item_id)
                      .join(ItemTag)
                      .group_by(Item.item_id)
                      .subquery())
        q = (self.items.filter(Item.item_id == sub.c.item_id)
                       .options(load_only(Item.qid)))

        if debug:
            print('running wbgetentities query')
            print(q)
            print(q.count())
        items = {i.qid: i for i in q}
        if debug:
            print('{} items'.format(len(items)))

        for qid, entity in wikidata.entity_iter(items.keys(), debug=debug):
            if debug:
                print(qid)
            items[qid].entity = entity

    def most_common_language(self):
        lang_count = Counter()
        for item in self.items:
            if item.entity and 'labels' in item.entity:
                for lang in item.entity['labels'].keys():
                    lang_count[lang] += 1
        try:
            return lang_count.most_common(1)[0][0]
        except IndexError:
            return None

    def database_loaded(self):
        tables = get_tables()
        expect = [self.prefix + '_' + t for t in ('line', 'point', 'polygon')]
        if not all(t in tables for t in expect):
            return

    def run_matcher(self, debug=False, progress=None):
        if progress is None:
            def progress(candidates, item):
                pass
        conn = session.bind.raw_connection()
        cur = conn.cursor()

        items = self.items.filter(Item.entity.isnot(None)).order_by(Item.item_id)
        for item in items:
            if debug:
                print('searching for', item.label())
                print(item.tags)

            if item.skip_item_during_match():
                candidates = []
            else:
                t0 = time()
                candidates = matcher.find_item_matches(cur, item, self.prefix, debug=debug)
                seconds = time() - t0
                if debug:
                    print('find_item_matches took {:.1f}'.format(seconds))
                    print('{}: {}'.format(len(candidates), item.label()))

            progress(candidates, item)

            # if this is a refresh we remove candidates that no longer match
            as_set = {(i['osm_type'], i['osm_id']) for i in candidates}
            for c in item.candidates[:]:
                if (c.osm_type, c.osm_id) not in as_set:
                    c.bad_matches.delete()
                    session.delete(c)

            if not candidates:
                continue

            for i in candidates:
                c = ItemCandidate.query.get((item.item_id, i['osm_id'], i['osm_type']))
                if c:
                    c.update(i)
                else:
                    c = ItemCandidate(**i, item=item)
                    session.add(c)

        self.state = 'ready'
        self.item_count = self.items.count()
        self.candidate_count = self.items_with_candidates_count()
        session.commit()

        conn.close()

    def do_match(self, debug=True):
        if self.state == 'ready':  # already done
            return

        if not self.state or self.state == 'refresh':
            print('load items')
            self.load_items()  # includes categories
            self.state = 'tags'
            session.commit()

        if self.state == 'tags':
            print('wbgetentities')
            self.wbgetentities(debug=debug)
            print('load extracts')
            self.load_extracts(debug=debug)
            self.state = 'wbgetentities'
            session.commit()

        if self.state in ('wbgetentities', 'overpass_error', 'overpass_timeout'):
            print('loading_overpass')
            self.get_overpass()
            self.state = 'postgis'
            session.commit()

        if self.state == 'postgis':
            print('running osm2pgsql')
            self.load_into_pgsql(capture_stderr=False)
            self.state = 'osm2pgsql'
            session.commit()

        if self.state == 'osm2pgsql':
            print('run matcher')
            self.run_matcher(debug=debug)
            print('ready')
            self.state = 'ready'
            session.commit()

    def get_overpass(self):
        oql = self.get_oql()
        if self.area_in_sq_km < 800:
            r = overpass.run_query_persistent(oql)
            assert r
            self.save_overpass(r.content)
        else:
            self.chunk()

    def get_items(self):
        items = [item for item in self.items_with_candidates()
                 if all('wikidata' not in c.tags for c in item.candidates)]

        filter_list = matcher.filter_candidates_more(items, bad=get_bad(items))
        add_tags = []
        for item, match in filter_list:
            picked = match.get('candidate')
            if not picked:
                continue
            dist = picked.dist
            intersection = set()
            for k, v in picked.tags.items():
                tag = k + '=' + v
                if k in item.tags or tag in item.tags:
                    intersection.add(tag)
            if dist < 400:
                symbol = '+'
            elif dist < 4000 and intersection == {'place=island'}:
                symbol = '+'
            elif dist < 3000 and intersection == {'natural=wetland'}:
                symbol = '+'
            elif dist < 2000 and intersection == {'natural=beach'}:
                symbol = '+'
            elif dist < 2000 and intersection == {'natural=bay'}:
                symbol = '+'
            elif dist < 2000 and intersection == {'aeroway=aerodrome'}:
                symbol = '+'
            elif dist < 1000 and intersection == {'amenity=school'}:
                symbol = '+'
            elif dist < 800 and intersection == {'leisure=park'}:
                symbol = '+'
            elif dist < 2000 and intersection == {'landuse=reservoir'}:
                symbol = '+'
            elif dist < 3000 and item.tags == {'place', 'admin_level'}:
                symbol = '+'
            elif dist < 3000 and item.tags == {'place', 'place=town', 'admin_level'}:
                symbol = '+'
            elif dist < 3000 and item.tags == {'admin_level', 'place', 'place=neighbourhood'} and 'place' in picked.tags:
                symbol = '+'
            else:
                symbol = '?'

            print('{:1s}  {:9s}  {:5.0f}  {!r}  {!r}'.format(symbol, item.qid, picked.dist, item.tags, intersection))
            if symbol == '+':
                add_tags.append((item, picked))
        return add_tags

    def chunk4(self):  # this code is unused
        print('chunk4')
        n = {}
        (n['south'], n['north'], n['west'], n['east']) = self.bbox
        ns = (n['north'] - n['south']) / 2
        ew = (n['east'] - n['west']) / 2

        chunks = [
            {'name': 'south west', 'bbox': (n['south'], n['south'] + ns, n['west'], n['west'] + ew)},
            {'name': 'south east', 'bbox': (n['south'], n['south'] + ns, n['west'] + ew, n['east'])},
            {'name': 'north west', 'bbox': (n['south'] + ns, n['north'], n['west'], n['west'] + ew)},
            {'name': 'north east', 'bbox': (n['south'] + ns, n['north'], n['west'] + ew, n['east'])},
        ]

        return chunks

    def chunk9(self):  # this code is unused
        print('chunk9')
        n = {}
        (n['south'], n['north'], n['west'], n['east']) = self.bbox
        ns = (n['north'] - n['south']) / 3
        ew = (n['east'] - n['west']) / 3

        chunks = [
            {'name': 'south west', 'bbox': (n['south'], n['south'] + ns, n['west'], n['west'] + ew)},
            {'name': 'south     ', 'bbox': (n['south'], n['south'] + ns, n['west'] + ew, n['west'] + ew * 2)},
            {'name': 'south east', 'bbox': (n['south'], n['south'] + ns, n['west'] + ew * 2, n['east'])},

            {'name': 'west  ', 'bbox': (n['south'] + ns, n['south'] + ns * 2, n['west'], n['west'] + ew)},
            {'name': 'centre', 'bbox': (n['south'] + ns, n['south'] + ns * 2, n['west'] + ew, n['west'] + ew * 2)},
            {'name': 'east  ', 'bbox': (n['south'] + ns, n['south'] + ns * 2, n['west'] + ew * 2, n['east'])},

            {'name': 'north west', 'bbox': (n['south'] + ns * 2, n['north'], n['west'], n['west'] + ew)},
            {'name': 'north     ', 'bbox': (n['south'] + ns * 2, n['north'], n['west'] + ew, n['west'] + ew * 2)},
            {'name': 'north east', 'bbox': (n['south'] + ns * 2, n['north'], n['west'] + ew * 2, n['east'])},
        ]

        return chunks

    def chunk_n(self, n):
        n = max(1, n)
        (south, north, west, east) = self.bbox
        ns = (north - south) / n
        ew = (east - west) / n

        chunks = []
        for row in range(n):
            chunks += [(south + ns * row, south + ns * (row + 1),
                         west + ew * col, west + ew * (col + 1)) for col in range(n)]

        return chunks

    def get_chunks(self):
        chunk_size = utils.calc_chunk_size(self.area_in_sq_km)
        bbox_chunks = self.chunk_n(chunk_size)

        chunks = []
        for num, chunk in enumerate(bbox_chunks):
            filename = self.chunk_filename(num, bbox_chunks)
            oql = self.oql_for_chunk(chunk, include_self=(num == 0))
            chunks.append({
                'num': num,
                'oql': oql,
                'filename': filename,
            })
        return chunks

    def chunk_filename(self, num, chunks):
        if len(chunks) == 1:
            return '{}.xml'.format(self.place_id)
        return '{}_{:03d}_{:03d}.xml'.format(self.place_id, num, len(chunks))

    def chunk(self):
        chunk_size = utils.calc_chunk_size(self.area_in_sq_km)
        chunks = self.chunk_n(chunk_size)

        print('chunk size:', chunk_size)

        files = []
        for num, chunk in enumerate(chunks):
            filename = self.chunk_filename(num, len(chunks))
            # print(num, q.count(), len(tags), filename, list(tags))
            full = os.path.join('overpass', filename)
            files.append(full)
            if os.path.exists(full):
                continue
            oql = self.oql_for_chunk(chunk, include_self=(num == 0))

            r = overpass.run_query_persistent(oql)
            if not r:
                print(oql)
            assert r
            open(full, 'wb').write(r.content)

        cmd = ['osmium', 'merge'] + files + ['-o', self.overpass_filename]
        print(' '.join(cmd))
        subprocess.run(cmd)

    def oql_for_chunk(self, chunk, include_self=False):
        q = self.items.filter(cast(Item.location, Geometry).contained(envelope(chunk)))

        tags = set()
        for item in q:
            tags |= set(item.tags)
        tags.difference_update(skip_tags)
        tags = matcher.simplify_tags(tags)
        if not(tags):
            print('no tags, skipping')
            return

        ymin, ymax, xmin, xmax = chunk
        bbox = '{:f},{:f},{:f},{:f}'.format(ymin, xmin, ymax, xmax)

        oql = overpass.oql_for_area(self.overpass_type,
                                    self.osm_id,
                                    tags,
                                    bbox, None,
                                    include_self=include_self)
        return oql

    def chunk_count(self):
        return len(self.chunk_n(utils.calc_chunk_size(self.area_in_sq_km)))

    def geojson_chunks(self):
        chunk_size = utils.calc_chunk_size(self.area_in_sq_km)
        chunks = []
        for chunk in self.chunk_n(chunk_size):
            clip = func.ST_Intersection(Place.geom, envelope(chunk))

            geojson = (session.query(func.ST_AsGeoJSON(clip, 4))
                              .filter(Place.place_id == self.place_id)
                              .scalar())

            chunks.append(geojson)
        return chunks

    def wikidata_chunk_size(self):
        area = self.area_in_sq_km
        if area < 10000:
            return 2 if self.wikidata_query_timeout else 1
        else:
            return utils.calc_chunk_size(area, size=32)

    def latest_matcher_run(self):
        return self.matcher_runs.order_by(PlaceMatcher.start.desc()).first()

class PlaceMatcher(Base):
    __tablename__ = 'place_matcher'
    start = Column(DateTime, default=func.now(), primary_key=True)
    end = Column(DateTime)
    osm_type = Column(osm_type_enum, primary_key=True)
    osm_id = Column(BigInteger, primary_key=True)
    remote_addr = Column(String)
    user_id = Column(Integer, ForeignKey('user.id'))
    user_agent = Column(String)
    is_refresh = Column(Boolean, nullable=False)

    place = relationship('Place', uselist=False,
                         backref=backref('matcher_runs',
                                         lazy='dynamic',
                                         order_by='PlaceMatcher.start.desc()'))

    user = relationship('User', uselist=False,
                         backref=backref('matcher_runs',
                                         lazy='dynamic',
                                         order_by='PlaceMatcher.start.desc()'))

    __table_args__ = (
        ForeignKeyConstraint(
            ['osm_type', 'osm_id'],
            ['place.osm_type', 'place.osm_id'],
        ),
    )

    def duration(self):
        if self.end:
            return self.end - self.start

    def complete(self):
        self.end = func.now()
        session.commit()

def get_top_existing(limit=39):
    cols = [Place.place_id, Place.display_name, Place.area, Place.state,
            Place.candidate_count, Place.item_count]
    c = func.count(Changeset.place_id)

    q = (Place.query.filter(Place.state.in_(['ready', 'refresh']),
                            Place.area > 0,
                            Place.candidate_count > 4)
                    .options(load_only(*cols))
                    .outerjoin(Changeset)
                    .group_by(*cols)
                    .having(c == 0)
                    .order_by((Place.item_count / Place.area).desc()))
    return q[:limit]
