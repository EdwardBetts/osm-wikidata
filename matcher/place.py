from flask import current_app, url_for, g, abort
from .model import Base, Item, ItemCandidate, PlaceItem, ItemTag, Changeset, osm_type_enum, get_bad
from sqlalchemy.types import BigInteger, Float, Integer, JSON, String, DateTime
from sqlalchemy.schema import Column
from sqlalchemy import func, select, cast
from sqlalchemy.orm import relationship, backref, column_property, object_session, deferred, load_only
from geoalchemy2 import Geography, Geometry
from sqlalchemy.ext.hybrid import hybrid_property
from .database import session, get_tables
from . import wikidata, matcher, wikipedia, overpass
from collections import Counter
from .overpass import oql_from_tag

import subprocess
import os.path
import re
import shutil

overpass_types = {'way': 'way', 'relation': 'rel', 'node': 'node'}

skip_tags = {'route:road',
             'highway=primary',
             'highway=road',
             'highway=service',
             'highway=motorway',
             'highway=trunk',
             'highway=unclassified',
             'highway',
             'tunnel',
             'name',
             'tunnel'
             'website',
             'type=waterway',
             'waterway=river'
             'addr:street',
             'type=associatedStreet'}

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

    area = column_property(func.ST_Area(geom))
    # match_ratio = column_property(candidate_count / item_count)

    items = relationship('Item',
                         secondary='place_item',
                         lazy='dynamic',
                         backref=backref('places', lazy='dynamic'))

    @classmethod
    def get_by_osm(cls, osm_type, osm_id):
        return cls.query.filter_by(osm_type=osm_type, osm_id=osm_id).one_or_none()

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
        if address and address.get('country_code') == 'us':
            state = address.get('state')
            if state and n != state:
                return n + ', ' + state

        country = address.get('country')
        if country and self.name != country:
            return '{} ({})'.format(self.name, country)
        return self.name

    @property
    def name_for_change_comment(self):
        address = self.address
        n = self.name
        if address and address.get('country_code') == 'us':
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

        q = wikidata.get_item_tag_query(*bbox)
        rows = wikidata.run_query(q)
        wikidata.parse_item_tag_query(rows, items)

        return {k: v
                for k, v in items.items()
                if self.osm_type == 'node' or self.covers(v)}

    def covers(self, item):
        return object_session(self).scalar(
                select([func.ST_Covers(Place.geom, item['location'])]).where(Place.place_id == self.place_id))

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

    @property
    def overpass_backup(self):
        overpass_dir = current_app.config['OVERPASS_DIR']
        return os.path.join(overpass_dir, 'backup', '{}.xml'.format(self.place_id))

    def move_overpass_to_backup(self):
        filename = self.overpass_filename
        if not os.path.exists(filename):
            return
        shutil.move(filename, self.overpass_backup)

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

    def load_into_pgsql(self, capture_stderr=True):
        if not os.path.exists(self.overpass_filename):
            return 'no data from overpass to load with osm2pgsql'

        if os.stat(self.overpass_filename).st_size == 0:
            return 'no data from overpass to load with osm2pgsql'

        cmd = ['osm2pgsql', '--create', '--drop', '--slim',
                '--hstore-all', '--hstore-add-index',
                '--prefix', self.prefix,
                '--cache', '1000',
                '--multi-geometry',
                '--host', current_app.config['DB_HOST'],
                '--username', current_app.config['DB_USER'],
                '--database', current_app.config['DB_NAME'],
                self.overpass_filename]

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

        large_area = self.area > 3000 * 1000 * 1000

        union = ['{}({});'.format(self.overpass_type, self.osm_id)]

        for tag in self.all_tags:
            u = (oql_from_tag(tag, large_area, filters=self.overpass_filter)
                 if self.osm_type == 'node'
                 else oql_from_tag(tag, large_area))
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

    def matcher_progress_url(self):
        return url_for('matcher.matcher_progress',
                       osm_type=self.osm_type,
                       osm_id=self.osm_id)

    def item_list(self):
        lang = self.most_common_language() or 'en'
        q = self.items.filter(Item.entity.isnot(None)).order_by(Item.item_id)
        return [{'id': i.item_id, 'name': i.label(lang=lang)}
                for i in q]

    def load_items(self, bbox=None, debug=False):
        if bbox is None:
            bbox = self.bbox

        items = self.items_from_wikidata(bbox)
        if debug:
            print('{:d} items'.format(len(items)))

        enwiki_to_item = {v['enwiki']: v for v in items.values() if 'enwiki' in v}

        for title, cats in wikipedia.page_category_iter(enwiki_to_item.keys()):
            enwiki_to_item[title]['categories'] = cats

        seen = set()
        for qid, v in items.items():
            wikidata_id = qid[1:]
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
            if 'building' in tags and len(tags) > 1:
                tags.remove('building')

            item.tags = tags
            seen.add(int(item.item_id))

            existing = PlaceItem.query.filter_by(item=item, place=self).one_or_none()
            if not existing:
                place_item = PlaceItem(item=item, place=self)
                session.add(place_item)

        for item in self.items:
            if int(item.item_id) not in seen:
                link = PlaceItem.query.filter_by(item=item, place=self).one()
                session.delete(link)
        session.commit()

    def load_extracts(self, debug=False):
        by_title = {item.enwiki: item for item in self.items if item.enwiki}

        for title, extract in wikipedia.get_extracts(by_title.keys()):
            item = by_title[title]
            if debug:
                print(title)
            item.extract = extract
            item.extract_names = wikipedia.html_names(extract)

    def wbgetentities(self, debug=False):
        sub = (session.query(Item.item_id)
                      .join(ItemTag)
                      .group_by(Item.item_id)
                      .subquery())
        q = self.items.filter(Item.item_id == sub.c.item_id)

        if debug:
            print('running wbgetentities query')
        items = {i.qid: i for i in q}
        if debug:
            print('{} items'.format(len(items)))

        for qid, entity in wikidata.entity_iter(items.keys()):
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

    def run_matcher(self, debug=False):
        conn = session.bind.raw_connection()
        cur = conn.cursor()

        items = self.items.filter(Item.entity.isnot(None)).order_by(Item.item_id)
        if debug:
            print(items.count())
        for item in items:
            candidates = matcher.find_item_matches(cur, item, self.prefix, debug=False)
            if debug:
                print(len(candidates), item.label())
            as_set = {(i['osm_type'], i['osm_id']) for i in candidates}
            for c in item.candidates[:]:
                if (c.osm_type, c.osm_id) not in as_set:
                    c.bad_matches.delete()
                    session.delete(c)
                    session.commit()

            if not candidates:
                continue

            for i in candidates:
                c = ItemCandidate.query.get((item.item_id, i['osm_id'], i['osm_type']))
                if not c:
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
            self.load_items()
            self.state = 'wikipedia'

        if self.state == 'wikipedia':
            print('add tags')
            self.add_tags_to_items()
            self.state = 'tags'
            session.commit()

        if self.state == 'tags':
            print('wbgetentities')
            self.wbgetentities(debug=debug)
            print('load extracts')
            self.load_extracts(debug=debug)
            self.state = 'wbgetentities'
            session.commit()

        if self.state == 'wbgetentities':
            oql = self.get_oql()
            if self.area_in_sq_km < 1000:
                r = overpass.run_query_persistent(oql)
                assert r
                self.save_overpass(r.content)
            else:
                self.chunk()
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

    def chunk4(self):
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

    def chunk9(self):
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

    def chunk(self):
        chunks = self.chunk4() if self.area_in_sq_km < 10000 else self.chunk9()

        files = []

        for chunk in chunks:
            bbox = chunk['bbox']
            quad = ''.join(i[0] for i in chunk['name'].strip().split())
            ymin, ymax, xmin, xmax = bbox
            q = self.items
            q = q.filter(cast(Item.location, Geometry).contained(func.ST_MakeEnvelope(xmin, ymin, xmax, ymax)))
            # place.load_items(bbox=chunk['bbox'], debug=True)

            tags = set()
            for item in q:
                tags |= set(item.tags)
            tags.difference_update(skip_tags)
            tags = matcher.simplify_tags(tags)
            filename = '{}_{}.xml'.format(self.place_id, quad)
            print(chunk['name'], q.count(), quad, len(tags), filename)
            full = os.path.join('overpass', filename)
            files.append(full)
            if os.path.exists(full):
                continue

            oql_bbox = '{:f},{:f},{:f},{:f}'.format(ymin, xmin, ymax, xmax)

            oql = overpass.oql_for_area(self.overpass_type,
                                        self.osm_id,
                                        tags,
                                        oql_bbox, None)
            # print(oql)

            r = overpass.run_query_persistent(oql, attempts=3)
            open(full, 'wb').write(r.content)

        cmd = ['osmium', 'merge'] + files + ['-o', self.overpass_filename]
        print(' '.join(cmd))
        subprocess.run(cmd)

def get_top_existing(limit=30):
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
