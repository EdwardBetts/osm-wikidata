# coding: utf-8
from flask import current_app, url_for, g
from sqlalchemy import ForeignKey, Column, func, select
from sqlalchemy.types import BigInteger, Float, Integer, JSON, String, Enum
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geography  # noqa: F401
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship, backref, column_property, object_session
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql.expression import cast
from .database import session
from . import wikidata, matcher

import subprocess
import os.path
import re

Base = declarative_base()
Base.query = session.query_property()

# states: wikipedia, tags, wbgetentities, overpass, postgis, osm2pgsql, ready
# bad state: overpass_fail

name_only_tag = {'area=yes', 'type=tunnel', 'leisure=park', 'leisure=garden',
        'site=aerodome', 'amenity=hospital', 'boundary', 'amenity=pub',
        'amenity=cinema', 'ruins', 'retail=retail_park',
        'amenity=concert_hall', 'amenity=theatre'}

name_only_key = ['place', 'landuse', 'admin_level', 'water', 'man_made',
        'railway', 'aeroway', 'bridge', 'natural']

def oql_from_tag(tag, large_area, filters='area.a'):
    # optimisation: we only expect route, type or site on relations
    if tag == 'highway':
        return
    relation_only = tag == 'site'
    if large_area or tag in name_only_tag or any(tag.startswith(k) for k in name_only_key):
        name_filter = '[name]'
    else:
        name_filter = '[~"^(addr:housenumber|.*name.*)$"~".",i]'
    if '=' in tag:
        k, _, v = tag.partition('=')
        if tag == 'type=waterway' or k == 'route' or tag == 'type=route':
            return  # ignore because osm2pgsql only does multipolygons
        if k in {'site', 'type', 'route'}:
            relation_only = True
        if ':' in tag or ' ' in tag:
            tag = '"{}"="{}"'.format(k, v)

    return ['\n    {}({})[{}]{};'.format(t, filters, tag, name_filter)
            for t in (('rel',) if relation_only else ('node', 'way', 'rel'))]

    # return ['\n    {}(area.a)[{}]{};'.format(t, tag, name_filter) for ('node', 'way', 'rel')]

class Place(Base):   # assume all places are relations
    __tablename__ = 'place'

    osm_id = Column(BigInteger, primary_key=True, autoincrement=False)
    display_name = Column(String, nullable=False)
    category = Column(String, nullable=False)
    type = Column(String, nullable=False)
    place_id = Column(BigInteger, nullable=False)
    place_rank = Column(Integer, nullable=False)
    icon = Column(String)
    geom = Column(Geography(spatial_index=True))
    south = Column(Float, nullable=False)
    west = Column(Float, nullable=False)
    north = Column(Float, nullable=False)
    east = Column(Float, nullable=False)
    extratags = Column(JSON)
    address = Column(JSON)
    namedetails = Column(JSON)
    item_count = Column(Integer)
    candidate_count = Column(Integer)
    state = Column(String, index=True)

    area = column_property(func.ST_Area(geom))
    # match_ratio = column_property(candidate_count / item_count)

    items = relationship('Item',
                         secondary='place_item',
                         lazy='dynamic',
                         backref=backref('places', lazy='dynamic'))

    @property
    def osm_type(self):
        return 'relation'

    @hybrid_property
    def area_in_sq_km(self):
        return self.area / (1000 * 1000)

    @classmethod
    def from_nominatim(cls, hit):
        if hit.get('osm_type') != 'relation':
            return
        keys = ('osm_id', 'display_name', 'category', 'type', 'place_id', 'place_rank',
                'icon', 'extratags', 'address', 'namedetails')
        n = {k: hit[k] for k in keys if k in hit}
        bbox = hit['boundingbox']
        (n['south'], n['north'], n['west'], n['east']) = bbox
        n['geom'] = hit['geotext']
        return cls(**n)

    @property
    def match_ratio(self):
        if self.state == 'ready' and self.item_count:
            return self.candidate_count / self.item_count

    @property
    def bbox(self):
        return (self.south, self.north, self.west, self.east)

    @property
    def display_area(self):
        return '{:.1f} km²'.format(self.area_in_sq_km)

    def get_wikidata_query(self):
        return wikidata.get_query(*self.bbox)

    def items_from_wikidata(self):
        r = wikidata.run_query(*self.bbox)
        results = wikidata.parse_query(r.json()['results']['bindings'])
        return [item for item in results if self.covers(item)]

    def covers(self, item):
        return object_session(self).scalar(
                select([func.ST_Covers(Place.geom, item['location'])]).where(Place.osm_id == self.osm_id))

    def add_tags_to_items(self):
        cat_to_entity = matcher.build_cat_map()
        for item in self.items.filter(Item.categories != '{}'):
            tags = set()
            for cat in item.categories:
                lc_cat = cat.lower()
                for key, value in cat_to_entity.items():
                    pattern = re.compile(r'\b' + re.escape(key) + r'\b', re.I)
                    if not pattern.search(lc_cat):
                        continue
                    exclude = value.get('exclude_cats')
                    if exclude:
                        pattern = re.compile(r'\b(' + '|'.join(re.escape(e) for e in exclude) + r')\b', re.I)
                        if pattern.search(lc_cat):
                            continue
                    tags |= set(value['tags'])
            item.tags = sorted(tags)
            session.add(item)
        self.state = 'tags'
        session.commit()

    @property
    def prefix(self):
        return 'osm_{}'.format(self.osm_id)

    @property
    def overpass_filename(self):
        overpass_dir = current_app.config['OVERPASS_DIR']
        return os.path.join(overpass_dir, '{}.xml'.format(self.osm_id))

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
                       .filter(Place.osm_id == self.osm_id)
                       .group_by(Item.item_id)
                       .count())

    def items_without_candidates(self):
        return self.items.outerjoin(ItemCandidate).filter(ItemCandidate.osm_id.is_(None))

    def items_with_multiple_candidates(self):
        # select count(*) from (select 1 from item, item_candidate where item.item_id=item_candidate.item_id) x;
        q = (self.items.join(ItemCandidate)
                 .group_by(Item.item_id)
                 .having(func.count(Item.item_id) > 1)
                 .with_entities(Item.item_id))
        return q

    @property
    def name(self):
        return self.namedetails.get('name:en') or self.namedetails['name']

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
                return p.stderr

    def save_overpass(self, content):
        with open(self.overpass_filename, 'wb') as out:
            out.write(content)

    @property
    def all_tags(self):
        tags = set()
        for item in self.items.filter(Item.tags != '{}'):
            tags |= set(item.tags)
        return matcher.simplify_tags(tags)

    def get_oql(self):
        large_area = self.area > 3000 * 1000 * 1000

        q = self.items.filter(Item.entity.isnot(None)).order_by(Item.item_id)

        if False and large_area and q.count() < 200:
            union = []
            for item in q:
                union += item.get_oql()

            oql = ('[timeout:300][out:xml];\n' +
                   '({});\n' +
                   '(._;>;);\n' +
                   'out qt;').format(''.join(union))
        else:
            bbox = '{:f},{:f},{:f},{:f}'.format(self.south, self.west, self.north, self.east)
            union = ['rel({});'.format(self.osm_id)]
            for tag in self.all_tags:
                u = oql_from_tag(tag, large_area)
                if u:
                    union += u

            area_id = 3600000000 + int(self.osm_id)
            oql = ('[timeout:300][out:xml][bbox:{}];\n' +
                   'area({})->.a;\n' +
                   '({});\n' +
                   '(._;>;);\n' +
                   'out qt;').format(bbox, area_id, ''.join(union))
        return oql

    def candidates_url(self, **kwargs):
        if g.get('filter'):
            return url_for('candidates_with_filter',
                           name_filter=g.filter,
                           osm_id=self.osm_id, **kwargs)
        else:
            return url_for('candidates', osm_id=self.osm_id, **kwargs)

    def matcher_progress_url(self):
        if g.get('filter'):
            return url_for('matcher_progress_with_filter',
                           name_filter=g.filter,
                           osm_id=self.osm_id)
        else:
            return url_for('matcher_progress', osm_id=self.osm_id)

    def item_list(self):
        q = self.items.filter(Item.entity.isnot(None)).order_by(Item.item_id)
        return [{'id': i.item_id, 'name': i.enwiki} for i in q]

class Item(Base):
    __tablename__ = 'item'

    item_id = Column(Integer, primary_key=True)
    location = Column(Geography('POINT', spatial_index=True), nullable=False)
    enwiki = Column(String, nullable=False)
    entity = Column(JSON)
    categories = Column(postgresql.ARRAY(String))
    tags = Column(postgresql.ARRAY(String))
    qid = column_property('Q' + cast(item_id, String))
    ewkt = column_property(func.ST_AsEWKT(location), deferred=True)

    @property
    def wikidata_uri(self):
        return 'https://www.wikidata.org/wiki/Q{}'.format(self.item_id)

    def get_osm_url(self, zoom=18):
        lat, lon = session.query(func.ST_Y(self.location), func.ST_X(self.location)).one()
        params = (zoom, lat, lon)
        return 'https://www.openstreetmap.org/#map={}/{}/{}'.format(*params)

    @property
    def hstore_query(self):
        if not self.tags:
            return
        cond = ("((tags->'{}') = '{}')".format(*tag.split('='))
                if '=' in tag
                else "(tags ? '{}')".format(tag) for tag in self.tags)
        return ' or '.join(cond)

    def instanceof(self):
        if self.entity:
            return [i['mainsnak']['datavalue']['value']['numeric-id']
                    for i in self.entity['claims'].get('P31', [])]

    def names(self):
        if self.entity:
            return wikidata.names_from_entity(self.entity)

    def get_oql(self):
        lat, lon = session.query(func.ST_Y(self.location), func.ST_X(self.location)).one()
        union = []
        for tag in self.tags:
            osm_filter = 'around:1000,{:f},{:f}'.format(lat, lon)
            union += oql_from_tag(tag, False, osm_filter)
        return union

class PlaceItem(Base):
    __tablename__ = 'place_item'

    item_id = Column(Integer, ForeignKey('item.item_id'), primary_key=True)
    osm_id = Column(BigInteger, ForeignKey('place.osm_id'), primary_key=True)

    item = relationship('Item')
    place = relationship('Place')

class ItemCandidate(Base):
    __tablename__ = 'item_candidate'

    item_id = Column(Integer, ForeignKey('item.item_id'), primary_key=True)
    osm_id = Column(BigInteger, primary_key=True)
    osm_type = Column(Enum('node', 'way', 'relation', name='osm_type'), primary_key=True)
    name = Column(String)
    dist = Column(Float)
    tags = Column(JSON)
    planet_table = Column(String)
    src_id = Column(BigInteger)

    item = relationship('Item', backref=backref('candidates', lazy='dynamic'))
