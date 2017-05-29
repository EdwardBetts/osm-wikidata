# coding: utf-8
from flask import current_app, url_for, g
from sqlalchemy import ForeignKey, Column, func, select
from sqlalchemy.types import BigInteger, Float, Integer, JSON, String, Enum, Boolean, DateTime
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geography  # noqa: F401
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship, backref, column_property, object_session
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql.expression import cast
from .database import session
from flask_login import UserMixin
from . import wikidata, matcher, match, wikipedia
from .overpass import oql_from_tag

import subprocess
import os.path
import re

Base = declarative_base()
Base.query = session.query_property()

osm_type_enum = postgresql.ENUM('node', 'way', 'relation',
                                name='osm_type_enum',
                                metadata=Base.metadata)

overpass_types = {'way': 'way', 'relation': 'rel', 'node': 'node'}

class User(Base, UserMixin):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    username = Column(String)
    password = Column(String)
    name = Column(String)
    email = Column(String)
    active = Column(Boolean, default=True)

    def is_active(self):
        return self.active

# states: wikipedia, tags, wbgetentities, overpass, postgis, osm2pgsql, ready
# bad state: overpass_fail

class Place(Base):   # assume all places are relations
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
    extratags = Column(JSON)
    address = Column(JSON)
    namedetails = Column(JSON)
    item_count = Column(Integer)
    candidate_count = Column(Integer)
    state = Column(String, index=True)
    override_name = Column(String)
    lat = Column(Float)
    lon = Column(Float)

    area = column_property(func.ST_Area(geom))
    # match_ratio = column_property(candidate_count / item_count)

    items = relationship('Item',
                         secondary='place_item',
                         lazy='dynamic',
                         backref=backref('places', lazy='dynamic'))

    @hybrid_property
    def area_in_sq_km(self):
        return self.area / (1000 * 1000)

    @classmethod
    def from_nominatim(cls, hit):
        keys = ('osm_id', 'osm_type', 'display_name', 'category', 'type',
                'place_id', 'place_rank', 'icon', 'extratags', 'address',
                'namedetails', 'lat', 'lon')
        n = {k: hit[k] for k in keys if k in hit}
        if hit['osm_type'] == 'node':
            n['radius'] = 5000   # 5km
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
        if self.osm_type == 'node':
            query = wikidata.get_point_query(self.lat, self.lon, self.radius)
        else:
            query = wikidata.get_enwiki_query(*self.bbox)
        return query

    def items_from_wikidata(self):
        q = wikidata.get_enwiki_query(*self.bbox)
        rows = wikidata.run_query(q)

        items = wikidata.parse_enwiki_query(rows)
        q = wikidata.get_item_tag_query(*self.bbox)
        rows = wikidata.run_query(q)
        wikidata.parse_item_tag_query(rows, items)

        print(len(items))

        return {k: v for k, v in items.items() if self.osm_type == 'node' or self.covers(v)}

    def covers(self, item):
        return object_session(self).scalar(
                select([func.ST_Covers(Place.geom, item['location'])]).where(Place.place_id == self.place_id))

    def add_tags_to_items(self):
        for item in self.items.filter(Item.categories != '{}'):
            item.tags = set(item.tags) | set(matcher.categories_to_tags(item.categories))

    @property
    def prefix(self):
        return 'osm_{}'.format(self.place_id)

    @property
    def overpass_filename(self):
        overpass_dir = current_app.config['OVERPASS_DIR']
        return os.path.join(overpass_dir, '{}.xml'.format(self.place_id))

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

        name = self.namedetails.get('name:en') or self.namedetails['name']
        display = self.display_name

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

    @property
    def overpass_type(self):
        return overpass_types[self.osm_type]

    @property
    def overpass_filter(self):
        return 'around:{0.radius},{0.lat},{0.lon}'.format(self)

    def get_oql(self):
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
        if g.get('filter'):
            return url_for('matcher_progress_with_filter',
                           name_filter=g.filter,
                           osm_id=self.osm_id)
        else:
            return url_for('matcher_progress', osm_id=self.osm_id)

    def item_list(self):
        q = self.items.filter(Item.entity.isnot(None)).order_by(Item.item_id)
        return [{'id': i.item_id, 'name': i.label} for i in q]

    def load_items(self):
        items = self.items_from_wikidata()
        print(len(items))

        enwiki_to_item = {v['enwiki']: v for v in items.values() if 'enwiki' in v}

        for title, cats in wikipedia.page_category_iter(enwiki_to_item.keys()):
            enwiki_to_item[title]['categories'] = cats

        for qid, v in items.items():
            print(qid, v['label'])
            wikidata_id = qid[1:]
            item = Item.query.get(wikidata_id)
            if not item:
                item = Item(item_id=wikidata_id, location=v['location'])
                session.add(item)
            for k in 'enwiki', 'categories', 'tags':
                if k in v:
                    setattr(item, k, v[k])
            if not item.places.filter(Place.place_id == self.place_id).count():
                print('append')
                item.places.append(self)
        session.commit()

    def wbgetentities(self):
        q = self.items.filter(Item.tags != '{}')
        items = {i.qid: i for i in q}

        for qid, entity in wikidata.entity_iter(items.keys()):
            items[qid].entity = entity
        session.commit()

class Item(Base):
    __tablename__ = 'item'

    item_id = Column(Integer, primary_key=True)
    location = Column(Geography('POINT', spatial_index=True), nullable=False)
    enwiki = Column(String)
    entity = Column(JSON)
    categories = Column(postgresql.ARRAY(String))
    tags = Column(postgresql.ARRAY(String))
    qid = column_property('Q' + cast(item_id, String))
    ewkt = column_property(func.ST_AsEWKT(location), deferred=True)

    @property
    def label(self):
        labels = self.entity['labels']
        if 'en' in labels:
            return labels['en']['value']
        else:
            return list(labels.values())[0]['value']

    @property
    def wikidata_uri(self):
        return 'https://www.wikidata.org/wiki/Q{}'.format(self.item_id)

    def get_osm_url(self, zoom=18):
        lat, lon = session.query(func.ST_Y(self.location), func.ST_X(self.location)).one()
        params = (zoom, lat, lon)
        return 'https://www.openstreetmap.org/#map={}/{}/{}'.format(*params)

    @property
    def hstore_query(self):
        '''hstore query for use with osm2pgsql database'''
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
    place_id = Column(BigInteger, ForeignKey('place.place_id'), primary_key=True)

    item = relationship('Item')
    place = relationship('Place')

class ItemCandidate(Base):
    __tablename__ = 'item_candidate'

    item_id = Column(Integer, ForeignKey('item.item_id'), primary_key=True)
    osm_id = Column(BigInteger, primary_key=True)
    osm_type = Column(osm_type_enum, primary_key=True)
    name = Column(String)
    dist = Column(Float)
    tags = Column(JSON)
    planet_table = Column(String)
    src_id = Column(BigInteger)

    item = relationship('Item', backref=backref('candidates', lazy='dynamic'))

    @property
    def key(self):
        return '{0.osm_type:s}_{0.osm_id:d}'.format(self)

    def get_match(self):
        endings = matcher.get_ending_from_criteria(set(self.tags))
        wikidata_names = self.item.names()
        return match.check_for_match(self.tags, wikidata_names, endings)

    def matching_tags(self):
        tags = []

        for tag_or_key in self.item.tags:
            if '=' not in tag_or_key and tag_or_key in self.tags:
                tags.append(tag_or_key)
                continue
            key, _, value = tag_or_key.partition('=')
            if self.tags.get(key) == value:
                tags.append(tag_or_key)
                continue

        return tags

class TagOrKey(Base):
    __tablename__ = 'tag_or_key'

    name = Column(String, primary_key=True)
    count_all = Column(Integer)

class Category(Base):
    __tablename__ = 'category'

    name = Column(String, primary_key=True)
    page_count = Column(Integer)

class Changeset(Base):
    __tablename__ = 'changeset'
    id = Column(BigInteger, primary_key=True)
    created = Column(DateTime)
    place_id = Column(BigInteger, ForeignKey(Place.place_id))
    item_id = Column(Integer)
    comment = Column(String)
    user_id = Column(Integer, ForeignKey(User.id))
    update_count = Column(Integer, nullable=False)

    user = relationship(User, backref=backref('changesets', lazy='dynamic'))
    place = relationship('Place')

    @property
    def item_label(self):
        item = Item.query.get(self.item_id)
        if item:
            return item.label
