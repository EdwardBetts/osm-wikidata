# coding: utf-8
from flask import current_app, url_for, g
from sqlalchemy import func, select
from sqlalchemy.schema import ForeignKeyConstraint, ForeignKey, Column
from sqlalchemy.types import BigInteger, Float, Integer, JSON, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from geoalchemy2 import Geography  # noqa: F401
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship, backref, column_property, object_session, deferred
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.sql.expression import cast
from collections import Counter
from .database import session
from flask_login import UserMixin
from . import wikidata, matcher, match, wikipedia
from .overpass import oql_from_tag, oql_for_area

import subprocess
import os.path
import re

Base = declarative_base()
Base.query = session.query_property()

osm_api_base = 'https://api.openstreetmap.org/api/0.6'

osm_type_enum = postgresql.ENUM('node', 'way', 'relation',
                                name='osm_type_enum',
                                metadata=Base.metadata)

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

class User(Base, UserMixin):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    username = Column(String)
    password = Column(String)
    name = Column(String)
    email = Column(String)
    active = Column(Boolean, default=True)
    sign_up = Column(DateTime, default=func.now())
    is_admin = Column(Boolean, default=False)
    description = Column(Text)
    img = Column(String)
    languages = Column(postgresql.ARRAY(String))

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

    @hybrid_property
    def area_in_sq_km(self):
        return self.area / (1000 * 1000)

    def update_from_nominatim(self, hit):
        keys = ('display_name', 'category', 'type',
                'place_rank', 'icon', 'extratags', 'address', 'namedetails')
        for n in keys:
            setattr(self, n, hit.get(n))

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
        keys = ('osm_id', 'osm_type', 'display_name', 'category', 'type',
                'place_id', 'place_rank', 'icon', 'extratags', 'address',
                'namedetails', 'lat', 'lon')
        n = {k: hit[k] for k in keys if k in hit}
        if hit['osm_type'] == 'node':
            n['radius'] = 1000   # 1km
        bbox = hit['boundingbox']
        (n['south'], n['north'], n['west'], n['east']) = bbox
        n['geom'] = hit['geotext']
        return cls(**n)

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

        return oql_for_area(self.overpass_type,
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
        if g.get('filter'):
            return url_for('matcher_progress_with_filter',
                           name_filter=g.filter,
                           osm_id=self.osm_id)
        else:
            return url_for('matcher_progress', osm_id=self.osm_id)

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

        for qid, v in items.items():
            wikidata_id = qid[1:]
            item = Item.query.get(wikidata_id)
            if not item:
                item = Item(item_id=wikidata_id, location=v['location'])
                session.add(item)
            for k in 'enwiki', 'categories', 'query_label':
                if k in v:
                    setattr(item, k, v[k])

            tags = set(v['tags'])
            if 'building' in tags and len(tags) > 1:
                tags.remove('building')

            item.tags = tags

            place_item = PlaceItem(item=item, place=self)
            session.merge(place_item)
        session.commit()

    def load_extracts(self):
        by_title = {item.enwiki: item for item in self.items if item.enwiki}

        for title, extract in wikipedia.get_extracts(by_title.keys()):
            item = by_title[title]
            item.extract = extract
            item.extract_names = wikipedia.html_names(extract)

    def wbgetentities(self):
        sub = (session.query(Item.item_id)
                      .join(ItemTag)
                      .group_by(Item.item_id)
                      .subquery())
        q = self.items.filter(Item.item_id == sub.c.item_id)

        items = {i.qid: i for i in q}

        for qid, entity in wikidata.entity_iter(items.keys()):
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

class Item(Base):
    __tablename__ = 'item'

    item_id = Column(Integer, primary_key=True)
    location = Column(Geography('POINT', spatial_index=True), nullable=False)
    enwiki = Column(String)
    entity = Column(JSON)
    categories = Column(postgresql.ARRAY(String))
    old_tags = Column(postgresql.ARRAY(String))
    qid = column_property('Q' + cast(item_id, String))
    ewkt = column_property(func.ST_AsEWKT(location), deferred=True)
    query_label = Column(String)
    extract = Column(String)
    extract_names = Column(postgresql.ARRAY(String))

    db_tags = relationship('ItemTag',
                           collection_class=set,
                           cascade='save-update, merge, delete, delete-orphan',
                           backref='item')

    tags = association_proxy('db_tags', 'tag_or_key')

    def label(self, lang='en'):
        if not self.entity:
            return self.enwiki or self.query_label or None

        labels = self.entity['labels']
        if lang in labels:
            return labels[lang]['value']
        elif lang != 'en' and 'en' in labels:
            return labels['en']['value']
        elif labels:
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
        d = wikidata.names_from_entity(self.entity) or defaultdict(list)
        for name in self.extract_names or []:
            d[name].append(('extract', 'enwiki'))
        return d or None

    def get_oql(self):
        lat, lon = session.query(func.ST_Y(self.location), func.ST_X(self.location)).one()
        union = []
        for tag in self.tags:
            osm_filter = 'around:1000,{:f},{:f}'.format(lat, lon)
            union += oql_from_tag(tag, False, osm_filter)
        return union

    def coords(self):
        return session.query(func.ST_Y(self.location), func.ST_X(self.location)).one()

    def image_filenames(self):
        return [i['mainsnak']['datavalue']['value']
                for i in self.entity['claims'].get('P18', [])]

    def defunct_cats(self):
        words = {'demolish', 'disestablishment', 'defunct', 'abandon', 'decommission', 'former', 'dismantled',
                 'disused', 'disassembled', 'abandoned', 'disband', 'scrapped', 'unused', 'closed', 'condemned',
                 'mothballed'}

        exclude = {'Defunct baseball venues in the United States', 'Defunct National Football League venues'}

        found = []
        for item_cat in self.categories or []:
            if item_cat in exclude:
                continue
            lc_item_cat = item_cat.lower()
            found += [item_cat for i in words if i in lc_item_cat]
        return found

class ItemTag(Base):
    __tablename__ = 'item_tag'

    item_id = Column(Integer, ForeignKey('item.item_id'), primary_key=True)
    tag_or_key = Column(String, primary_key=True, index=True)

    def __init__(self, tag_or_key):
        self.tag_or_key = tag_or_key

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

    item = relationship('Item', backref=backref('candidates',
                                                lazy='dynamic',
                                                cascade='save-update, merge, delete, delete-orphan'))

    @property
    def key(self):
        return '{0.osm_type:s}_{0.osm_id:d}'.format(self)

    def get_match(self):
        endings = matcher.get_ending_from_criteria(self.tags)
        wikidata_names = self.item.names()
        return match.check_for_match(self.tags, wikidata_names, endings)

    def get_all_matches(self):
        endings = matcher.get_ending_from_criteria(self.item.tags)
        wikidata_names = self.item.names()
        m = match.get_all_matches(self.tags, wikidata_names, endings)
        return m

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

    @property
    def wikidata_tag(self):
        return self.tags.get('wikidata') or None

    @property
    def label(self):
        if 'name' in self.tags:
            name = self.tags['name']
            if 'addr:housename' in self.tags:
                return '{} (house name: {})'.format(name, self.tags['addr:housename'])
            else:
                return name

        if 'name:en' in self.tags:
            return self.tags['name:en']
        for k, v in self.tags.items():
            if k.startswith('name:'):
                return v
        for k, v in self.tags.items():
            if 'name' in k:
                return v
        return '{}/{}'.format(self.osm_type, self.osm_id)

    @property
    def url(self):
        return '{}/{}/{}'.format(osm_api_base, self.osm_type, self.osm_id)

# class ItemCandidateTag(Base):
#     __tablename__ = 'item_candidate_tag'
#     __table_args__ = (
#         ForeignKeyConstraint(['item_id', 'osm_id', 'osm_type'],
#                              [ItemCandidate.item_id,
#                               ItemCandidate.osm_id,
#                               ItemCandidate.osm_type]),
#     )
#
#     item_id = Column(Integer, primary_key=True)
#     osm_id = Column(BigInteger, primary_key=True)
#     osm_type = Column(osm_type_enum, primary_key=True)
#     k = Column(String, primary_key=True)
#     v = Column(String, primary_key=True)
#
#     item_candidate = relationship(ItemCandidate,
#                                   backref=backref('tag_table', lazy='dynamic'))

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

    user = relationship('User', backref=backref('changesets', lazy='dynamic'))
    place = relationship('Place', backref=backref('changesets', lazy='dynamic'))

    @property
    def item_label(self):
        item = Item.query.get(self.item_id)
        if item:
            return item.label()

class ChangesetEdit(Base):
    __tablename__ = 'changeset_edit'
    __table_args__ = (
        ForeignKeyConstraint(['item_id', 'osm_id', 'osm_type'],
                             [ItemCandidate.item_id,
                              ItemCandidate.osm_id,
                              ItemCandidate.osm_type]),
    )

    changeset_id = Column(BigInteger,
                          ForeignKey('changeset.id'),
                          primary_key=True)
    item_id = Column(Integer, primary_key=True)
    osm_id = Column(BigInteger, primary_key=True)
    osm_type = Column(osm_type_enum, primary_key=True)

    changeset = relationship('Changeset',
                             backref=backref('matches', lazy='dynamic'))

class BadMatch(Base):
    __tablename__ = 'bad_match'
    __table_args__ = (
        ForeignKeyConstraint(['item_id', 'osm_id', 'osm_type'],
                             [ItemCandidate.item_id,
                              ItemCandidate.osm_id,
                              ItemCandidate.osm_type]),
    )

    item_id = Column(Integer, primary_key=True)
    osm_id = Column(BigInteger, primary_key=True)
    osm_type = Column(osm_type_enum, primary_key=True)
    user_id = Column(Integer, ForeignKey(User.id), primary_key=True)
    created = Column(DateTime, default=func.now())
    comment = Column(Text)

    item_candidate = relationship(ItemCandidate,
                                  backref=backref('bad_matches', lazy='dynamic'))
    user = relationship(User, backref=backref('bad_matches', lazy='dynamic'))

class Timing(Base):
    __tablename__ = 'timing'
    id = Column(Integer, primary_key=True)
    start = Column(Float, nullable=False)
    path = Column(String, nullable=False)
    name = Column(String, nullable=False)
    seconds = Column(Float, nullable=False)
