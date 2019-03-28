# coding: utf-8
from flask import g, has_app_context
from sqlalchemy import func
from sqlalchemy.schema import ForeignKeyConstraint, ForeignKey, Column
from sqlalchemy.types import BigInteger, Float, Integer, String, Boolean, DateTime, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm.collections import attribute_mapped_collection
from geoalchemy2 import Geography  # noqa: F401
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship, backref, column_property
from sqlalchemy.sql.expression import cast
from sqlalchemy.orm.collections import attribute_mapped_collection
from .database import session, now_utc
from flask_login import UserMixin
from . import wikidata, matcher, match, wikipedia, country_units, utils, mail
from .overpass import oql_from_tag
from .utils import capfirst
from collections import defaultdict

import re

re_lau_code = re.compile(r'^[A-Z]{2}([^A-Z].+)$')

Base = declarative_base()
Base.query = session.query_property()

osm_api_base = 'https://api.openstreetmap.org/api/0.6'

osm_type_enum = postgresql.ENUM('node', 'way', 'relation',
                                name='osm_type_enum',
                                metadata=Base.metadata)

# also check for tags that start with 'disused:'
disused_prefix_key = {'amenity', 'railway', 'leisure', 'tourism',
                      'man_made', 'shop', 'building'}

class User(Base, UserMixin):
    __tablename__ = 'user'
    id = Column(Integer, primary_key=True)
    username = Column(String)
    password = Column(String)
    name = Column(String)
    email = Column(String)
    active = Column(Boolean, default=True)
    sign_up = Column(DateTime, default=now_utc())
    is_admin = Column(Boolean, default=False)
    description = Column(Text)
    img = Column(String)
    languages = Column(postgresql.ARRAY(String))
    single = Column(String)
    multi = Column(String)
    units = Column(String)
    wikipedia_tag = Column(Boolean, default=False)

    def is_active(self):
        return self.active

# states: wikipedia, tags, wbgetentities, overpass, postgis, osm2pgsql, ready
# bad state: overpass_fail

class IsA(Base):
    __tablename__ = 'isa'
    item_id = Column(Integer, primary_key=True, autoincrement=False)
    entity = Column(postgresql.JSON)
    qid = column_property('Q' + cast(item_id, String))
    label = Column(String)

    def url(self):
        return f'https://www.wikidata.org/wiki/Q{self.item_id}'

    def entity_label(self, lang='en'):
        labels = self.entity['labels']
        if lang in labels:
            return labels[lang]['value']
        elif lang != 'en' and 'en' in labels:
            return labels['en']['value']
        elif labels:
            return list(labels.values())[0]['value']

    def label_and_qid(self):
        if self.entity and 'labels' not in self.entity:
            subject = f'missing labels: {self.qid}'
            body = f'Wikidata entity is missing labels\n\n{self.url}'
            mail.send_mail(subject, body)
            return self.qid
        else:
            return f'{self.entity_label()} ({self.qid})'

    def labels(self):
        return self.entity['labels']

class ItemIsA(Base):
    __tablename__ = 'item_isa'
    item_id = Column(Integer,
                     ForeignKey('item.item_id'),
                     primary_key=True,
                     autoincrement=False)
    isa_id = Column(Integer,
                     ForeignKey('isa.item_id'),
                     primary_key=True,
                     autoincrement=False)

    item = relationship('Item')
    isa = relationship('IsA')

class Extract(Base):
    __tablename__ = 'extract'

    item_id = Column(Integer,
                     ForeignKey('item.item_id'),
                     primary_key=True,
                     autoincrement=False)
    site = Column(String, primary_key=True)
    extract = Column(String, nullable=False)

    def __init__(self, site, extract):
        self.site = site
        self.extract = extract

class Item(Base):
    __tablename__ = 'item'

    item_id = Column(Integer, primary_key=True, autoincrement=False)
    location = Column(Geography('POINT', spatial_index=True), nullable=False)
    enwiki = Column(String, index=True)
    entity = Column(postgresql.JSON)
    categories = Column(postgresql.ARRAY(String))
    old_tags = Column(postgresql.ARRAY(String))
    qid = column_property('Q' + cast(item_id, String))
    ewkt = column_property(func.ST_AsEWKT(location), deferred=True)
    query_label = Column(String, index=True)
    # extract = Column(String)
    extract_names = Column(postgresql.ARRAY(String))

    db_tags = relationship('ItemTag',
                           collection_class=set,
                           cascade='save-update, merge, delete, delete-orphan',
                           backref='item')

    tags = association_proxy('db_tags', 'tag_or_key')

    isa = relationship('IsA', secondary='item_isa')
    wiki_extracts = relationship('Extract',
                                 collection_class=attribute_mapped_collection('site'),
                                 cascade='save-update, merge, delete, delete-orphan',
                                 backref='item')
    extracts = association_proxy('wiki_extracts', 'extract')

    @property
    def extract(self):
        return self.extracts.get('enwiki')

    @extract.setter
    def extract(self, value):
        self.extracts['enwiki'] = value

    @property
    def labels(self):
        if not self.entity:
            return None

        return {l['language']: l['value']
                for l in self.entity['labels'].values()}

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

    def label_best_language(self, languages):
        if not languages:
            return self.label()
        labels = self.entity['labels']
        for lang in languages:
            code = lang if isinstance(lang, str) else lang.wikimedia_language_code
            if code in labels:
                return labels[code]['value']
        return self.label()

    def languages(self):
        entity = self.entity
        labels = {lang for lang in entity['labels'].keys() if '-' not in lang}
        sitelinks = {i[:-4] for i in entity['sitelinks'].keys() if i.endswith('wiki')}

        return labels | sitelinks

    def more_endings_from_isa(self):
        endings = set()
        langs = self.languages()
        # avoid trimming "cottage", it produces too many mismatches
        skip_isa = {
            5783996,  # cottage
        }
        for isa in self.isa:
            if isa.item_id in skip_isa or not isa.entity or 'missing' in isa.entity:
                continue
            for lang, label in isa.entity.get('labels', {}).items():
                if lang in langs:
                    endings.add(label['value'])
        return endings

    @classmethod
    def get_by_qid(cls, qid):
        if qid and len(qid) > 1 and qid[0].upper() == 'Q' and qid[1:].isdigit():
            return cls.query.get(qid[1:])

    def label_and_qid(self, lang='en'):
        label = self.label(lang=lang)
        if label:
            return '{label} ({item.qid})'.format(label=label, item=self)
        else:
            return self.qid

    @property
    def wikidata_uri(self):
        return 'https://www.wikidata.org/wiki/Q{}'.format(self.item_id)

    def get_lat_lon(self):
        return session.query(func.ST_Y(self.location),
                             func.ST_X(self.location)).one()

    def get_osm_url(self, zoom=18):
        lat, lon = self.get_lat_lon()
        return f'https://www.openstreetmap.org/#map={zoom}/{lat}/{lon}'

    def get_extra_tags(self):
        tags = set()
        for qid in self.instanceof():
            for tag in wikidata.extra_keys.get(qid, []):
                if tag:
                    tags.add(tag[4:])

        return tags

    @property
    def ref_keys(self):
        return {f'ref:nrhp={v}' for v in (self.ref_nrhp() or [])}

    def disused_tags(self):
        tags = set()
        prefixes = ('disused', 'was', 'abandoned', 'demolished',
                    'destroyed', 'ruins', 'historic')
        for i in self.tags:
            if i == 'amenity':  # too generic
                continue
            key = i.split('=')[0] if '=' in i else i
            if key in disused_prefix_key:
                tags |= {prefix + ':' + i for prefix in prefixes}
        return tags

    def calculate_tags(self, ignore_tags=None):
        ignore_tags = set(ignore_tags or [])

        # Ignore some overly generic tags from Wikidata objects:
        # facility (Q13226383)            - osm tag: amenity
        # geographic location (Q2221906)  - osm tag: location
        # artificial entity (Q16686448)   - osm tag: man_made

        ignore_tags.update('amenity', 'location', 'man_made')

        instanceof = self.instanceof()

        tags = (self.get_extra_tags() | set(self.tags)) - ignore_tags
        if matcher.could_be_building(tags, instanceof):
            tags.add('building')
            if any(n.lower().endswith(' church') for n in self.names().keys()):
                tags.update({'amenity=place_of_worship', 'building=church'})

        # disable disused tag checking for performance reasons
        tags |= self.ref_keys # | self.disused_tags()
        tags -= ignore_tags
        return tags

    def instanceof(self):
        if self.entity and 'claims' not in self.entity:
            subject = f'missing claims: {self.qid}'
            body = f'''
Wikidata entity is missing claims

https://www.wikidata.org/wiki/{self.qid}
'''
            mail.send_mail(subject, body)

        if not self.entity or 'claims' not in self.entity:
            return []

        return [i['mainsnak']['datavalue']['value']['id']
                for i in self.entity['claims'].get('P31', [])
                if 'datavalue' in i['mainsnak']]

    def identifiers(self):
        ret = set()
        for v in self.get_item_identifiers().values():
            ret.update(v)
        return ret

    def identifier_values(self):
        ret = defaultdict(set)
        for osm_key, wikidata_values in self.get_item_identifiers().items():
            for values, label in wikidata_values:
                ret[osm_key].update(values)
        return ret

    def get_item_identifiers(self):
        if not self.entity:
            return {}

        property_map = [
            ('P238', ['iata'], 'IATA airport code'),
            ('P239', ['icao'], 'ICAO airport code'),
            ('P240', ['faa', 'ref'], 'FAA airport code'),
            # ('P281', ['addr:postcode', 'postal_code'], 'postal code'),
            ('P296', ['ref', 'ref:train', 'railway:ref'], 'station code'),
            ('P300', ['ISO3166-2'], 'ISO 3166-2 code'),
            ('P649', ['ref:nrhp'], 'NRHP reference number'),
            ('P722', ['uic_ref'], 'UIC station code'),
            ('P782', ['ref'], 'LAU (local administrative unit)'),
            ('P836', ['ref:gss'], 'UK Government Statistical Service code'),
            ('P856', ['website', 'contact:website', 'url'], 'website'),
            ('P882', ['nist:fips_code'], 'FIPS 6-4 (US counties)'),
            ('P883', ['state_code', 'ref', 'nist:fips_code'], 'FIPS 5-2 (code for US states)'),
            # A UIC id can be a IBNR, but not every IBNR is an UIC id
            ('P954', ['uic_ref'], 'IBNR ID'),
            ('P1216', ['HE_ref'], 'National Heritage List for England number'),
            ('P2253', ['ref:edubase'], 'EDUBase URN'),
            ('P2815', ['esr:user', 'ref', 'ref:train'], 'ESR station code'),
            ('P3425', ['ref', 'ref:SIC'], 'Natura 2000 site ID'),
            ('P3562', ['seamark:light:reference'], 'Admiralty number'),
            ('P4755', ['ref', 'ref:train', 'ref:crs', 'crs', 'nat_ref'], 'UK railway station code'),
            ('P4803', ['ref', 'ref:train'], 'Amtrak station code'),
        ]

        tags = defaultdict(list)
        for claim, osm_keys, label in property_map:
            values = [i['mainsnak']['datavalue']['value']
                      for i in self.entity['claims'].get(claim, [])
                      if 'datavalue' in i['mainsnak']]
            if not values:
                continue
            if claim == 'P782':
                values += [m.group(1) for m in (re_lau_code.match(v) for v in values) if m]
            for osm_key in osm_keys:
                tags[osm_key].append((tuple(values), label))
        return tags

    def ref_nrhp(self):
        if self.entity:
            return [i['mainsnak']['datavalue']['value']
                    for i in self.entity['claims'].get('P649', [])]
        else:
            return []

    def names(self):
        d = wikidata.names_from_entity(self.entity) or defaultdict(list)
        for name in self.extract_names or []:
            d[name].append(('extract', 'enwiki'))
        return dict(d) or None

    def refresh_extract_names(self):
        self.extract_names = wikipedia.html_names(self.extract)

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
        words = {'demolish', 'disestablishment', 'defunct', 'abandon', 'mothballed',
                 'decommission', 'former', 'dismantled', 'disused', 'disassembled',
                 'abandoned', 'disband', 'scrapped', 'unused', 'closed', 'condemned',
                 'redundant'}

        exclude = {'Defunct baseball venues in the United States',
                   'Defunct National Football League venues',
                   'Enclosed roller coasters',
                   'Former civil parishes in England',
                   'Capitals of former nations',
                   'Former state capitals in the United States'}

        found = []
        for item_cat in self.categories or []:
            if item_cat in exclude:
                continue
            lc_item_cat = item_cat.lower()
            found += [item_cat for i in words if i in lc_item_cat]
        return found

    def get_claim(self, pid):
        return [i['mainsnak']['datavalue']['value']
                for i in self.entity['claims'].get(pid, [])]

    @property
    def criteria(self):
        return {('Tag:' if '=' in t else 'Key:') + t for t in self.tags or []}

    @property
    def category_map(self):
        if self.categories:
            return matcher.categories_to_tags_map(self.categories)

    def sitelinks(self):
        if self.entity:
            return self.entity.get('sitelinks')

    def is_hamlet(self):
        return ('Q5084' in self.instanceof() or
                any(cat.startswith('Hamlets ')
                    for cat in self.categories or []))

    def is_farm_house(self):
        return 'Q489357' in self.instanceof()

    def is_mountain_range(self):
        return 'Q46831' in self.instanceof()

    def is_farmhouse(self):
        return 'Q489357' in self.instanceof()

    def is_proposed(self):
        '''is this item a proposed building or structure?'''

        cats = self.categories or []
        if any(cat.startswith('Disused ') for cat in cats):
            # disused stations that might be reopened could be in OSM
            return False
        if any(cat.startswith('Proposed ') for cat in cats):
            return True
        # proposed building or structure (Q811683)
        return 'Q811683' in (self.instanceof() or [])

    def is_a_historic_district(self):
        cats = self.categories or []
        return (('Q15243209' in (self.instanceof() or []) or
                    any(cat.startswith('Historic district') for cat in cats)) and
                not any(cat.startswith('Historic district contributing properties') or
                        cat.startswith('Churches ') or
                        cat.startswith('Towers ') or
                        cat.startswith('Educational institutions ') or
                        cat.startswith('Schools ') or
                        cat.startswith('Houses ') or
                        cat.startswith('Historic house ') or
                        cat.startswith('Museums ') or
                        ' buildings ' in cat or
                        cat.startswith('Buildings and structures ') for cat in cats))

    def is_a_station(self):
        stations = {
            'Q55488',    # railway station
            'Q928830',   # metro station
            'Q4663385',  # former railway station
        }
        if set(self.instanceof()) & stations:
            return True

        cats = {'railway stations', 'railroad stations', 'train stations',
                'metro stations', 'subway stations'}

        return any(any(cat in item_cat.lower() for cat in cats)
                   for item_cat in (self.categories or []))

    def is_a_stadium(self):
        isa = {
            'Q483110',   # stadium
            'Q641226',   # arena
            'Q1076486',  # sports venue
        }
        if set(self.instanceof()) & isa:
            return True

        cats = {'football venues', 'ice rinks', 'stadiums', 'velodromes',
                'cycling venues', 'grounds'}

        return any(any(cat in item_cat.lower() for cat in cats)
                   for item_cat in (self.categories or []))

    def is_a_school(self):
        return 'amenity=school' in self.tags

    def skip_item_during_match(self):
        ''' cebwiki and svwiki contain lots of poor quality stubs
        best to skip items that are only cebwiki or cebwiki + svwiki
        '''
        if self.is_proposed():  # skip proposed building or structure
            return True
        if not self.entity:
            return False
        sitelinks = self.entity.get('sitelinks')
        if not sitelinks:
            return False
        sites = set(sitelinks.keys())
        return sites == {'cebwiki'} or sites == {'cebwiki', 'svwiki'}

    def get_names(self):
        item = self.entity
        if not item:
            return

        names = defaultdict(list)
        skip_lang = {'ar', 'arc', 'pl'}
        # only include aliases if there are less than 6 other names
        if len(item.get('sitelinks', {})) < 6 and len(item['labels']) < 6:
            for k, v in item.get('aliases', {}).items():
                if k in skip_lang:
                    continue
                if len(v) > 3:
                    continue
                for name in v:
                    names[name].append(('alias', k))
        for k, v in item['labels'].items():
            if k in skip_lang:
                continue
            names[v].append(('label', k))
        for k, v in item.get('sitelinks', {}).items():
            if k + 'wiki' in skip_lang:
                continue
            names[v].append(('sitelink', k))
        return names

    def first_paragraph(self, languages=None):
        if languages is None:
            return self.first_paragraph_language('enwiki')
        for lang in languages:
            extract = self.first_paragraph_language(lang.site_name)
            if extract:
                return extract

    def first_paragraph_language(self, lang):
        extract = self.extracts.get(lang)
        if not extract:
            return

        empty_p_span = '<p><span></span></p>'
        text = extract.strip()
        if text.startswith(empty_p_span):
            text = text[len(empty_p_span):].strip()

        close_tag = '</p>'
        first_end_p_tag = text.find(close_tag)
        if first_end_p_tag == -1:
            # FIXME: e-mail admin
            return text

        return text[:first_end_p_tag + len(close_tag)]

    def place_names(self):
        names = set()
        for place in self.places:
            if not isinstance(place.address, list):
                continue
            names.update({i['name'] for i in place.address
                         if i['type'] != 'country_code'})
        start = 'Isle of '
        trimmed = {utils.drop_start(n, start) for n in names if n.startswith(start)}
        return names | trimmed

    def set_country_code(self):
        for place in self.places:
            if place.country_code:
                g.country_code = place.country_code
                return

    @property
    def is_nhle(self):
        '''Is this a National Heritage List for England item?'''
        return self.entity and 'P1216' in self.entity.get('claims', {})

class ItemTag(Base):
    __tablename__ = 'item_tag'

    item_id = Column(Integer, ForeignKey('item.item_id'), primary_key=True)
    tag_or_key = Column(String, primary_key=True, index=True)

    def __init__(self, tag_or_key):
        self.tag_or_key = tag_or_key

class PlaceItem(Base):
    __tablename__ = 'place_item'

    item_id = Column(Integer, ForeignKey('item.item_id'), primary_key=True)
    osm_type = Column(osm_type_enum, primary_key=True)
    osm_id = Column(BigInteger, primary_key=True)
    place_id = Column(BigInteger)  # unused, replaced by osm_type & osm_id
    done = Column(Boolean)

    __table_args__ = (
        ForeignKeyConstraint(
            ['osm_type', 'osm_id'],
            ['place.osm_type', 'place.osm_id']
        ),
    )

    item = relationship('Item')
    place = relationship('Place')

class OsmCandidate(Base):
    __tablename__ = 'osm_candidate'
    osm_type = Column(osm_type_enum, primary_key=True)
    osm_id = Column(BigInteger, primary_key=True)
    name = Column(String)
    tags = Column(postgresql.JSON)
    geom = Column(Geography(srid=4326, spatial_index=True))

class ItemCandidate(Base):
    __tablename__ = 'item_candidate'

    item_id = Column(Integer, ForeignKey('item.item_id'), primary_key=True)
    osm_id = Column(BigInteger, primary_key=True)
    osm_type = Column(osm_type_enum, primary_key=True)
    name = Column(String)
    dist = Column(Float)
    tags = Column(postgresql.JSON)
    planet_table = Column(String)
    src_id = Column(BigInteger)
    geom = Column(Geography(srid=4326, spatial_index=True))
    geojson = column_property(func.ST_AsGeoJSON(geom), deferred=True)
    identifier_match = Column(Boolean)
    address_match = Column(Boolean)
    name_match = Column(postgresql.JSON)

#    __table_args__ = (
#        ForeignKeyConstraint(
#            ['osm_type', 'osm_id'],
#            ['osm_candidate.osm_type', 'osm_candidate.osm_id']
#        ),
#    )

    item = relationship('Item', backref=backref('candidates',
                                                lazy='dynamic',
                                                cascade='save-update, merge, delete, delete-orphan'))
    # candidate = relationship(OsmCandidate)

#     @property
#     def name(self):
#         return self.candidate.name
#
#     @property
#     def tags(self):
#         return self.candidate.tags
#
    @property
    def key(self):
        return f'Q{self.item_id}-{self.osm_type:s}-{self.osm_id:d}'

    def get_match(self):
        endings = matcher.get_ending_from_criteria(self.tags)
        wikidata_names = self.item.names()
        return match.check_for_match(self.tags, wikidata_names, endings)

    def get_all_matches(self):
        endings = matcher.get_ending_from_criteria(self.item.tags)
        wikidata_names = self.item.names()
        m = match.get_all_matches(self.tags, wikidata_names, endings)
        return m

    def languages(self):
        return {key[5:] for key in self.tags.keys()
                if key.startswith('name:')}

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

    def update(self, candidate):
        for k, v in candidate.items():
            if k in {'osm_id', 'osm_type'}:
                continue
            setattr(self, k, v)

    @property
    def wikidata_tag(self):
        return self.tags.get('wikidata') or None

    def label_best_language(self, languages):
        if not languages:
            return self.label

        for key in 'bridge:name', 'tunnel:name', 'lock_name':
            if key in self.tags:
                return self.tags[key]

        names = {k[5:]: v for k, v in self.tags.items()
                 if k.startswith('name:')}
        if 'name' in self.tags:
            top_lang = g.default_languages[0]['code']
            if top_lang not in names:
                names[top_lang] = self.tags['name']

        for lang in languages:
            key = lang if isinstance(lang, str) else lang.iso_639_1
            if key in names:
                return names[key]

        return self.label

    @property
    def label(self):
        for key in 'bridge:name', 'tunnel:name', 'lock_name':
            if key in self.tags:
                return self.tags[key]

        if 'name' in self.tags:
            name = self.tags['name']
            if 'addr:housename' in self.tags:
                return f'{name} (house name: {self.tags["addr:housename"]})'
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

        if all(tag in self.tags for tag in ('addr:housenumber', 'addr:street')):
            housenumber = self.tags['addr:housenumber']
            street = self.tags['addr:street']
            return f'{housenumber} {street}'

        return f'{self.osm_type}/{self.osm_id}'

    @property
    def url(self):
        return f'{osm_api_base}/{self.osm_type}/{self.osm_id}'

    def name_match_count(self, osm_key):
        if not self.name_match:
            return

        match_count = 0
        for match_type, wikidata_name, source in self.name_match[osm_key]:
            match_count += len(source)
        return match_count

    def set_match_detail(self):
        keys = ['identifier', 'address', 'name']
        if any(getattr(self, key + '_match') is not None for key in keys):
            return False  # no need

        endings = matcher.get_ending_from_criteria(self.tags)
        endings |= self.item.more_endings_from_isa()

        names = self.item.names()
        identifiers = self.item.get_item_identifiers()
        self.address_match = match.check_name_matches_address(self.tags, names)
        self.name_match = match.check_for_match(self.tags, names, endings)
        self.identifier_match = match.check_identifier(self.tags, identifiers)
        return True

    def display_distance(self):
        if has_app_context() and g.user.is_authenticated and g.user.units:
            units = g.user.units
        else:
            units = 'local'  # default

        if units == 'local':
            country_code = (getattr(g, 'country_code', None)
                            if has_app_context()
                            else None)
            units = country_units.get(country_code, 'km_and_metres')

        return utils.display_distance(units, self.dist)

    def checkbox_ticked(self):
        max_dist = 500
        if any(tag == 'place' or (tag != 'place=farm' and tag.startswith('place='))
               for tag in self.matching_tags()):
            max_dist = 2000
        elif self.item.is_nhle:
            max_dist = 100
        return ((not self.dist or
                 self.dist < max_dist and
                 'designation=civil_parish' not in self.matching_tags()) or
                 self.item.candidates.count() > 1)

    def new_wikipedia_tag(self, languages):
        sitelinks = {code[:-4]: link['title']
                     for code, link in self.item.sitelinks().items()
                     if code.endswith('wiki')}

        for lang in languages:
            code = lang if isinstance(lang, str) else lang.wikimedia_language_code
            if code in sitelinks:
                return (code, sitelinks[code])
        return (None, None)

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
    place_id = Column(BigInteger)
    osm_type = Column(osm_type_enum, index=True)
    osm_id = Column(BigInteger, index=True)
    item_id = Column(Integer)
    comment = Column(String)
    user_id = Column(Integer, ForeignKey(User.id))
    update_count = Column(Integer, nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ['osm_type', 'osm_id'],
            ['place.osm_type', 'place.osm_id']
        ),
    )

    user = relationship('User',
                        backref=backref('changesets',
                                        lazy='dynamic',
                                        order_by='Changeset.created.desc()'))
    place = relationship('Place',
                         backref=backref('changesets',
                                        lazy='dynamic',
                                        order_by='Changeset.created.desc()'))

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
    saved = Column(DateTime, default=now_utc(), nullable=False)

    changeset = relationship('Changeset',
                             backref=backref('edits', lazy='dynamic'))

    candidate = relationship('ItemCandidate',
                             backref=backref('edits', lazy='dynamic'))

class EditMatchReject(Base):
    __tablename__ = 'edit_match_reject'

    __table_args__ = (
        ForeignKeyConstraint(['changeset_id',
                              'item_id',
                              'osm_id',
                              'osm_type'],
                             [ChangesetEdit.changeset_id,
                              ChangesetEdit.item_id,
                              ChangesetEdit.osm_id,
                              ChangesetEdit.osm_type]),
    )

    changeset_id = Column(BigInteger, primary_key=True)
    item_id = Column(Integer, primary_key=True)
    osm_id = Column(BigInteger, primary_key=True)
    osm_type = Column(osm_type_enum, primary_key=True)
    report_timestamp = Column(DateTime, primary_key=True)
    matcher_result = Column(postgresql.JSON, nullable=False)

    edit = relationship('ChangesetEdit')

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
    created = Column(DateTime, default=now_utc())
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

def get_bad(items):
    if not items:
        return {}
    q = (session.query(BadMatch.item_id)
                .filter(BadMatch.item_id.in_([i.item_id for i in items])))
    return {item_id for item_id, in q}

class Language(Base):
    __tablename__ = 'language'
    item_id = Column(Integer, primary_key=True, autoincrement=False)
    iso_639_1 = Column(String(2))
    iso_639_2 = Column(String(3))
    iso_639_3 = Column(String(3))
    wikimedia_language_code = Column(String, unique=True)
    qid = column_property('Q' + cast(item_id, String))
    labels = relationship('LanguageLabel',
                          lazy='dynamic',
                          foreign_keys=lambda: LanguageLabel.item_id)

    def english_name(self):
        return self.labels.filter_by(wikimedia_language_code='en').one().label

    def self_name(self):
        ''' Name of this language in this language. '''
        name = self.labels.filter_by(language=self).one_or_none()
        if name:
            return name.label

    def label(self):
        name = self.self_name()
        if not name:  # self label missing for language
            name = self.english_name()
        elif self.wikimedia_language_code != 'en':  # add name in English
            name = capfirst(name) + ' / ' + capfirst(self.english_name())
        return f'{name} [{self.wikimedia_language_code}]'

    @property
    def site_name(self):
        return f'{self.wikimedia_language_code}wiki'

class LanguageLabel(Base):
    __tablename__ = 'language_label'
    item_id = Column(Integer,
                     ForeignKey(Language.item_id),
                     primary_key=True,
                     autoincrement=False)
    wikimedia_language_code = Column(String,
                                     ForeignKey(Language.wikimedia_language_code),
                                     primary_key=True)
    label = Column(String, nullable=False)

    language = relationship('Language', foreign_keys=[wikimedia_language_code])
