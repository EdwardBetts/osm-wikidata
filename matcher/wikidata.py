from flask import render_template_string
from urllib.parse import unquote
from collections import defaultdict
from .utils import chunk, drop_start, cache_filename
from .language import get_language_label
from . import user_agent_headers, overpass, mail, language, match, matcher
import requests
import os
import json

page_size = 50
wd_entity = 'http://www.wikidata.org/entity/Q'
enwiki = 'https://en.wikipedia.org/wiki/'
skip_tags = {'route:road',
             'route=road',
             'highway=primary',
             'highway=road',
             'highway=service',
             'highway=motorway',
             'highway=trunk',
             'highway=unclassified',
             'highway',
             'landuse'
             'name',
             'website',
             'addr:street',
             'type=associatedStreet',
             'type=waterway',
             'waterway=river'}

extra_keys = {
    'Q1021290': 'Tag:amenity=college',  # music school
    'Q5167149': 'Tag:amenity=college',  # cooking school
    'Q383092': 'Tag:amenity=college',   # film school
    'Q11303': 'Key:height',             # skyscraper
    'Q18142': 'Key:height',             # high-rise building
    'Q33673393': 'Key:height',          # multi-storey building
    'Q641226': 'Tag:leisure=stadium',   # arena
    'Q2301048': 'Tag:aeroway=helipad',  # special airfield
}

# search for items in bounding box that have an English Wikipedia article
wikidata_enwiki_query = '''
SELECT ?place ?placeLabel (SAMPLE(?location) AS ?location) ?article WHERE {
    SERVICE wikibase:box {
        ?place wdt:P625 ?location .
        bd:serviceParam wikibase:cornerWest "Point({{ west }} {{ south }})"^^geo:wktLiteral .
        bd:serviceParam wikibase:cornerEast "Point({{ east }} {{ north }})"^^geo:wktLiteral .
    }
    ?article schema:about ?place .
    ?article schema:inLanguage "en" .
    ?article schema:isPartOf <https://en.wikipedia.org/> .
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q18340550 } .          # ignore timeline articles
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q13406463 } .          # ignore list articles
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q17362920 } .          # ignore Wikimedia duplicated pages
    FILTER NOT EXISTS { ?place wdt:P31/wdt:P279* wd:Q192611 } .  # ignore constituencies
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?place ?placeLabel ?article
'''

# search for items in bounding box that have an English Wikipedia article
# look for coordinates in the headquarters location (P159)
wikidata_enwiki_hq_query = '''
SELECT ?place ?placeLabel (SAMPLE(?location) AS ?location) ?article WHERE {
    ?place p:P159 ?statement .
    SERVICE wikibase:box {
        ?statement pq:P625 ?location .
        bd:serviceParam wikibase:cornerWest "Point({{ west }} {{ south }})"^^geo:wktLiteral .
        bd:serviceParam wikibase:cornerEast "Point({{ east }} {{ north }})"^^geo:wktLiteral .
    }
    ?article schema:about ?place .
    ?article schema:inLanguage "en" .
    ?article schema:isPartOf <https://en.wikipedia.org/> .
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?place ?placeLabel ?article
'''

wikidata_point_query = '''
SELECT ?place (SAMPLE(?location) AS ?location) ?article WHERE {
    SERVICE wikibase:around {
        ?place wdt:P625 ?location .
        bd:serviceParam wikibase:center "Point({{ lon }} {{ lat }})"^^geo:wktLiteral .
        bd:serviceParam wikibase:radius "{{ '{:.1f}'.format(radius) }}" .
    }
    ?article schema:about ?place .
    ?article schema:inLanguage "en" .
    ?article schema:isPartOf <https://en.wikipedia.org/> .
}
GROUP BY ?place ?article
'''

wikidata_subclass_osm_tags = '''
SELECT DISTINCT ?item ?itemLabel ?tag
WHERE
{
  {
    wd:{{qid}} wdt:P31/wdt:P279* ?item .
    {
        ?item wdt:P1282 ?tag .
    }
    UNION
    {
        ?item wdt:P641 ?sport .
        ?sport wdt:P1282 ?tag
    }
    UNION
    {
        ?item wdt:P140 ?religion .
        ?religion wdt:P1282 ?tag
    }
  }
  UNION
  {
      wd:{{qid}} wdt:P1435 ?item .
      ?item wdt:P1282 ?tag
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}'''

# search for items in bounding box that have OSM tags in the subclass tree
wikidata_item_tags = '''
SELECT ?place ?placeLabel (SAMPLE(?location) AS ?location) ?address ?street ?item ?itemLabel ?tag WHERE {
    SERVICE wikibase:box {
        ?place wdt:P625 ?location .
        bd:serviceParam wikibase:cornerWest "Point({{ west }} {{ south }})"^^geo:wktLiteral .
        bd:serviceParam wikibase:cornerEast "Point({{ east }} {{ north }})"^^geo:wktLiteral .
    }
    ?place wdt:P31/wdt:P279* ?item .
    ?item wdt:P1282 ?tag .
    OPTIONAL { ?place wdt:P969 ?address } .
    OPTIONAL { ?place wdt:P669 ?street } .
    FILTER NOT EXISTS { ?item wdt:P31 wd:Q18340550 } .           # ignore timeline articles
    FILTER NOT EXISTS { ?item wdt:P31 wd:Q13406463 } .           # ignore list articles
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q17362920 } .          # ignore Wikimedia duplicated pages
    FILTER NOT EXISTS { ?place wdt:P31/wdt:P279* wd:Q192611 } .  # ignore constituencies
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?place ?placeLabel ?address ?street ?item ?itemLabel ?tag
'''

# search for items in bounding box that have OSM tags in the subclass tree
# look for coordinates in the headquarters location (P159)
wikidata_hq_item_tags = '''
SELECT ?place ?placeLabel (SAMPLE(?location) AS ?location) ?address ?street ?item ?itemLabel ?tag WHERE {
    ?place p:P159 ?statement .
    SERVICE wikibase:box {
        ?statement pq:P625 ?location .
        bd:serviceParam wikibase:cornerWest "Point({{ west }} {{ south }})"^^geo:wktLiteral .
        bd:serviceParam wikibase:cornerEast "Point({{ east }} {{ north }})"^^geo:wktLiteral .
    }
    ?place wdt:P31/wdt:P279* ?item .
    ?item wdt:P1282 ?tag .
    OPTIONAL { ?place wdt:P969 ?address } .
    OPTIONAL { ?place wdt:P669 ?street } .
    FILTER NOT EXISTS { ?place wdt:P31/wdt:P279* wd:Q192611 } .     # ignore constituencies
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?place ?placeLabel ?address ?street ?item ?itemLabel ?tag
'''

# Q15893266 == former entity
# Q56061 == administrative territorial entity

next_level_query = '''
SELECT DISTINCT ?item ?itemLabel ?startLabel (SAMPLE(?pop) AS ?pop) ?area WHERE {
  VALUES ?start { wd:QID } .
  ?start wdt:P31/wdt:P279* ?subclass .
  ?subclass wdt:P150 ?nextlevel .
  ?item wdt:P131 ?start .
  ?item wdt:P31/wdt:P279* ?nextlevel .
  ?item wdt:P31/wdt:P279* wd:Q56061 .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q15893266 } .
  FILTER NOT EXISTS { ?item wdt:P576 ?end } .
  OPTIONAL { ?item wdt:P1082 ?pop } .
  OPTIONAL { ?item wdt:P2046 ?area } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?item ?itemLabel ?startLabel ?area
ORDER BY ?itemLabel
'''

# administrative territorial entity of a single country (Q15916867)

#           'Q349084'],    # England  -> district of England
admin_area_map = {
    'Q21': ['Q1136601',    # England  -> unitary authority of England
            'Q211690',     # |           London borough
            'Q1002812',    # |           metropolitan borough
            'Q643815'],    # |           (non-)metropolitan county of England
    'Q22': ['Q15060255'],  # Scotland          -> council area
    'Q25': ['Q15979307'],  # Wales            -> principal area of Wales
    'Q26': ['Q17364572'],  # Northern Ireland -> district of Northern Ireland
}

next_level_query2 = '''
SELECT DISTINCT ?item ?itemLabel ?startLabel (SAMPLE(?pop) AS ?pop) ?area WHERE {
  VALUES ?start { wd:QID } .
  TYPES
  # metropolitan borough of the County of London (old)
  FILTER NOT EXISTS { ?item wdt:P31 wd:Q9046617 } .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q19953632 } .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q15893266 } .
  FILTER NOT EXISTS { ?item wdt:P576 ?end } .
  OPTIONAL { ?item wdt:P1082 ?pop } .
  OPTIONAL { ?item wdt:P2046 ?area } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?item ?itemLabel ?startLabel ?area
ORDER BY ?itemLabel
'''

countries_in_continent_query = '''
SELECT DISTINCT ?item
                ?itemLabel
                ?startLabel
                (SAMPLE(?pop) AS ?pop)
                (SAMPLE(?area) AS ?area)
WHERE {
  VALUES ?start { wd:QID } .
  {
      ?item wdt:P31/wdt:P279* wd:Q3624078 .  # sovereign state
  } UNION {
      ?item wdt:P31/wdt:P279* wd:Q161243 .   # dependent territory
  } UNION {
      ?item wdt:P31/wdt:P279* wd:Q179164 .   # unitary state
  } UNION {
      ?item wdt:P31/wdt:P279* wd:Q1763527 .  # constituent country
  }

  ?item wdt:P30 ?start .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q15893266 } .
  FILTER NOT EXISTS { ?item wdt:P576 ?end } .
  OPTIONAL { ?item wdt:P1082 ?pop } .
  OPTIONAL { ?item wdt:P2046 ?area } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?item ?itemLabel ?startLabel
ORDER BY ?itemLabel
'''

# walk place hierarchy grabbing labels and country names
located_in_query = '''
SELECT ?item ?itemLabel ?country ?countryLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  VALUES ?start { wd:QID } .
  ?start wdt:P131* ?item .
  OPTIONAL { ?item wdt:P17 ?country.}
}
'''

up_one_level_query = '''
SELECT ?startLabel ?itemLabel ?country1 ?country1Label ?country2 ?country2Label WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  VALUES ?start { wd:QID } .
  OPTIONAL { ?start wdt:P17 ?country1 }
  OPTIONAL { ?start wdt:P131 ?item }
  OPTIONAL { ?item wdt:P17 ?country2 }
}
'''

next_level_type_map = {
    'Q48091': 'Q180673',  # English region -> ceremonial county of England
}

next_level_by_type = '''
SELECT DISTINCT ?item ?itemLabel
                ?startLabel
                (SAMPLE(?pop) AS ?pop)
                (SAMPLE(?area) AS ?area) WHERE {
  VALUES ?start { wd:QID } .
  TYPES
  ?item wdt:P131 ?start .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q19953632 } .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q15893266 } .
  FILTER NOT EXISTS { ?item wdt:P576 ?end } .
  OPTIONAL { ?item wdt:P1082 ?pop } .
  OPTIONAL { ?item wdt:P2046 ?area } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?item ?itemLabel ?startLabel
ORDER BY ?itemLabel
'''

wikidata_query_api_url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'

class QueryError(Exception):
    def __init__(self, query, r):
        self.query = query
        self.r = r

def get_query(q, south, north, west, east):
    return render_template_string(q,
                                  south=south,
                                  north=north,
                                  west=west,
                                  east=east)

def get_enwiki_query(*args):
    return get_query(wikidata_enwiki_query, *args)

def get_enwiki_hq_query(*args):
    return get_query(wikidata_enwiki_hq_query, *args)

def get_item_tag_query(*args):
    return get_query(wikidata_item_tags, *args)

def get_hq_item_tag_query(*args):
    return get_query(wikidata_hq_item_tags, *args)

def get_point_query(lat, lon, radius):
    return render_template_string(wikidata_point_query,
                                  lat=lat,
                                  lon=lon,
                                  radius=float(radius) / 1000.0)

def run_query(query, name=None, timeout=None):
    if name:
        filename = cache_filename(name + '.json')
        if os.path.exists(filename):
            return json.load(open(filename))['results']['bindings']

    r = requests.get(wikidata_query_api_url,
                     params={'query': query, 'format': 'json'},
                     timeout=timeout,
                     headers=user_agent_headers())
    if r.status_code != 200:
        mail.error_mail('wikidata query error', query, r)
        raise QueryError(query, r)
    if name:
        open(filename, 'wb').write(r.content)
    return r.json()['results']['bindings']

def flatten_criteria(items):
    start = {'Tag:' + i[4:] + '=' for i in items if i.startswith('Key:')}
    return {i for i in items if not any(i.startswith(s) for s in start)}

def wd_uri_to_id(value):
    return int(drop_start(value, wd_entity))

def wd_uri_to_qid(value):
    assert value.startswith(wd_entity)
    return value[len(wd_entity) - 1:]

def enwiki_url_to_title(url):
    return unquote(drop_start(url, enwiki)).replace('_', ' ')

def parse_enwiki_query_old(query):
    return [{
        'location': i['location']['value'],
        'id': wd_uri_to_id(i['place']['value']),
        'enwiki':enwiki_url_to_title(i['article']['value']),
    } for i in query]

def parse_enwiki_query(rows):
    return {wd_uri_to_qid(row['place']['value']):
            {
                'query_label': row['placeLabel']['value'],
                'enwiki': enwiki_url_to_title(row['article']['value']),
                'location': row['location']['value'],
                'tags': set(),
            } for row in rows}

def drop_tag_prefix(v):
    if v.startswith('Key:') and '=' not in v:
        return v[4:]
    if v.startswith('Tag:') and '=' in v:
        return v[4:]

def parse_item_tag_query(rows, items):
    for row in rows:
        tag_or_key = drop_tag_prefix(row['tag']['value'])
        if not tag_or_key or tag_or_key in skip_tags:
            continue
        qid = wd_uri_to_qid(row['place']['value'])

        if qid not in items:
            items[qid] = {
                'query_label': row['placeLabel']['value'],
                'location': row['location']['value'],
                'tags': set(),
            }
            for k in 'address', 'street':
                if k in row:
                    items[qid][k] = row[k]['value']
        items[qid]['tags'].add(tag_or_key)

def entity_iter(ids, debug=False):
    wikidata_url = 'https://www.wikidata.org/w/api.php'
    params = {
        'format': 'json',
        'formatversion': 2,
        'action': 'wbgetentities',
    }
    for num, cur in enumerate(chunk(ids, page_size)):
        if debug:
            print('entity_iter: {}/{}'.format(num * page_size, len(ids)))
        params['ids'] = '|'.join(cur)
        r = requests.get(wikidata_url,
                         params=params,
                         headers=user_agent_headers())
        r.raise_for_status()
        json_data = r.json()
        for qid, entity in json_data['entities'].items():
            yield qid, entity

def get_entity(qid):
    wikidata_url = 'https://www.wikidata.org/w/api.php'
    params = {
        'format': 'json',
        'formatversion': 2,
        'action': 'wbgetentities',
        'ids': qid,
    }
    json_data = requests.get(wikidata_url,
                             params=params,
                             headers=user_agent_headers()).json()
    try:
        entity = list(json_data['entities'].values())[0]
    except KeyError:
        return None
    if 'missing' not in entity:
        return entity

def get_entities(ids):
    if not ids:
        return []
    wikidata_url = 'https://www.wikidata.org/w/api.php'
    params = {
        'format': 'json',
        'formatversion': 2,
        'action': 'wbgetentities',
        'ids': '|'.join(ids),
    }
    json_data = requests.get(wikidata_url,
                             params=params,
                             headers=user_agent_headers()).json()
    return list(json_data['entities'].values())

def names_from_entity(entity, skip_lang=None):
    if skip_lang is None:
        skip_lang = set()
    if not entity:
        return

    ret = defaultdict(list)
    cat_start = 'Category:'

    for k, v in entity['labels'].items():
        if k in skip_lang:
            continue
        ret[v['value']].append(('label', k))

    for k, v in entity['sitelinks'].items():
        if k + 'wiki' in skip_lang:
            continue
        title = v['title']
        if title.startswith(cat_start):
            title = title[len(cat_start):]

        ret[title].append(('sitelink', k))

    for lang, value_list in entity.get('aliases', {}).items():
        if lang in skip_lang or len(value_list) > 3:
            continue
        for name in value_list:
            ret[name['value']].append(('alias', lang))

    commonscats = entity.get('claims', {}).get('P373', [])
    for i in commonscats:
        value = i['mainsnak']['datavalue']['value']
        ret[value].append(('commonscat', None))

    officialname = entity.get('claims', {}).get('P1448', [])
    for i in officialname:
        value = i['mainsnak']['datavalue']['value']
        ret[value['text']].append(('officialname', value['language']))

    nativelabel = entity.get('claims', {}).get('P1705', [])
    for i in nativelabel:
        value = i['mainsnak']['datavalue']['value']
        ret[value['text']].append(('nativelabel', value['language']))

    return ret

def parse_osm_keys(rows):
    start = 'http://www.wikidata.org/entity/'
    items = {}
    for row in rows:
        uri = row['item']['value']
        qid = drop_start(uri, start)
        tag = row['tag']['value']
        for i in 'Key:', 'Tag:':
            if tag.startswith(i):
                tag = tag[4:]
        if qid not in items:
            items[qid] = {
                'uri': uri,
                'label': row['itemLabel']['value'],
                'tags': set(),
            }
        items[qid]['tags'].add(tag)
    return items

def get_location_hierarchy(qid, name=None):
    # not currently in use
    query = located_in_query.replace('QID', qid)
    return [{
        'qid': wd_uri_to_qid(row['item']['value']),
        'label': row['itemLabel']['value'],
        'country': row['countryLabel']['value'],
    } for row in run_query(query, name=name)]

def up_one_level(qid, name=None):
    query = up_one_level_query.replace('QID', qid)
    try:
        rows = run_query(query, name=name, timeout=2)
    except requests.Timeout:
        return

    if rows:
        row = rows[0]
        return {
            'name': row['startLabel']['value'],
            'up': row['itemLabel']['value'],
            'country_qid': wd_uri_to_qid(row['country1']['value']),
            'country_name': row['country1Label']['value'],
            'up_country_qid': wd_uri_to_qid(row['country2']['value']),
            'up_country_name': row['country2Label']['value'],
        }

def next_level_types(types):
    types = list(types)
    if len(types) == 1:
        return '?item wdt:P31/wdt:P279* wd:{} .'.format(types[0])
    return ' union '.join('{ ?item wdt:P31/wdt:P279* wd:' + t + ' }' for t in types)

def isa_list(types):
    types = list(types)
    if len(types) == 1:
        return '?item wdt:P31 wd:{} .'.format(types[0])
    return ' union '.join('{ ?item wdt:P31 wd:' + t + ' }' for t in types)

def next_level_places(qid, entity, name=None):
    isa = {i['mainsnak']['datavalue']['value']['id']
           for i in entity.get('claims', {}).get('P31', [])}

    isa_continent = {
        'Q5107',     # continent
        'Q855697',   # subcontinent
    }

    types_from_isa = isa & next_level_type_map.keys()

    rows = []
    if types_from_isa:
        # use first match in type map
        types = isa_list(next_level_type_map[t] for t in types_from_isa)
        query = next_level_by_type.replace('TYPES', types)
    elif isa & isa_continent:
        query = countries_in_continent_query
    elif qid in admin_area_map:
        types = next_level_types(admin_area_map[qid])
        query = next_level_query2.replace('TYPES', types)
    else:
        query = next_level_query

    for row in run_query(query.replace('QID', qid), name=name):
        item_id = wd_uri_to_id(row['item']['value'])
        qid = 'Q{:d}'.format(item_id)
        i = {
            'population': (int(row['pop']['value']) if row.get('pop') else None),
            'area': (int(float(row['area']['value'])) if row.get('area') else None),
            'label': row['itemLabel']['value'],
            'start': row['startLabel']['value'],
            'item_id': item_id,
            'qid': qid,
        }
        rows.append(i)
    return rows

class WikidataItem:
    def __init__(self, qid, entity):
        assert entity
        self.qid = qid
        self.entity = entity

    @classmethod
    def retrieve_item(cls, qid):
        entity = get_entity(qid)
        if not entity:
            return
        item = cls(qid, entity)
        return item

    @property
    def claims(self):
        return self.entity['claims']

    @property
    def labels(self):
        return self.entity.get('labels', {})

    @property
    def aliases(self):
        return self.entity.get('aliases', {})

    @property
    def sitelinks(self):
        return self.entity.get('sitelinks', {})

    def get_sitelinks(self):
        '''List of sitelinks with language names in English.'''
        sitelinks = []
        for key, value in self.sitelinks.items():
            if len(key) != 6 or not key.endswith('wiki'):
                continue
            lang = key[:2]
            url = 'https://{}.wikipedia.org/wiki/{}'.format(lang, value['title'].replace(' ', '_'))
            sitelinks.append({
                'code': lang,
                'lang': get_language_label(lang),
                'url': url,
                'title': value['title'],
            })

        sitelinks.sort(key=lambda i: i['lang'])
        return sitelinks

    def remove_badges(self):
        if 'sitelinks' not in self.entity:
            return
        for v in self.entity['sitelinks'].values():
            if 'badges' in v:
                del v['badges']

    @property
    def has_coords(self):
        try:
            self.claims['P625'][0]['mainsnak']['datavalue']['value']
        except (IndexError, KeyError):
            return False
        return True

    @property
    def has_earth_coords(self):
        if not self.has_coords:
            return
        globe = self.claims['P625'][0]['mainsnak']['datavalue']['value']['globe']
        return globe == 'http://www.wikidata.org/entity/Q2'

    @property
    def coords(self):
        if not self.has_coords:
            return None, None
        c = self.claims['P625'][0]['mainsnak']['datavalue']['value']
        return c['latitude'], c['longitude']

    def get_oql(self, criteria, radius):
        if not criteria:
            return
        lat, lon = self.coords
        if lat is None or lon is None:
            return

        osm_filter = 'around:{},{:.5f},{:.5f}'.format(radius, lat, lon)

        union = []
        for tag_or_key in sorted(criteria):
            union += overpass.oql_from_wikidata_tag_or_key(tag_or_key, osm_filter)

        # FIXME extend oql to also check is_in
        # like this:
        #
        # is_in(48.856089,2.29789);
        # area._[admin_level];
        # out tags;

        oql = ('[timeout:300][out:json];\n' +
               '({}\n);\n' +
               'out center tags;').format(''.join(union))

        return oql

    def trim_location_from_names(self, wikidata_names):
        if 'P131' not in self.entity['claims']:
            return

        location_names = set()
        located_in = [i['mainsnak']['datavalue']['value']['id']
                      for i in self.entity['claims']['P131']]

        for location in get_entities(located_in):
            location_names |= {v['value']
                               for v in location['labels'].values()
                               if v['value'] not in wikidata_names}

        for name_key, name_values in list(wikidata_names.items()):
            for n in location_names:
                new = None
                if name_key.startswith(n + ' '):
                    new = name_key[len(n) + 1:]
                elif name_key.endswith(', ' + n):
                    new = name_key[:-(len(n) + 2)]
                if new and new not in wikidata_names:
                    wikidata_names[new] = name_values

    def osm_key_query(self):
        return render_template_string(wikidata_subclass_osm_tags,
                                      qid=self.qid)

    @property
    def osm_keys(self):
        if hasattr(self, '_osm_keys'):
            return self._osm_keys
        self._osm_keys = run_query(self.osm_key_query())
        return self._osm_keys

    def languages_from_country(self):
        langs = []
        for country in self.claims.get('P17', []):
            c = country['mainsnak']['datavalue']['value']['numeric-id']
            for l in language.get_country_lanaguage(c):
                if l not in langs:
                    langs.append(l)
        return langs

    def query_language_from_country(self):
        if hasattr(self, '_language_codes'):
            return self._language_codes
        query = '''
SELECT DISTINCT ?code WHERE {
  wd:QID wdt:P17 ?country .
  ?country wdt:P37 ?lang .
  ?lang wdt:P424 ?code .
}'''.replace('QID', self.qid)
        rows = run_query(query)
        self._language_codes = [row['code']['value'] for row in rows]
        return self._language_codes

    def label(self, lang=None):
        labels = self.labels
        sitelinks = [i[:-4] for i in self.sitelinks.keys() if i.endswith('wiki')]
        if not labels:
            return
        if lang and lang in labels:  # requested language
            return labels[lang]['value']

        language_codes = self.languages_from_country()
        for code in language_codes:
            if code in labels and code in sitelinks:
                return labels[code]['value']

        for code in language_codes:
            if code in labels:
                return labels[code]['value']

        if 'en' in labels:
            return labels['en']['value']

        for code in sitelinks:
            if code in labels:
                return labels[code]['value']

        return list(labels.values())[0]['value']

    @property
    def names(self):
        if self.entity:
            return dict(names_from_entity(self.entity))
        else:
            return {}

    @property
    def is_a(self):
        return [isa['mainsnak']['datavalue']['value']['id']
                for isa in self.entity.get('claims', {}).get('P31', [])]

    def criteria(self):
        items = {row['tag']['value'] for row in self.osm_keys}
        items |= {extra_keys[is_a] for is_a in self.is_a if is_a in extra_keys}
        return items

    def report_broken_wikidata_osm_tags(self):
        for row in self.osm_keys:
            if not any(row['tag']['value'].startswith(start) for start in ('Key:', 'Tag')):
                body = 'qid: {}\nrow: {}\n'.format(self.qid, repr(row))
                mail.send_mail('broken OSM tag in Wikidata', body)

    def parse_item_query(self, criteria, overpass_reply):
        wikidata_names = self.names
        self.trim_location_from_names(wikidata_names)
        endings = matcher.get_ending_from_criteria({i.partition(':')[2] for i in criteria})

        found = []
        for element in overpass_reply:
            m = match.check_for_match(element['tags'], wikidata_names, endings=endings)
            if m:
                element['key'] = '{0[type]:s}_{0[id]:d}'.format(element)
                found.append((element, m))
        return found
