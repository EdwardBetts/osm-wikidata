from flask import render_template_string
from urllib.parse import unquote
from collections import defaultdict
from .utils import chunk, drop_start, cache_filename
from .language import get_language_label
from . import user_agent_headers, overpass, mail, language, match, matcher
import requests
import os
import json
import simplejson.errors

page_size = 50
report_missing_values = False
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

edu = ['Tag:amenity=college', 'Tag:amenity=university', 'Tag:amenity=school',
       'Tag:office=educational_institution']
tall = ['Key:height', 'Key:building:levels']

extra_keys = {
    'Q1021290': edu,                            # music school
    'Q5167149': edu,                            # cooking school
    'Q383092': edu,                             # film school
    'Q2143781': edu,                            # drama school
    'Q322563': edu,                             # vocational school
    'Q2385804': edu,                            # educational institution
    'Q47530379': edu,                           # agricultural college
    'Q1469420': edu,                            # adult education centre
    'Q7894959': edu,                            # University Technical College
    'Q11303': tall,                             # skyscraper
    'Q18142': tall,                             # high-rise building
    'Q33673393': tall,                          # multi-storey building
    'Q641226': ['Tag:leisure=stadium'],         # arena
    'Q2301048': ['Tag:aeroway=helipad'],        # special airfield
    'Q622425': ['Tag:amenity=pub',
                'Tag:amenity=music_venue'],     # nightclub
    'Q187456': ['Tag:amenity=pub',
                'Tag:amenity=nightclub'],       # bar
    'Q16917': ['Tag:amenity=clinic',
               'Tag:building=clinic'],          # hospital
    'Q330284': ['Tag:amenity=market'],          # marketplace
    'Q5307737': ['Tag:amenity=pub',
                 'Tag:amenity=bar'],            # drinking establishment
    'Q875157': ['Tag:tourism=resort'],          # resort
    'Q174782': ['Tag:leisure=park'],            # square
    'Q34627': ['Tag:religion=jewish'],          # synagogue
    'Q16970': ['Tag:religion=christian'],       # church
    'Q32815': ['Tag:religion=islam'],           # mosque
    'Q811979': ['Key:building'],                # architectural structure
    'Q1329623': ['Tag:amenity=arts_centre'],    # cultural centre
    'Q856584': ['Tag:amenity=library'],         # library building
    'Q11315': ['Tag:landuse=retail'],           # shopping mall
    'Q39658032': ['Tag:landuse=retail'],        # open air shopping centre
    'Q277760': ['Tag:historic=folly'],          # gatehouse
    'Q15243209': ['Tag:leisure=park'],          # historic district
    'Q3010369': ['Tag:historic=monument'],      # opening ceremony
    'Q123705': ['Tag:place=suburb'],            # neighbourhood
    'Q256020': ['Tag:amenity=pub'],             # inn
    'Q41253': ['Tag:amenity=theatre'],          # movie theater
    'Q17350442': ['Tag:amenity=theatre'],       # venue
    'Q156362': ['Tag:amenity=winery'],          # winery
    'Q14092': ['Tag:leisure=fitness_centre',
               'Tag:leisure=sports_centre'],    # gymnasium
    'Q27686': ['Tag:tourism=hostel'],           # hotel
    'Q11707': ['Tag:amenity=cafe', 'Tag:amenity=fast_food',
               'Tag:shop=deli', 'Tag:shop=bakery',
               'Key:cuisine'],                  # restaurant
    'Q2360219': ['Tag:amenity=embassy'],        # permanent mission
    'Q838948': ['Tag:historic=memorial',
                'Tag:historic=monument'],       # work of art
    'Q23413': ['Tag:place=locality'],           # castle
    'Q28045079': ['Tag:historic=archaeological_site',
                  'Tag:site_type=fortification',
                  'Tag:embankment=yes'],        # contour fort
    'Q515': ['Tag:border_type=city'],           # city
    'Q1254933': ['Tag:amenity=university'],     # astronomical observatory
    'Q1976594': ['Tag:landuse=industrial'],     # science park
    'Q190928': ['Tag:landuse=industrial'],      # shipyard
    'Q11997323': ['Tag:emergency=lifeboat_station'],  # lifeboat station
    'Q16884952': ['Tag:castle_type=stately'],   # country house
    'Q1343246': ['Tag:castle_type=stately'],    # English country house
    'Q4919932': ['Tag:castle_type=stately'],    # stately home
    'Q1763828': ['Tag:amenity=community_centre'],  # multi-purpose hall
    'Q489357': ['Tag:landuse=farmyard', 'Tag:place=farm'],  # farmhouse
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
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q18340550 } .          # ignore timeline article
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q13406463 } .          # ignore list article
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q17362920 } .          # ignore Wikimedia duplicated page
    FILTER NOT EXISTS { ?place wdt:P31/wdt:P279* wd:Q192611 } .  # ignore constituency
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q811683 } .            # ignore proposed building or structure
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
    ?item ((p:P1282/ps:P1282)|wdt:P641/(p:P1282/ps:P1282)|wdt:P140/(p:P1282/ps:P1282)|wdt:P366/(p:P1282/ps:P1282)) ?tag .
  }
  UNION
  {
      wd:{{qid}} wdt:P1435 ?item .
      ?item (p:P1282/ps:P1282) ?tag
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
    ?item ((p:P1282/ps:P1282)|wdt:P641/(p:P1282/ps:P1282)|wdt:P140/(p:P1282/ps:P1282)|wdt:P366/(p:P1282/ps:P1282)) ?tag .
    OPTIONAL { ?place wdt:P969 ?address } .
    OPTIONAL { ?place wdt:P669 ?street } .
    FILTER NOT EXISTS { ?item wdt:P31 wd:Q18340550 } .           # ignore timeline article
    FILTER NOT EXISTS { ?item wdt:P31 wd:Q13406463 } .           # ignore list article
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q17362920 } .          # ignore Wikimedia duplicated page
    FILTER NOT EXISTS { ?place wdt:P31/wdt:P279* wd:Q192611 } .  # ignore constituency
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q811683 } .            # ignore proposed building or structure
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
    ?item ((p:P1282/ps:P1282)|wdt:P641/(p:P1282/ps:P1282)|wdt:P140/(p:P1282/ps:P1282)|wdt:P366/(p:P1282/ps:P1282)) ?tag .
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
SELECT DISTINCT ?item ?itemLabel
                ?startLabel
                (SAMPLE(?pop) AS ?pop)
                (SAMPLE(?area) AS ?area)
                (GROUP_CONCAT(DISTINCT ?isa) as ?isa_list)
WHERE {
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
  OPTIONAL { ?item wdt:P31 ?isa } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?item ?itemLabel ?startLabel
ORDER BY ?itemLabel
'''

next_level_query3 = '''
SELECT DISTINCT ?item ?itemLabel
                ?startLabel
                (SAMPLE(?pop) AS ?pop)
                (SAMPLE(?area) AS ?area)
                (GROUP_CONCAT(?isa) as ?isa_list)
WHERE {
  VALUES ?start { wd:QID } .
  VALUES (?item) { PLACES }
  OPTIONAL { ?item wdt:P1082 ?pop } .
  OPTIONAL { ?item wdt:P2046 ?area } .
  OPTIONAL { ?item wdt:P31 ?isa } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?item ?itemLabel ?startLabel
ORDER BY ?itemLabel
'''

item_labels_query = '''
SELECT ?item ?itemLabel
WHERE {
  VALUES (?item) { ITEMS }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}'''

item_types = '''
SELECT DISTINCT ?item ?type WHERE {
  VALUES ?item { ITEMS }
  {
      ?item wdt:P31/wdt:P279* ?type .
      ?type ((p:P1282/ps:P1282)|wdt:P641/(p:P1282/ps:P1282)|wdt:P140/(p:P1282/ps:P1282)|wdt:P366/(p:P1282/ps:P1282)) ?tag .
      FILTER(?tag != 'Key:amenity')
  } UNION {
      ?item wdt:P31 ?type .
      VALUES (?type) { TYPES }
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
'''

item_types_tree = '''
SELECT DISTINCT ?item ?itemLabel ?country ?countryLabel ?type ?typeLabel WHERE {
  {
    VALUES ?top { ITEMS }
    ?top wdt:P31/wdt:P279* ?item .
    ?item wdt:P279 ?type .
    ?type wdt:P279* ?subtype .
    ?subtype ((p:P1282/ps:P1282)|wdt:P641/(p:P1282/ps:P1282)|wdt:P140/(p:P1282/ps:P1282)|wdt:P366/(p:P1282/ps:P1282)) ?tag .
  } UNION {
    VALUES ?item { ITEMS }
    ?item wdt:P31 ?type .
  }
  OPTIONAL { ?item wdt:P17 ?country }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
'''

subclasses = '''
SELECT DISTINCT ?item ?itemLabel ?type ?typeLabel WHERE {
  VALUES (?item) { ITEMS }
  VALUES (?type) { ITEMS }
  ?item wdt:P279* ?type .
  FILTER (?item != ?type)
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
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
SELECT DISTINCT ?item ?itemLabel
                ?startLabel
                (SAMPLE(?pop) AS ?pop)
                (SAMPLE(?area) AS ?area)
                (GROUP_CONCAT(?isa) as ?isa_list)
WHERE {
  VALUES ?start { wd:QID } .
  TYPES
  # metropolitan borough of the County of London (old)
  FILTER NOT EXISTS { ?item wdt:P31 wd:Q9046617 } .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q19953632 } .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q15893266 } .
  FILTER NOT EXISTS { ?item wdt:P576 ?end } .
  OPTIONAL { ?item wdt:P1082 ?pop } .
  OPTIONAL { ?item wdt:P2046 ?area } .
  OPTIONAL { ?item wdt:P31 ?isa } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?item ?itemLabel ?startLabel
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
  VALUES (?country) {
    (wd:Q3624078)  # sovereign state
    (wd:Q161243)   # dependent territory
    (wd:Q179164)   # unitary state
    (wd:Q1763527)  # constituent country
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
                (SAMPLE(?area) AS ?area)
                (GROUP_CONCAT(?isa) as ?isa_list)
WHERE {
  VALUES ?start { wd:QID } .
  TYPES
  ?item wdt:P131 ?start .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q19953632 } .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q15893266 } .
  FILTER NOT EXISTS { ?item wdt:P576 ?end } .
  OPTIONAL { ?item wdt:P1082 ?pop } .
  OPTIONAL { ?item wdt:P2046 ?area } .
  OPTIONAL { ?item wdt:P31 ?isa } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?item ?itemLabel ?startLabel
ORDER BY ?itemLabel
'''

instance_of_query = '''
SELECT DISTINCT ?item ?itemLabel ?countryLabel (SAMPLE(?location) AS ?location) WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en" }
  ?item wdt:P31/wdt:P279* wd:QID .
  OPTIONAL { ?item wdt:P17 ?country }
  OPTIONAL { ?item wdt:P625 ?location }
}
GROUP BY ?item ?itemLabel ?countryLabel
'''

wikidata_query_api_url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'

class QueryError(Exception):
    def __init__(self, query, r):
        self.query = query
        self.r = r

class TooManyEntities(Exception):
    pass

class QueryTimeout(QueryError):
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

def run_query(query, name=None, timeout=None, send_error_mail=True):
    if name:
        filename = cache_filename(name + '.json')
        if os.path.exists(filename):
            return json.load(open(filename))['results']['bindings']

    r = requests.post(wikidata_query_api_url,
                      data={'query': query, 'format': 'json'},
                      timeout=timeout,
                      headers=user_agent_headers())
    if r.status_code == 200:
        if name:
            open(filename, 'wb').write(r.content)
        return r.json()['results']['bindings']

    # query timeout generates two different exceptions
    # java.lang.RuntimeException: java.util.concurrent.ExecutionException: com.bigdata.bop.engine.QueryTimeoutException: Query deadline is expired.
    # java.util.concurrent.TimeoutException
    if ('Query deadline is expired.' in r.text or
            'java.util.concurrent.TimeoutException' in r.text):
        if send_error_mail:
            mail.error_mail('wikidata query timeout', query, r)
        raise QueryTimeout(query, r)

    if send_error_mail:
        mail.error_mail('wikidata query error', query, r)
    raise QueryError(query, r)

def flatten_criteria(items):
    start = {'Tag:' + i[4:] + '=' for i in items if i.startswith('Key:')}
    return {i for i in items if not any(i.startswith(s) for s in start)}

def wd_uri_to_id(value):
    return int(drop_start(value, wd_entity))

def wd_to_qid(wd):
    # expecting {'type': 'url', 'value': 'https://www.wikidata.org/wiki/Q30'}
    if wd['type'] == 'uri':
        return wd_uri_to_qid(wd['value'])

def wd_uri_to_qid(value):
    if not value.startswith(wd_entity):
        print(repr(value))
    assert value.startswith(wd_entity)
    return value[len(wd_entity) - 1:]

def enwiki_url_to_title(url):
    return unquote(drop_start(url, enwiki)).replace('_', ' ')

def parse_enwiki_query(rows):
    return {wd_to_qid(row['place']):
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
        qid = wd_to_qid(row['place'])
        if not qid:
            continue

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

def entity_label(entity):
    if 'en' in entity['labels']:
        return entity['labels']['en']['value']
    else:  # pick a label at random
        return list(entity['labels'].values())[0]['value']

def get_entities(ids):
    if not ids:
        return []
    if len(ids) > 50:
        raise TooManyEntities
    wikidata_url = 'https://www.wikidata.org/w/api.php'
    params = {
        'format': 'json',
        'formatversion': 2,
        'action': 'wbgetentities',
        'ids': '|'.join(ids),
    }
    r = requests.get(wikidata_url, params=params,
                                   headers=user_agent_headers())
    try:
        json_data = r.json()
    except simplejson.errors.JSONDecodeError:
        raise QueryError(params, r)
    return list(json_data['entities'].values())

def names_from_entity(entity, skip_lang=None):
    if not entity:
        return
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

        first_letter = title[0]
        if first_letter.isupper():
            lc_first_title = first_letter.lower() + title[1:]
            if lc_first_title in ret:
                title = lc_first_title

        ret[title].append(('sitelink', k))

    for lang, value_list in entity.get('aliases', {}).items():
        if lang in skip_lang or len(value_list) > 3:
            continue
        for name in value_list:
            ret[name['value']].append(('alias', lang))

    commonscats = entity.get('claims', {}).get('P373', [])
    for i in commonscats:
        if 'datavalue' not in i['mainsnak']:
            if report_missing_values:
                mail.datavalue_missing('commons category', entity)
            continue
        value = i['mainsnak']['datavalue']['value']
        ret[value].append(('commonscat', None))

    officialname = entity.get('claims', {}).get('P1448', [])
    for i in officialname:
        if 'datavalue' not in i['mainsnak']:
            if report_missing_values:
                mail.datavalue_missing('official name', entity)
            continue
        value = i['mainsnak']['datavalue']['value']
        ret[value['text']].append(('officialname', value['language']))

    nativelabel = entity.get('claims', {}).get('P1705', [])
    for i in nativelabel:
        if 'datavalue' not in i['mainsnak']:
            if report_missing_values:
                mail.datavalue_missing('native label', entity)
            continue
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

        # On Wikidata the item for 'facility' (Q13226383), has an OSM key of
        # 'amenity'. This is too generic, so we ignore it.
        if tag == 'amenity':
            continue
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
        'qid': wd_to_qid(row['item']),
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
            'country_qid': wd_to_qid(row['country1']),
            'country_name': row['country1Label']['value'],
            'up_country_qid': wd_to_qid(row['country2']),
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

def get_next_level_query(qid, entity, name=None):
    claims = entity.get('claims', {})

    isa = {i['mainsnak']['datavalue']['value']['id']
           for i in claims.get('P31', [])}

    isa_continent = {
        'Q5107',     # continent
        'Q855697',   # subcontinent
    }

    types_from_isa = isa & next_level_type_map.keys()

    if types_from_isa:
        # use first match in type map
        types = isa_list(next_level_type_map[t] for t in types_from_isa)
        query = next_level_by_type.replace('TYPES', types)
    elif isa & isa_continent:
        query = countries_in_continent_query
    elif qid in admin_area_map:
        types = next_level_types(admin_area_map[qid])
        query = next_level_query2.replace('TYPES', types)
    elif 'P150' in claims:
        places = [i['mainsnak']['datavalue']['value']['id'] for i in claims['P150']]
        query_places = ' '.join(f'(wd:{qid})' for qid in places)
        query = next_level_query3.replace('PLACES', query_places)
    else:
        query = next_level_query

    return query.replace('QID', qid)

def next_level_places(qid, entity, name=None):
    query = get_next_level_query(qid, entity)

    rows = []
    for row in run_query(query, name=name):
        item_id = wd_uri_to_id(row['item']['value'])
        qid = 'Q{:d}'.format(item_id)
        isa_list = []
        for url in row['isa_list']['value'].split(' '):
            if not url:
                continue
            isa_qid = wd_uri_to_qid(url)
            if isa_qid not in isa_list:
                isa_list.append(isa_qid)
        i = {
            'population': (int(row['pop']['value']) if row.get('pop') else None),
            'area': (int(float(row['area']['value'])) if row.get('area') else None),
            'label': row['itemLabel']['value'],
            'start': row['startLabel']['value'],
            'item_id': item_id,
            'qid': qid,
            'isa': isa_list,
        }
        rows.append(i)

    return rows

def get_item_labels_query(items):
    assert items
    query_items = ' '.join(f'(wd:{qid})' for qid in items)
    return item_labels_query.replace('ITEMS', query_items)

def get_item_labels(items):
    query = get_item_labels_query(items)
    rows = []
    for row in run_query(query):
        item_id = wd_uri_to_id(row['item']['value'])
        qid = 'Q{:d}'.format(item_id)

        i = {
            'label': row['itemLabel']['value'],
            'item_id': item_id,
            'qid': qid,
        }
        rows.append(i)
    return rows

def row_qid_and_label(row, name):
    qid = wd_to_qid(row[name])
    if not qid:
        return
    return {'qid': qid, 'label': row[name + 'Label']['value']}

def get_isa(items, name=None):
    graph = item_types_graph(items, name=name)

    ret = {}
    for qid in items:
        if qid not in graph:
            continue
        visited, queue = set(), [qid]
        result = []
        while queue:
            vertex = queue.pop(0)
            if vertex in visited:
                continue
            if vertex != qid:
                result.append(graph[vertex])
            visited.add(vertex)
            if ((vertex == qid or 'country' in graph[vertex]) and
                    'children' in graph[vertex]):
                queue.extend(graph[vertex]['children'] - visited)

        drop = set()
        for i in result[:]:
            if not (len(i.get('children', [])) == 1 and 'country' in i and
                    any(c.isupper() for c in i['label'])):
                continue
            child = graph[list(i['children'])[0]]['label']
            if i['label'].startswith(child):
                drop.add(i['qid'])
            else:
                i['label'] += f' ({child})'

        result = [i for i in result if i['qid'] not in drop]

        all_children = set()
        for i in result:
            if 'children' in i:
                all_children.update(i.pop('children'))
            if 'country' in i:
                del i['country']
        ret[qid] = [i for i in result if i['qid'] not in all_children]
    return ret

def item_types_graph(items, name=None):
    query_items = ' '.join(f'wd:{qid}' for qid in items)
    query = item_types_tree.replace('ITEMS', query_items)

    graph = {}
    for row in run_query(query, name=name, send_error_mail=False):
        item_qid = wd_to_qid(row['item'])
        type_qid = wd_to_qid(row['type'])
        if not item_qid or not type_qid:
            continue
        if type_qid not in graph:
            graph[type_qid] = {
                'qid': type_qid,
                'label': row['typeLabel']['value'],
                'children': set(),
            }
        if item_qid not in graph:
            graph[item_qid] = {
                'qid': item_qid,
                'label': row['itemLabel']['value'],
                'children': set(),
            }
        if 'country' in row and 'country' not in graph[item_qid]:
            country = row_qid_and_label(row, 'country')
            if country:
                graph[item_qid]['country'] = country

        graph[item_qid]['children'].add(type_qid)

    return graph

    return {i: row_qid_and_label(row, i)
            for i in ('item', 'type', 'country')}

def get_item_types(items, name=None):  # unused
    extra_types = extra_keys.keys() | {
        'Q102496',    # parish
        'Q28564',     # public library
        'Q856234',    # academic library
        'Q19860854',  # former building or structure
        'Q15893266',  # former entity
        'Q1761072',   # state park
        'Q17343829',  # unincorporated community
        'Q358',       # heritage site
        'Q486972',    # human settlement
        'Q7894959',   # University Technical College
        'Q22746',     # urban park
        'Q294440',    # public space
        'Q253019',    # Ortsteil
        'Q16895517',  # traction maintenance depot
        'Q10283556'   # motive power depot
        'Q1076486',   # sports venue
        'Q5435556',   # farm museum
        'Q756102',    # open-air museum
        'Q24699794',  # museum building
        'Q2516357',   # transport museum
        'Q2398990',   # technology museum
        'Q588140',    # science museum
        'Q1093436',   # computer museum
        'Q16735822'   # history museum
        'Q1595639',   # local museum
        'Q17431399',  # national museum
        'Q1320830',   # covered passageway
        'Q13866185',  # museum library
        'Q2588070',   # residential project
        'Q1329623',   # cultural centre
        'Q2087181',   # historic house museum
        'Q5773747',   # historic house
        'Q1343246',   # English country house
        'Q16884952',  # country house
        'Q1802963',   # mansion
        'Q3950',      # villa
        'Q1030403',   # navigable aqueduct
        'Q256020',    # inn
        'Q23002054',  # private not-for-profit educational institution
        'Q23002042',  # private educational institution
        'Q446901',    # nursing school
        'Q38723',     # higher education institution
        'Q1103285',   # clubhouse
        'Q1236680',   # gentlemen's club
        'Q41971160',  # subway tunnel
        'Q1311958',   # railway tunnel
        'Q2354973',   # road tunnel
        'Q915063',    # sorting office
        'Q853854',    # clock tower
        'Q3457526',   # local nature reserve
        'Q245016',    # military base
        'Q17350442',  # venue
        'Q39659461',  # shopping arcade
        'Q2304397',   # residential tower
        'Q39658032',  # open air shopping centre
        'Q13586493',  # outdoor pool
        'Q48635101',  # Jewish delicatessen
        'Q47012103',  # mixed-use building
        'Q4298922',   # mixed-use development
        'Q42929138',  # Tramlink stop
        'Q3640372',   # brewpub
        'Q42011297',  # puppet theatre
        'Q40553563',  # restaurant district
        'Q40531603',  # railway station footbridge
        'Q1068842',   # footbridge
        'Q39947311',  # warehouse district
        'Q39919471',  # former warehouse
        'Q39917125',  # railway building
        'Q39364723',  # hospital building
        'Q11755959',  # multi-storey urban building
        'Q30301606',  # cubic building
        'Q30301498',  # student housing
        'Q3661265',   # halls of residence
        'Q18658261',  # flower market
        'Q29905619',  # chalk pit
        'Q28148504',  # museum district
        'Q3710552',   # cultural district
        'Q28142754',  # food market
        'Q27702623',  # academic enclave
        'Q27095213',  # shopping district
        'Q27095194',  # bookshop neighbourhood
        'Q277760',    # gatehouse
        'Q15243209',  # historic district
        'Q9826',      # high school
        'Q159334',    # secondary school
        'Q9842',      # primary school
        'Q628179',    # trail
        'Q74047',     # ghost town
        'Q7894959',   # University Technical College
    }
    query_items = ' '.join(f'wd:{qid}' for qid in items)
    query_types = ' '.join(f'(wd:{qid})' for qid in extra_types)
    query = (item_types.replace('ITEMS', query_items)
                       .replace('TYPES', query_types))

    return {(wd_to_qid(row['item']), wd_to_qid(row['type']))
            for row in run_query(query, name=name)}

def find_superclasses(items, name=None):
    query_items = ' '.join(f'(wd:{qid})' for qid in items)
    query = subclasses.replace('ITEMS', query_items)

    return {(wd_to_qid(row['item']), wd_to_qid(row['type']))
            for row in run_query(query, name=name)}

def claim_value(claim):
    try:
        return claim['mainsnak']['datavalue']['value']
    except KeyError:
        pass

def country_iso_codes_from_qid(qid):
    item = WikidataItem.retrieve_item(qid)
    codes = [claim_value(c) for c in item.claims.get('P297')]
    codes += [claim_value(c) for c in item.claims.get('P298')]
    return codes

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

    def first_claim_value(self, key):
        return claim_value(self.claims[key][0])

    @property
    def has_coords(self):
        try:
            claim_value(self.claims['P625'][0])
        except (IndexError, KeyError):
            return False
        return True

    @property
    def has_earth_coords(self):
        if not self.has_coords:
            return
        globe = claim_value(self.claims['P625'][0])['globe']
        return globe == 'http://www.wikidata.org/entity/Q2'

    @property
    def coords(self):
        if not self.has_coords:
            return None, None
        c = claim_value(self.claims['P625'][0])
        return c['latitude'], c['longitude']

    @property
    def nrhp(self):
        try:
            nrhp = claim_value(self.claims['P649'][0])
        except (IndexError, KeyError):
            return
        if nrhp.isdigit():
            return nrhp

    def get_oql(self, criteria, radius):
        nrhp = self.nrhp
        if not criteria:
            return
        lat, lon = self.coords
        if lat is None or lon is None:
            return

        osm_filter = 'around:{},{:.5f},{:.5f}'.format(radius, lat, lon)

        union = []
        for tag_or_key in sorted(criteria):
            union += overpass.oql_from_wikidata_tag_or_key(tag_or_key, osm_filter)

        if nrhp:
            union += ['\n    {}({})["ref:nrhp"={}];'.format(t, osm_filter, nrhp)
            for t in ('node', 'way', 'rel')]

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

        # Parc naturel régional des marais du Cotentin et du Bessin (Q2138341)
        # is in more than 50 locations. The maximum entities in one request is 50.
        if len(located_in) > 50:
            return
        for location in get_entities(located_in):
            if 'labels' not in location:
                continue
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
            c = claim_value(country)['numeric-id']
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
        return dict(names_from_entity(self.entity))

    @property
    def is_a(self):
        return [isa['mainsnak']['datavalue']['value']['id']
                for isa in self.entity.get('claims', {}).get('P31', [])]

    @property
    def is_a_detail(self):
        return [WikidataItem.retrieve_item(qid) for qid in self.is_a]

    def is_proposed(self):
        '''is this a proposed building or structure (Q811683)?'''
        return 'Q811683' in self.is_a

    def criteria(self):
        items = {row['tag']['value'] for row in self.osm_keys}
        for is_a in self.is_a:
            items |= set(extra_keys.get(is_a, []))

        # On Wikidata the item for 'facility' (Q13226383), has an OSM key of
        # 'amenity'. This is too generic, so we discard it.
        items.discard('Key:amenity')
        return items

    def report_broken_wikidata_osm_tags(self):
        for row in self.osm_keys:
            if not any(row['tag']['value'].startswith(start) for start in ('Key:', 'Tag')):
                body = 'qid: {}\nrow: {}\n'.format(self.qid, repr(row))
                mail.send_mail('broken OSM tag in Wikidata', body)

    def find_nrhp_match(self, overpass_reply):
        nrhp = self.nrhp
        if not nrhp:
            return
        osm = [e for e in overpass_reply
               if e['tags'].get('ref:nrhp') == nrhp]
        if len(osm) == 1:
            return osm[0]

    def parse_item_query(self, criteria, overpass_reply):
        nrhp_match = self.find_nrhp_match(overpass_reply)
        if nrhp_match:
            return [(nrhp_match, None)]

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
