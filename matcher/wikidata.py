from flask import render_template_string
from urllib.parse import unquote
from collections import defaultdict
from .utils import chunk, drop_start
import requests
from . import user_agent_headers

page_size = 50

wikidata_query = '''
SELECT ?place (SAMPLE(?location) AS ?location) ?article ?end ?point_in_time WHERE {
    SERVICE wikibase:box {
        ?place wdt:P625 ?location .
        bd:serviceParam wikibase:cornerWest "Point({{ west }} {{ south }})"^^geo:wktLiteral .
        bd:serviceParam wikibase:cornerEast "Point({{ east }} {{ north }})"^^geo:wktLiteral .
    }
    ?article schema:about ?place .
    ?article schema:inLanguage "en" .
    ?article schema:isPartOf <https://en.wikipedia.org/> .
    OPTIONAL { ?place wdt:P582 ?end . }
    OPTIONAL { ?place wdt:P585 ?point_in_time . }
}
GROUP BY ?place ?article ?end ?point_in_time
'''

wikidata_point_query = '''
SELECT ?place (SAMPLE(?location) AS ?location) ?article ?end ?point_in_time WHERE {
    SERVICE wikibase:around {
        ?place wdt:P625 ?location .
        bd:serviceParam wikibase:center "Point({{ lon }} {{ lat }})"^^geo:wktLiteral .
        bd:serviceParam wikibase:radius "{{ '{:.1f}'.format(radius) }}" .
    }
    ?article schema:about ?place .
    ?article schema:inLanguage "en" .
    ?article schema:isPartOf <https://en.wikipedia.org/> .
    OPTIONAL { ?place wdt:P582 ?end . }
    OPTIONAL { ?place wdt:P585 ?point_in_time . }
}
GROUP BY ?place ?article ?end ?point_in_time
'''

wikidata_subclass_osm_tags = '''
SELECT DISTINCT ?item ?itemLabel ?tag
WHERE
{
  wd:{{qid}} wdt:P31/wdt:P279* ?item .
  {
  ?item wdt:P1282 ?tag .
  } UNION {
  ?item wdt:P641 ?sport .
  ?sport wdt:P1282 ?tag
  } UNION {
  ?item wdt:P140 ?religion .
  ?religion wdt:P1282 ?tag
  } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}'''

def get_query(south, north, west, east):
    return render_template_string(wikidata_query,
                                  south=south,
                                  north=north,
                                  west=west,
                                  east=east)

def get_point_query(lat, lon, radius):
    return render_template_string(wikidata_point_query,
                                  lat=lat,
                                  lon=lon,
                                  radius=float(radius) / 1000.0)

def run_query(query):
    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
    r = requests.get(url,
                     params={'query': query, 'format': 'json'},
                     headers=user_agent_headers())
    assert r.status_code == 200
    return r.json()['results']['bindings']

def parse_query(query):
    wd = 'http://www.wikidata.org/entity/Q'
    enwiki = 'https://en.wikipedia.org/wiki/'
    return [{
        'location': i['location']['value'],
        'id': int(drop_start(i['place']['value'], wd)),
        'enwiki': unquote(drop_start(i['article']['value'], enwiki)),
    } for i in query]

def entity_iter(ids):
    wikidata_url = 'https://www.wikidata.org/w/api.php'
    params = {
        'format': 'json',
        'formatversion': 2,
        'action': 'wbgetentities',
    }
    for cur in chunk(ids, page_size):
        params['ids'] = '|'.join(cur)
        json_data = requests.get(wikidata_url,
                                 params=params,
                                 headers=user_agent_headers()).json()
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
    return list(json_data['entities'].values())[0]

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

def names_from_entity(entity, skip_lang={'ar', 'arc', 'pl'}):
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

    return ret

def osm_key_query(qid):
    return render_template_string(wikidata_subclass_osm_tags, qid=qid)

def get_osm_keys(query):
    r = requests.get('https://query.wikidata.org/bigdata/namespace/wdq/sparql',
                     params={'query': query, 'format': 'json'},
                     headers=user_agent_headers())
    assert r.status_code == 200
    return r.json()['results']['bindings']
