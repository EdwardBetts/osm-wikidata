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

wikidata_subclass_osm_tags = '''
SELECT ?item ?label ?tag
WHERE
{
  wd:{{qid}} wdt:P31/wdt:P279* ?item .
  ?item wdt:P1282 ?tag
  OPTIONAL {
     ?item rdfs:label ?label filter (lang(?label) = "en").
   }
 }'''


def get_query(south, north, west, east):
    return render_template_string(wikidata_query,
                                  south=south,
                                  north=north,
                                  west=west,
                                  east=east)


def run_query(south, north, west, east):
    query = get_query(south, north, west, east)

    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
    r = requests.get(url,
                     params={'query': query, 'format': 'json'},
                     headers=user_agent_headers())
    assert r.status_code == 200
    return r

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

    if len(entity['sitelinks']) < 6 and len(entity['labels']) < 6:
        for lang, value_list in entity.get('aliases', {}).items():
            if lang in skip_lang:
                continue
            if len(value_list) > 3:
                continue
            for name in value_list:
                ret[name['value']].append(('alias', lang))

    return ret

def get_osm_keys(qid):
    query = render_template_string(wikidata_subclass_osm_tags, qid=qid)
    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
    r = requests.get(url,
                     params={'query': query, 'format': 'json'},
                     headers=user_agent_headers())
    assert r.status_code == 200
    return r
