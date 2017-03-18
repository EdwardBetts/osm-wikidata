from flask import render_template_string
from urllib.parse import unquote
from collections import defaultdict
from .utils import chunk, drop_start
import requests

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

def get_query(south, north, west, east):
    return render_template_string(wikidata_query,
                                  south=south,
                                  north=north,
                                  west=west,
                                  east=east)


def run_query(south, north, west, east):
    query = get_query(south, north, west, east)

    url = 'https://query.wikidata.org/bigdata/namespace/wdq/sparql'
    r = requests.get(url, params={'query': query, 'format': 'json'})
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
        json_data = requests.get(wikidata_url, params=params).json()
        for qid, entity in json_data['entities'].items():
            yield qid, entity

def wbgetentities(items):
    wikidata_url = 'https://www.wikidata.org/w/api.php'
    params = {
        'format': 'json',
        'formatversion': 2,
        'action': 'wbgetentities',
    }
    # only items with tags
    with_tags = (item for item in items.values() if item.get('tags'))
    for cur in chunk(with_tags, page_size):
        params['ids'] = '|'.join(item['qid'] for item in cur)
        r = requests.get(wikidata_url, params=params, timeout=90)
        # print(cur[0]['qid'], cur[0]['label'])
        json_data = r.json()
        for qid, entity in json_data['entities'].items():
            claims = entity['claims']
            instanceof = [i['mainsnak']['datavalue']['value']['numeric-id']
                          for i in claims.get('P31', [])]
            aliases = {lang: [i['value'] for i in value_list]
                       for lang, value_list in entity.get('aliases', {}).items()}
            labels = {lang: v['value']
                      for lang, v in entity['labels'].items()}
            sitelinks = {site: v['title']
                         for site, v in entity['sitelinks'].items()}
            enwp = sitelinks['enwiki']
            if instanceof:
                items[enwp]['instanceof'] = instanceof
            if aliases:
                items[enwp]['aliases'] = aliases
            if labels:
                items[enwp]['labels'] = labels
            items[enwp]['sitelinks'] = sitelinks

def names_from_entity(entity, skip_lang={'ar', 'arc', 'pl'}):
    if not entity:
        return

    ret = defaultdict(list)

    for k, v in entity['labels'].items():
        if k in skip_lang:
            continue
        ret[v['value']].append(('label', k))

    for k, v in entity['sitelinks'].items():
        if k + 'wiki' in skip_lang:
            continue
        ret[v['title']].append(('sitelink', k))

    if len(entity['sitelinks']) < 6 and len(entity['labels']) < 6:
        for lang, value_list in entity.get('aliases', {}).items():
            if lang in skip_lang:
                continue
            if len(value_list) > 3:
                continue
            for name in value_list:
                ret[name['value']].append(('alias', lang))

    return ret
