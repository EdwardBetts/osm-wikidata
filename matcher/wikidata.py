from flask import render_template_string
from urllib.parse import unquote
from .utils import chunk, cache_filename
import requests
import os.path
import json

wikidata_query = '''
SELECT ?place ?placeLabel ?lat ?lon ?article ?end WHERE {
    SERVICE wikibase:box {
        ?place wdt:P625 ?location .
        bd:serviceParam wikibase:cornerWest "Point({{ west }} {{ south }})"^^geo:wktLiteral .
        bd:serviceParam wikibase:cornerEast "Point({{ east }} {{ north }})"^^geo:wktLiteral .
    }
    ?place p:P625 ?statement .
    ?statement psv:P625 ?coordinate_node .
    ?coordinate_node wikibase:geoLatitude ?lat .
    ?coordinate_node wikibase:geoLongitude ?lon .
    ?article schema:about ?place .
    ?article schema:inLanguage "en" .
    ?article schema:isPartOf <https://en.wikipedia.org/> .
    OPTIONAL {
        ?place wdt:P582 ?end .
    }
    SERVICE wikibase:label {
        bd:serviceParam wikibase:language "en"
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
    r = requests.get(url, params={'query': query, 'format': 'json'})
    assert r.status_code == 200
    return r

def parse_query(query):
    wd_start = 'http://www.wikidata.org/entity/'
    enwp_start = 'https://en.wikipedia.org/wiki/'
    items = {}
    for i in query:
        wd_uri = i['place']['value']
        enwp_uri = i['article']['value']
        assert wd_uri.startswith(wd_start)
        assert enwp_uri.startswith(enwp_start)
        qid = wd_uri[len(wd_start):]
        enwp = unquote(enwp_uri[len(enwp_start):])
        item = {
            'wikidata_uri': wd_uri,
            'lat': i['lat']['value'],
            'lon': i['lon']['value'],
            'qid': qid,
            'label': i['placeLabel']['value'],
        }
        items[enwp] = item
    return items

def wbgetentities(items):
    wikidata_url = 'https://www.wikidata.org/w/api.php'
    params = {
        'format': 'json',
        'formatversion': 2,
        'action': 'wbgetentities',
    }
    # only items with tags
    with_tags = (item for item in items.values() if item.get('tags'))
    for cur in chunk(with_tags, 50):
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
