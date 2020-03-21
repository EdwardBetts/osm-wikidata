import requests
import requests.exceptions
import time
import simplejson.errors
from .utils import chunk
from . import user_agent_headers, mail

wikidata_url = 'https://www.wikidata.org/w/api.php'
page_size = 50

class TooManyEntities(Exception):
    pass

class QueryError(Exception):
    def __init__(self, query, r):
        self.query = query
        self.r = r

class QueryTimeout(QueryError):
    def __init__(self, query, r):
        self.query = query
        self.r = r

def api_call(params):
    call_params = {
        'format': 'json',
        'formatversion': 2,
        **params,
    }

    r = requests.get(wikidata_url,
                     params=call_params,
                     headers=user_agent_headers())
    return r

def entity_iter(ids, debug=False, attempts=5):
    for num, cur in enumerate(chunk(ids, page_size)):
        if debug:
            print('entity_iter: {}/{}'.format(num * page_size, len(ids)))
        ids = '|'.join(cur)
        for attempt in range(attempts):
            try:
                r = api_call({'action': 'wbgetentities', 'ids': ids})
                break
            except requests.exceptions.ChunkedEncodingError:
                if attempt == attempts - 1:
                    raise
                time.sleep(1)
        r.raise_for_status()
        json_data = r.json()
        if 'entities' not in json_data:
            mail.send_mail('error fetching wikidata entities', r.text)

        for qid, entity in json_data['entities'].items():
            yield qid, entity

def get_entity(qid):
    json_data = api_call({'action': 'wbgetentities', 'ids': qid}).json()

    try:
        entity = list(json_data['entities'].values())[0]
    except KeyError:
        return
    if 'missing' not in entity:
        return entity

def get_lastrevid(qid):
    params = {'action': 'query', 'prop': 'info', 'titles': qid}
    return api_call(params).json()['query']['pages'][0]['lastrevid']

def get_lastrevids(qid_list):
    if not qid_list:
        return {}
    params = {'action': 'query', 'prop': 'info', 'titles': '|'.join(qid_list)}
    r = api_call(params)
    json_data = r.json()
    if 'query' not in json_data:
        print(r.text)
    return {page['title']: page['lastrevid'] for page in json_data['query']['pages']}

def get_entities(ids, attempts=5):
    if not ids:
        return []
    if len(ids) > 50:
        raise TooManyEntities
    params = {'action': 'wbgetentities', 'ids': '|'.join(ids)}
    for attempt in range(attempts):
        try:  # retry if we get a ChunkedEncodingError
            r = api_call(params)
            try:
                json_data = r.json()
            except simplejson.errors.JSONDecodeError:
                raise QueryError(params, r)
            return list(json_data['entities'].values())
        except requests.exceptions.ChunkedEncodingError:
            if attempt == attempts - 1:
                raise QueryError(params, r)
