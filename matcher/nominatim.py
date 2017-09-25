from flask import current_app
import requests
from . import user_agent_headers
import simplejson

class SearchError(Exception):
    pass

def lookup_with_params(**kwargs):
    url = 'http://nominatim.openstreetmap.org/search'

    params = {
        'format': 'jsonv2',
        'addressdetails': 1,
        'email': current_app.config['ADMIN_EMAIL'],
        'extratags': 1,
        'limit': 20,
        'namedetails': 1,
        'accept-language': 'en',
        'polygon_text': 1,
    }
    params.update(kwargs)
    r = requests.get(url, params=params, headers=user_agent_headers())
    if r.status_code == 500:
        raise SearchError

    try:
        return r.json()
    except simplejson.scanner.JSONDecodeError:
        raise SearchError

def lookup(q):
    return lookup_with_params(q=q)

def get_us_county(name, state):
    if ' ' not in name and 'county' not in name:
        name += ' county'
    results = lookup(q='{}, {}'.format(name, state))

    def pred(hit):
        return (hit['osm_type'] != 'node' and
                'county' in hit['display_name'].lower())
    return next(filter(pred, results), None)

def get_us_city(name, state):
    results = lookup_with_params(city=name, state=state)
    if len(results) != 1:
        results = [hit for hit in results
                   if hit['type'] == 'city' or hit['osm_type'] == 'node']
        if len(results) != 1:
            print('more than one')
            return
    hit = results[0]
    if hit['type'] not in ('administrative', 'city'):
        print('not a city')
        return
    if hit['osm_type'] == 'node':
        print('node')
        return
    if not hit['display_name'].startswith(name):
        print('wrong name')
        return
    assert ('osm_type' in hit and 'osm_id' in hit and 'geotext' in hit)
    return hit
