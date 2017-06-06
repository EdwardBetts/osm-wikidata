from flask import current_app
import requests
from . import user_agent_headers
import simplejson

class SearchError(Exception):
    pass

def lookup(q):
    url = 'http://nominatim.openstreetmap.org/search'

    params = {
        'q': q,
        'format': 'jsonv2',
        'addressdetails': 1,
        'email': current_app.config['ADMIN_EMAIL'],
        'extratags': 1,
        'limit': 20,
        'namedetails': 1,
        'accept-language': 'en',
        'polygon_text': 1,
    }
    r = requests.get(url, params=params, headers=user_agent_headers())
    if r.status_code == 500:
        raise SearchError

    try:
        return r.json()
    except simplejson.scanner.JSONDecodeError:
        raise SearchError
