import requests
import os.path
import json
from .utils import chunk, drop_start
from .wikidata import wikidata_items

def get_cats(titles):
    query_url = 'https://en.wikipedia.org/w/api.php'
    params = {
        'format': 'json',
        'formatversion': 2,
        'action': 'query',
        'continue': '',
        'prop': 'categories',
        'titles': '|'.join(titles),
        'cllimit': 'max',
        'clshow': '!hidden',
    }
    r = requests.get(query_url, params=params)
    json_reply = r.json()
    return json_reply['query']['pages']

def get_items_with_cats(osm_id):
    filename = 'cache/{}_items_with_cats.json'.format(osm_id)
    if os.path.exists(filename):
        return json.load(open(filename))

    items = wikidata_items(osm_id)
    for cur in chunk(items.keys(), 50):
        # print(cur[0])
        for page in get_cats(cur):
            if 'categories' not in page:  # redirects
                continue
            title = page['title']
            cats = [drop_start(cat['title'], 'Category:') for cat in page['categories']]
            items[title]['cats'] = cats

    out = open(filename, 'w')
    json.dump(items, out, indent=2)
    return items
