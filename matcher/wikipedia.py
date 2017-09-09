import requests
import lxml.html
import simplejson
from .utils import chunk, drop_start
from . import user_agent_headers

page_size = 50
extracts_page_size = 20
query_url = 'https://{}.wikipedia.org/w/api.php'


def run_query(titles, params, language_code='en'):
    base = {
        'format': 'json',
        'formatversion': 2,
        'action': 'query',
        'continue': '',
        'titles': '|'.join(titles),
    }
    p = base.copy()
    p.update(params)

    url = query_url.format(language_code)
    r = requests.get(url, params=p, headers=user_agent_headers())
    expect = 'application/json; charset=utf-8'
    assert r.status_code == 200 and r.headers['content-type'] == expect
    json_reply = r.json()
    return json_reply['query']['pages']

def get_cats(titles, language_code='en'):
    params = {'prop': 'categories', 'cllimit': 'max', 'clshow': '!hidden'}
    return run_query(titles, params, language_code)

def get_coords(titles, language_code='en'):
    return run_query(titles, {'prop': 'coordinates'}, language_code)

def page_category_iter(titles):
    for cur in chunk(titles, page_size):
        for page in get_cats(cur):
            if 'categories' not in page:  # redirects
                continue
            cats = [drop_start(cat['title'], 'Category:')
                    for cat in page['categories']]
            yield (page['title'], cats)

def get_items_with_cats(items):
    assert isinstance(items, dict)
    for cur in chunk(items.keys(), page_size):
        for page in get_cats(cur):
            if 'categories' not in page:  # redirects
                continue
            cats = [drop_start(cat['title'], 'Category:')
                    for cat in page['categories']]
            items[page['title']]['cats'] = cats

def html_names(article):
    if article.strip() == '':
        return []
    root = lxml.html.fromstring(article)
    # avoid picking pronunciation guide bold text
    # <small title="English pronunciation respelling"><i><b>MAWD</b>-lin</i></small>
    return [b.text_content() for b in root.xpath('.//b[not(ancestor::small)]')]

def extracts_query(titles, language_code='en'):
    params = {
        'prop': 'extracts',
        'exlimit': extracts_page_size,
        'exintro': '1',
    }
    return run_query(titles, params, language_code)

def get_extracts(titles):
    for cur in chunk(titles, extracts_page_size):
        for page in extracts_query(cur):
            if 'extract' not in page:
                continue
            extract = page['extract'].strip()
            if extract:
                yield (page['title'], page['extract'])

