import requests
from collections import defaultdict

def build_all_tags(entity_types):
    all_tags = defaultdict(set)
    for i in entity_types:
        for t in i['tags']:
            if '=' not in t:
                continue
            k, v = t.split('=')
            all_tags[k].add(v)
    return all_tags

def get_tags(entity_types):
    all_tags = build_all_tags(entity_types)
    params = {
        'tags': ','.join('{}={}'.format(k, ','.join(v)) for k, v in all_tags.items()),
        'format': 'json_pretty',
    }

    r = requests.get('https://taginfo.openstreetmap.org/api/4/tags/list', params=params)
    return r.json()['data']

def get_keys():
    params = {
        'page': 1,
        'rp': 200,
        'filter': 'in_wiki',
        'sortname': 'count_all',
        'sortorder': 'desc',
        'format': 'json_pretty',
    }

    url = 'https://taginfo.openstreetmap.org/api/4/keys/all'
    r = requests.get(url, params=params)
    return r.json()['data']

def get_taginfo(entity_types):
    tags = get_tags(entity_types)
    keys = get_keys()

    taginfo = {i['key']: {'count_all': i['count_all']} for i in keys}

    for i in tags:
        tag = i['key'] + '=' + i['value']
        taginfo[tag] = {'count_all': i['count_all']}
        if not i.get('wiki'):
            continue
        image = i['wiki'].get('en', {}).get('image')
        if not image:
            image = next((l['image'] for l in i['wiki'].values() if 'image' in l), None)
        if image:
            taginfo[tag]['image'] = image

    return taginfo

