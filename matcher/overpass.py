from flask import current_app
from .utils import load_from_cache
import os.path
import requests
import json

def generate_oql(osm_id, tags):
    item = load_from_cache('{}_nominatim.json'.format(osm_id))
    (south, north, west, east) = map(float, item['boundingbox'])
    bbox = ','.join('{}'.format(i) for i in (south - 0.01, west - 0.01, north + 0.01, east + 0.01))
    union = []
    # optimisation: we only expect type=site or site=<something> on relations
    for tag in tags:
        relation_only = tag == 'site'
        if '=' in tag:
            k, _, v = tag.partition('=')
            if k == 'site' or tag == 'type=site':
                relation_only = True
            tag = '"{}"="{}"'.format(k, v)
        for t in ('rel',) if relation_only else ('node', 'way', 'rel'):
            union.append('\n    {}(area.a)[{}][name];'.format(t, tag))
    area_id = 3600000000 + int(osm_id)
    oql = '''[timeout:600][out:xml][bbox:{}];
area({})->.a;
({});
(._;>;);
out qt;'''.format(bbox, area_id, ''.join(union))
    return oql

def overpass_filename(osm_id):
    overpass_dir = current_app.config['OVERPASS_DIR']
    return os.path.join(overpass_dir, '{}.xml'.format(osm_id))

def overpass_done(osm_id):
    return os.path.exists(overpass_filename(osm_id))

def get_from_overpass(osm_id, oql):
    return  # unused
    filename = overpass_filename(osm_id)
    if os.path.exists(filename):
        return open(filename, 'rb').read()

    overpass_url = 'http://overpass-api.de/api/interpreter'
    r = requests.post(overpass_url, data=oql, stream=True)
    chunk_size = 1024 * 10
    with open(filename, 'wb') as f:
        for overpass_chunk in r.iter_content(chunk_size):
            f.write(overpass_chunk)
    return open(filename).read()
