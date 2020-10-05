from flask import Blueprint, abort, jsonify
from .place import Place
from .model import Item
from .match import check_for_match
from . import wikidata, overpass, matcher, utils
from geopy.distance import distance

import operator

api_blueprint = Blueprint('api', __name__)

def api_overpass_error(data, error):
    data['error'] = error
    data['response'] = 'error'
    return data

def api_osm_list(existing, found):
    osm = []
    osm_lookup = {}
    for i in existing:
        index = (i['type'], i['id'])
        i['existing'] = True
        i['match'] = False
        osm.append(i)
        osm_lookup[index] = i
    for i in found:
        index = (i['type'], i['id'])
        if index in osm_lookup:
            osm_lookup[index]['match'] = True
            continue
        i['match'] = True
        i['existing'] = False
        osm.append(i)
    return osm

def api_get(wikidata_id, entity, radius):
    qid = f'Q{wikidata_id}'
    if not entity:
        abort(404)

    entity.remove_badges()  # don't need badges in API response

    wikidata_names = entity.names
    entity.trim_location_from_names(wikidata_names)
    entity.report_broken_wikidata_osm_tags()

    criteria = entity.criteria()

    item = Item.query.get(wikidata_id)
    if item:  # add criteria from the Item object
        criteria |= item.criteria

    criteria = wikidata.flatten_criteria(criteria)

    data = {
        'wikidata': {
            'item': qid,
            'labels': entity.labels,
            'aliases': entity.aliases,
            'sitelinks': entity.sitelinks,
        },
        'search': {
            'radius': radius,
            'criteria': sorted(criteria),
        },
        'found_matches': False,
    }

    if not entity.has_coords:
        return api_overpass_error(data, 'no coordinates')

    lat, lon = entity.coords
    data['wikidata']['lat'] = lat
    data['wikidata']['lon'] = lon

    oql = entity.get_oql(criteria, radius)

    try:
        existing = overpass.get_existing(qid)
    except overpass.RateLimited:
        return api_overpass_error(data, 'overpass rate limited')
    except overpass.Timeout:
        return api_overpass_error(data, 'overpass timeout')

    found = []
    if criteria:
        try:
            overpass_reply = overpass.item_query(oql, qid, radius)
        except overpass.RateLimited:
            return api_overpass_error(data, 'overpass rate limited')
        except overpass.Timeout:
            return api_overpass_error(data, 'overpass timeout')

        endings = matcher.get_ending_from_criteria({i.partition(':')[2] for i in criteria})
        found = [element for element in overpass_reply
                 if check_for_match(element['tags'], wikidata_names, endings=endings)]

    osm = api_osm_list(existing, found)

    for i in osm:
        coords = operator.itemgetter('lat', 'lon')(i.get('center', i))
        i['distance'] = int(distance(coords, (lat, lon)).m)

    data['response'] = 'ok'
    data['found_matches'] = bool(found)
    data['osm'] = osm

    return data

@api_blueprint.route('/api/1/place_items/<osm_type>/<osm_id>')
def api_place_items(osm_type, osm_id):
    place = Place.get_by_osm(osm_type, osm_id)
    items = [{'qid': item.qid, 'label': item.query_label} for item in place.items]

    return jsonify({
        'osm_type': osm_type,
        'osm_id': osm_id,
        'items': items,
    })

@api_blueprint.route('/api/1/item/Q<int:wikidata_id>')
def api_item_match(wikidata_id):
    '''API call: find matches for Wikidata item

    Optional parameter: radius (in metres)
    '''

    qid = f'Q{wikidata_id}'
    entity = wikidata.WikidataItem.retrieve_item(qid)
    data = api_get(wikidata_id, entity, utils.get_radius())

    response = jsonify(data)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

@api_blueprint.route('/api/1/names/Q<int:wikidata_id>')
def api_item_names(wikidata_id):

    qid = f'Q{wikidata_id}'
    entity = wikidata.WikidataItem.retrieve_item(qid)
    api_data = api_get(wikidata_id, entity, utils.get_radius())

    def json_data(**kwargs):
        response = jsonify(**kwargs)
        response.headers.add('Access-Control-Allow-Origin', '*')
        return response

    if not api_data.get('osm'):
        return json_data(found=False,
                         labels=False,
                         message='item not found in OSM')

    for osm in api_data['osm']:
        osm['names'] = {k[5:]: v for k, v in osm['tags'].items()
                        if k.startswith('name:')}
        osm['name_count'] = len(osm['names'])

    picked = max(api_data['osm'], key=lambda osm: osm['name_count'])
    osm_type, osm_id = picked['type'], picked['id']
    url = f'https://www.openstreetmap.org/{osm_type}/{osm_id}'

    data = {}
    if picked['name_count']:
        data['names'] = picked['names']
        data['labels'] = True
    else:
        data['message'] = 'no labels found in OSM'
        data['labels'] = False

    return json_data(found=True,
                     osm_type=osm_type,
                     osm_id=osm_id,
                     osm_url=url,
                     **data)
