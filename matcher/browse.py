from . import database, nominatim, wikidata
from .place import Place

def place_from_qid(qid, q=None, entity=None):
    if q is None:
        if entity is None:
            entity = wikidata.get_entity(qid)
        q = qid_to_search_string(qid, entity)

    hits = nominatim.lookup(q=q)
    for hit in hits:
        hit_qid = hit['extratags'].get('wikidata')
        if hit_qid != qid:
            continue
        return place_from_nominatim(hit)

def qid_to_search_string(qid, entity):
    isa = {i['mainsnak']['datavalue']['value']['id']
           for i in entity.get('claims', {}).get('P31', [])}

    if 'en' in entity['labels']:
        label = entity['labels']['en']['value']
    else:  # pick a label at random
        label = list(entity['labels'].values())[0]['value']

    country_or_bigger = {
        'Q5107',     # continent
        'Q6256',     # country
        'Q484652',   # international organization
        'Q855697',   # subcontinent
        'Q3624078',  # sovereign state
        'Q1335818',  # supranational organisation
        'Q4120211',  # regional organization
    }

    if isa & country_or_bigger:
        return label

    names = wikidata.up_one_level(qid)
    if not names:
        return label
    country = names['country_name'] or names['up_country_name']

    q = names['name']
    if names['up']:
        q += ', ' + names['up']
    if country and country != names['up']:
        q += ', ' + country
    return q

def place_from_nominatim(hit):
    if not ('osm_type' in hit and 'osm_id' in hit):
        return
    p = Place.query.filter_by(osm_type=hit['osm_type'],
                              osm_id=hit['osm_id']).one_or_none()
    if p:
        p.update_from_nominatim(hit)
    else:
        p = Place.from_nominatim(hit)
        database.session.add(p)
    database.session.commit()
    return p
