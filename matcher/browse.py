from . import database, nominatim, wikidata, wikidata_api, wikidata_language
from .place import Place
from .model import WikidataItem, IsA
from time import time

def place_from_qid(qid, q=None, entity=None):
    hit = hit_from_qid(qid, q=None, entity=None)
    if hit:
        return place_from_nominatim(hit)

def hit_from_qid(qid, q=None, entity=None):
    if q is None:
        if entity is None:
            entity = wikidata_api.get_entity(qid)
        q = qid_to_search_string(qid, entity)

    hits = nominatim.lookup(q=q)
    for hit in hits:
        hit_qid = hit['extratags'].get('wikidata')
        if hit_qid != qid:
            continue
        return hit

def qid_to_search_string(qid, entity):
    isa = {i['mainsnak']['datavalue']['value']['id']
           for i in entity.get('claims', {}).get('P31', [])}

    label = wikidata.entity_label(entity)

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

def get_details(item_id, timing=None, lang=None, sort=None):
    if timing is None:
        timing = []
    qid = f'Q{item_id}'
    place = Place.get_by_wikidata(qid)
    check_lastrevid = []

    item = WikidataItem.query.get(item_id)
    items = {}
    if item:
        check_lastrevid.append((qid, item.rev_id))
    else:
        item = WikidataItem.download(item_id)
    items[qid] = item

    lang_qids = wikidata_language.get_lang_qids(item.entity)

    if not lang_qids and 'P17' in item.entity['claims']:
        for c in item.entity['claims']['P17']:
            if 'datavalue' not in c['mainsnak']:
                continue
            country_qid = c['mainsnak']['datavalue']['value']['id']
            country_item_id = c['mainsnak']['datavalue']['value']['numeric-id']
            country = WikidataItem.query.get(country_item_id)
            if country:
                check_lastrevid.append((country_qid, country.rev_id))
            else:
                country = WikidataItem.download(country_item_id)
            items[country_qid] = country

            for lang_qid in wikidata_language.get_lang_qids(country.entity):
                if lang_qid not in lang_qids:
                    lang_qids.append(lang_qid)

    for lang_qid in lang_qids:
        lang_item_id = int(lang_qid[1:])
        lang_item = WikidataItem.query.get(lang_item_id)
        if lang_item:
            check_lastrevid.append((lang_qid, lang_item.rev_id))
        else:
            lang_item = WikidataItem.download(lang_item_id)
        items[lang_qid] = lang_item

    if check_lastrevid:
        check_qids = [check_qid for check_qid, rev_id in check_lastrevid]
        cur_rev_ids = wikidata_api.get_lastrevids(check_qids)
        for check_qid, rev_id in check_lastrevid:
            if cur_rev_ids[check_qid] > rev_id:
                items[check_qid].update()

    lang_items = (items[lang_qid].entity for lang_qid in lang_qids)
    languages = wikidata_language.process_language_entities(lang_items)

    entity = item.entity
    timing.append(('get entity done', time()))

    if languages and not any(l.get('code') == 'en' for l in languages):
        languages.append({'code': 'en', 'local': 'English', 'en': 'English'})

    if not lang and languages:
        for l in languages:
            if 'code' not in l:
                continue
            lang = l['code']
            break

    if not lang:
        lang = 'en'

    if not place:
        place = place_from_qid(qid, entity=entity)

    name = wikidata.entity_label(entity, language=lang)
    rows = wikidata.next_level_places(qid, entity, language=lang)
    timing.append(('next level places done', time()))

    if qid == 'Q21':
        types = wikidata.next_level_types(['Q48091'])
        query = (wikidata.next_level_query2
                    .replace('TYPES', types)
                    .replace('QID', qid)
                    .replace('LANGUAGE', lang))
        extra_rows = wikidata.next_level_places(qid, entity, language=lang, query=query)
        kwargs = {
            'extra_type_label': 'Regions of England',
            'extra_type_places': extra_rows,
        }
    else:
        extra_rows = []
        kwargs = {}

    timing.append(('start isa map', time()))
    isa_map = {}
    download_isa = set()
    for row in rows + extra_rows:
        for isa_qid in row['isa']:
            if isa_qid in isa_map:
                continue
            isa_obj = IsA.query.get(isa_qid[1:])
            isa_map[isa_qid] = isa_obj
            if isa_obj and isa_obj.entity:
                continue
            download_isa.add(isa_qid)

    for isa_qid, entity in wikidata_api.entity_iter(download_isa):
        if isa_map[isa_qid]:
            isa_map[isa_qid].entity = entity
            continue
        isa_obj = IsA(item_id=isa_qid[1:], entity=entity)
        isa_map[isa_qid] = isa_obj
        database.session.add(isa_obj)
    if download_isa:
        database.session.commit()
    timing.append(('isa map done', time()))

    if sort and sort in {'area', 'population', 'qid', 'label'}:
        rows.sort(key=lambda i: i[sort] if i[sort] else 0)

    former_type = {isa_qid for isa_qid, isa in isa_map.items()
                   if 'former' in isa.entity_label().lower() or
                      'historical' in isa.entity_label().lower()}

    current_places = [row for row in rows if not (set(row['isa']) & former_type)]
    former_places = [row for row in rows if set(row['isa']) & former_type]

    return {
        'qid': qid,
        'item_id': item_id,
        'place': place,
        'name': name,
        'entity': entity,
        'languages': languages,
        'current_places': current_places,
        'former_places': former_places,
        'isa_map': isa_map,
        **kwargs
    }
