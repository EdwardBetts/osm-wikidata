from flask import current_app
from collections import Counter, defaultdict
from . import match

import os.path
import json
import re

bad_name_fields = {'tiger:name_base', 'old_name', 'name:right', 'name:left',
                   'gnis:county_name', 'openGeoDB:name'}

cat_to_ending = {}
patterns = {}
entity_types = {}

def get_pattern(key):
    if key in patterns:
        return patterns[key]
    return patterns.setdefault(key, re.compile(r'\b' + re.escape(key) + r'\b', re.I))

def categories_to_tags(categories):
    cat_to_entity = build_cat_map()
    tags = set()
    for cat in categories:
        lc_cat = cat.lower()
        for key, value in cat_to_entity.items():
            if not get_pattern(key).search(lc_cat):
                continue
            exclude = value.get('exclude_cats')
            if exclude:
                pattern = re.compile(r'\b(' + '|'.join(re.escape(e) for e in exclude) + r')\b', re.I)
                if pattern.search(lc_cat):
                    continue
            tags |= set(value['tags'])
    return sorted(tags)

def categories_to_tags_map(categories):
    cat_to_entity = build_cat_map()
    ret = defaultdict(set)
    for cat in categories:
        lc_cat = cat.lower()
        for key, value in cat_to_entity.items():
            if not get_pattern(key).search(lc_cat):
                continue
            exclude = value.get('exclude_cats')
            if exclude:
                pattern = re.compile(r'\b(' + '|'.join(re.escape(e) for e in exclude) + r')\b', re.I)
                if pattern.search(lc_cat):
                    continue
            ret[cat] |= set(value['tags'])
    return ret

def load_entity_types():
    data_dir = current_app.config['DATA_DIR']
    filename = os.path.join(data_dir, 'entity_types.json')
    return json.load(open(filename))

def simplify_tags(tags):
    key_only = sorted(t for t in tags if '=' not in t)
    for k in key_only:
        for t in set(tags):
            if t.startswith(k + '='):
                tags.remove(t)
    return tags

def build_cat_map():
    cat_to_entity = {}
    for i in load_entity_types():
        for c in i['cats']:
            lc_cat = c.lower()
            if ' by ' in lc_cat:
                lc_cat = lc_cat[:lc_cat.find(' by ')]
            cat_to_entity[lc_cat] = i
    return cat_to_entity

def build_cat_to_ending():  # unused?
    global cat_to_ending

    if cat_to_ending:
        return cat_to_ending

    for i in load_entity_types():
        trim = {x.replace(' ', '').lower() for x in i['trim']}
        for c in i['cats']:
            lc_cat = c.lower()
            if ' by ' in lc_cat:
                lc_cat = lc_cat[:lc_cat.find(' by ')]
            cat_to_ending[lc_cat] = trim

    return cat_to_ending

def get_ending_from_criteria(tags):
    global entity_types

    if not entity_types:
        entity_types = load_entity_types()
    tags = set(tags)

    endings = set()
    for t in entity_types:
        if tags & set(t['tags']):
            endings.update(t.get('trim'))

    return endings

def find_item_matches(cur, item, prefix, debug=False):
    if not item or not item.entity:
        return []
    cats = item.categories or []

    # point = "ST_GeomFromEWKT('{}')".format(item.ewkt)
    point = "ST_TRANSFORM(ST_GeomFromEWKT('{}'), 3857)".format(item.ewkt)

    # item_max_dist = max(max_dist[cat] for cat in item['cats'])
    item_max_dist = 4  # FIXME

    hstore = item.hstore_query
    if not hstore:
        return []

    sql_list = []
    for obj_type in 'point', 'line', 'polygon':
        obj_sql = ('select \'{}\', osm_id, name, tags, '
                   'ST_Distance({}, way) as dist '
                   'from {}_{} '
                   'where ST_DWithin({}, way, {} * 1000)').format(obj_type, point, prefix, obj_type, point, item_max_dist)
        sql_list.append(obj_sql)
    sql = 'select * from (' + ' union '.join(sql_list) + ') a where ({}) order by dist'.format(hstore)

    if debug:
        print(sql)

    cur.execute(sql)
    rows = cur.fetchall()
    seen = set()

    endings = get_ending_from_criteria(set(item.tag_list))

    wikidata_names = item.names()

    candidates = []
    for osm_num, (src_type, src_id, osm_name, osm_tags, dist) in enumerate(rows):
        (osm_type, osm_id) = get_osm_id_and_type(src_type, src_id)
        if (obj_type, osm_id) in seen:
            continue
        if debug:
            print((osm_type, osm_id, osm_name, osm_tags, dist))
        seen.add((obj_type, osm_id))

        try:
            admin_level = int(osm_tags['admin_level']) if 'admin_level' in osm_tags else None
        except Exception:
            admin_level = None
        names = {k: v for k, v in osm_tags.items()
                 if 'name' in k and k not in bad_name_fields}

        if any(c.startswith('Cities ') for c in cats) and admin_level == 10:
            continue
        if not names:
            continue

        m = match.check_for_match(osm_tags, wikidata_names, endings)
        if not m:
            continue
        candidate = {
            'osm_type': osm_type,
            'osm_id': osm_id,
            'name': osm_name,
            'tags': osm_tags,
            'dist': dist,
            # 'match': match.match_type.name,
            'planet_table': src_type,
            'src_id': src_id,
        }
        candidates.append(candidate)
    return candidates

def get_osm_id_and_type(source_type, source_id):
    if source_type == 'point':
        return ('node', source_id)
    if source_id > 0:
        return ('way', source_id)
    return ('relation', -source_id)

def planet_table_id(osm):
    if osm['type'] == 'node':
        return ('point', osm['id'])
    table = 'polygon' if 'way_area' in osm['tags'] else 'line'
    return (table, osm['id'] if osm['type'] == 'way' else -osm['id'])

def get_biggest_polygon(item):
    biggest = None
    biggest_size = None
    for osm in item['candidates']:
        if osm['type'] not in {'way', 'relation'}:
            continue
        if 'way_area' not in osm['tags']:
            continue
        area = float(osm['tags']['way_area'])
        if biggest is None or area > biggest_size:
            biggest_size = area
            biggest = osm

    return -osm['id'] if osm['type'] == 'relation' else osm['id']

def all_in_one(item, conn, prefix):
    cur = conn.cursor()
    biggest = get_biggest_polygon(item)
    if not biggest:
        return
    sql_list = []

    for table in 'point', 'line', 'polygon':
        id_list = ','.join(str(osm['src_id']) for osm in item['candidates']
                       if osm['table'] == table and (table == 'point' or osm['src_id'] != biggest))

        if not id_list:
            continue
        obj_sql = ('select \'{}\' as t, osm_id, way '
                   'from {}_{} '
                   'where osm_id in ({})').format(table, table, prefix, id_list)
        sql_list.append(obj_sql)

    if not sql_list:
        return
    sql = 'select ST_Within(a.way, b.way) from (' + ' union '.join(sql_list) + ') a, {}_polygon b where b.osm_id={}'.format(prefix, biggest)
    cur.execute(sql)
    if all(row[0] for row in cur.fetchall()):
        return biggest

def filter_candidates(items, conn):  # unused?
    assert isinstance(items, list)
    for item in items[:]:
        candidates = item['candidates']
        for osm in candidates:
            osm['table'], osm['src_id'] = planet_table_id(osm)

        for line in candidates[:]:
            if line['table'] == 'line':
                if any(poly['table'] == 'polygon' and poly['src_id'] == line['src_id'] for poly in candidates):
                    candidates.remove(line)

        if not candidates:
            items.remove(item)
            continue

        item['candidates'] = candidates
        if len(candidates) == 1:
            continue

        re_place_cat = re.compile(r'\b(Districts|Areas|Cities|Towns|Villages|Airports)\b', re.I)
        if any(re_place_cat.search(cat) for cat in item['cats']):
            nodes = [osm for osm in candidates if osm['type'] == 'node']
            if len(nodes) == 1:
                candidates = nodes

        if len(candidates) == 1:
            item['candidates'] = candidates
            continue

        big = all_in_one(item, conn)
        if big:
            for osm in candidates:
                if osm['table'] == 'polygon' and osm['src_id'] == big:
                    candidates = [osm]
                    break
        if len(candidates) == 1:
            item['candidates'] = candidates
            continue
    return items

def filter_place(candidates):
    # FIXME: some places are more complex, Cambridge for example
    types = {c.osm_type for c in candidates}
    place_node = None
    other = False
    if len(candidates) < 2 or 'node' not in types:
        return
    if len(types) < 2:
        return

    for c in candidates:
        if c.osm_type == 'node':
            if 'place' in c.tags:
                place_node = c
        else:
            if 'admin_level' in c.tags or c.tags.get('landuse') == 'residential':
                other = True

    if place_node and other:
        return place_node

def filter_schools(candidates):
    if len(candidates) < 2:
        return
    if all('amenity=school' not in c.matching_tags() for c in candidates):
        return

    # use the one thing tagged amenity=school
    # check everything else is tagged building=school

    match = None
    for c in candidates:
        tags = c.matching_tags()
        if 'amenity=school' in tags:
            if match:
                return
            match = c
        elif tags != ['building=school']:
            return
    return match

def filter_candidates_more(items, debug=False):
    osm_count = Counter()
    for item in items:
        for c in item.candidates:
            osm_count[(c.osm_type, c.osm_id)] += 1

    for item in items:
        candidates = item.candidates.all()

        place = filter_place(candidates)
        if place:
            candidates = [place]
        else:
            school = filter_schools(candidates)
            if school:
                candidates = [school]

        if len(candidates) != 1:
            if debug:
                print('too many candidates', item.enwiki, item.candidates.count())
                for c in item.candidates:
                    print('  ', c.osm_type, c.tags)
            continue

        candidate = candidates[0]

        if candidate.matching_tags() == ['designation=civil_parish']:
            continue  # skip for now

        if osm_count[(candidate.osm_type, candidate.osm_id)] > 1:
            if debug:
                print('multiple matches', item.enwiki)
            continue

        if 'wikidata' in candidate.tags:
            if debug:
                print('already has wikidata', item.enwiki)
            continue

        yield (item, candidate)
