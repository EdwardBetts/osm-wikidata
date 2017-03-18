from flask import current_app
from collections import defaultdict
from .match import check_for_match, get_wikidata_names

import os.path
import json
import re

bad_name_fields = {'tiger:name_base', 'old_name', 'name:right', 'name:left',
                   'gnis:county_name', 'openGeoDB:name'}


def simplify_tags(tags):
    key_only = sorted(t for t in tags if '=' not in t)
    for k in key_only:
        for t in set(tags):
            if t.startswith(k + '='):
                tags.remove(t)
    return tags

def build_cat_map():
    cat_to_entity = {}
    data_dir = current_app.config['DATA_DIR']
    filename = os.path.join(data_dir, 'entity_types.json')
    for i in json.load(open(filename)):
        for c in i['cats']:
            lc_cat = c.lower()
            if ' by ' in lc_cat:
                lc_cat = lc_cat[:lc_cat.find(' by ')]
            cat_to_entity[lc_cat] = i
    return cat_to_entity

def find_tags(items):
    all_tags = set()

    cat_to_entity = build_cat_map()
    for item in items.values():
        if not item.get('cats'):
            continue

        tags = set()
        for cat in item['cats']:
            lc_cat = cat.lower()
            for key, value in cat_to_entity.items():
                pattern = re.compile(r'\b' + re.escape(key) + r'\b')
                if pattern.search(lc_cat):
                    tags |= set(value['tags'])
        item['tags'] = sorted(tags)
        all_tags |= tags
    return sorted(simplify_tags(all_tags))

def find_item_matches(cur, item, debug=False):
    if not item.entity:
        return []
    cats = item.categories

    # point = "ST_GeomFromEWKT('{}')".format(item.ewkt)
    point = "ST_TRANSFORM(ST_GeomFromEWKT('{}'), 3857)".format(item.ewkt)

    # item_max_dist = max(max_dist[cat] for cat in item['cats'])
    item_max_dist = 4  # FIXME

    sql_list = []
    for obj_type in 'point', 'line', 'polygon':
        obj_sql = ('select \'{}\', osm_id, name, tags, '
                   'ST_Distance({}, way) as dist '
                   'from planet_osm_{} '
                   'where ST_DWithin({}, way, {} * 1000)').format(obj_type, point, obj_type, point, item_max_dist)
        sql_list.append(obj_sql)
    sql = 'select * from (' + ' union '.join(sql_list) + ') a where ({}) order by dist'.format(item.hstore_query)

    if debug:
        print(sql)

    cur.execute(sql)
    rows = cur.fetchall()
    seen = set()

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

        match = check_for_match(osm_tags, item)
        if not match:
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

def find_matches(items, conn, debug=False):
    cur = conn.cursor()
    seen_wikidata = set()
    assert isinstance(items, list)
    items.sort(key=lambda i: int(i['qid'][1:]))
    found = []
    for num, item in enumerate(items):
        if 'tags' not in item or not item['tags']:
            continue
        # print(num, item['qid'], item['label'])
        candidates = []
        cats = item['cats']
        # cats = {p[0] for p in item['cat_paths']}
        # item['cats'] = cats
        assert item['qid'] not in seen_wikidata
        seen_wikidata.add(item['qid'])
        hstore_query = build_hstore_query(item['tags'])
        item['names'] = dict(get_wikidata_names(item))
        point = "ST_TRANSFORM(ST_SETSRID(ST_MAKEPOINT({}, {}),4326), 3857)".format(item['lon'], item['lat'])

        # item_max_dist = max(max_dist[cat] for cat in item['cats'])
        item_max_dist = 4  # FIXME

        sql_list = []
        for obj_type in 'point', 'line', 'polygon':
            obj_sql = ('select \'{}\', osm_id, name, tags, '
                       'ST_Distance({}, way) as dist '
                       'from planet_osm_{} '
                       'where ST_DWithin({}, way, {} * 1000)').format(obj_type, point, obj_type, point, item_max_dist)
            sql_list.append(obj_sql)
        sql = 'select * from (' + ' union '.join(sql_list) + ') a where ({}) order by dist'.format(hstore_query)

        cur.execute(sql)
        rows = cur.fetchall()
        seen = set()

        for osm_num, (src_type, src_id, osm_name, osm_tags, dist) in enumerate(rows):
            (osm_type, osm_id) = get_osm_id_and_type(src_type, src_id)
            if (obj_type, osm_id) in seen:
                continue
            seen.add((obj_type, osm_id))

            try:
                admin_level = int(osm_tags['admin_level']) if 'admin_level' in osm_tags else None
            except Exception:
                admin_level = None
            names = {k: v for k, v in osm_tags.items() if 'name' in k and k not in bad_name_fields}
            if any(c.startswith('Cities ') for c in cats) and admin_level == 10:
                continue
            if not names:
                continue

            match = check_for_match(osm_tags, item)
            if not match:
                continue
            candidate = {
                'type': osm_type,
                'id': osm_id,
                'name': osm_name,
                'tags': osm_tags,
                'dist': dist,
                'match': match.match_type.name,
                'planet_table': src_type,
                'src_id': src_id,
            }
            candidates.append(candidate)
        if candidates:
            item['candidates'] = candidates
            found.append(item)
    return found

def build_hstore_query(tags):
    tags = [tuple(tag.split('=')) if ('=' in tag) else (tag, None)
            for tag in tags]
    return ' or '.join("((tags->'{}') = '{}')".format(k, v)
                       if v else "(tags ? '{}')".format(k)
                       for k, v in tags)

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

def all_in_one(item, conn):
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
                   'from planet_osm_{} '
                   'where osm_id in ({})').format(table, table, id_list)
        sql_list.append(obj_sql)

    if not sql_list:
        return
    sql = 'select ST_Within(a.way, b.way) from (' + ' union '.join(sql_list) + ') a, planet_osm_polygon b where b.osm_id={}'.format(biggest)
    cur.execute(sql)
    if all(row[0] for row in cur.fetchall()):
        return biggest

def filter_candidates(items, conn):
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

def filter_candidates_more(items):
    items2 = []
    osm_count = defaultdict(list)
    for item in items:
        if len(item['candidates']) != 1:
            continue
        osm = item['candidates'][0]
        item['osm'] = osm
        if 'wikidata' in item['osm']['tags']:
            continue
        items2.append(item)
        osm_count[(osm['type'], osm['id'])].append(item)
    for k, v in osm_count.items():
        if len(v) > 1:
            # print (k, len(v))
            for item in v:
                items2.remove(item)
    return items2


