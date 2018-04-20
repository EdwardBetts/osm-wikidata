from matcher import matcher

def test_get_pattern():
    key = 'test'
    re_pattern = matcher.get_pattern('test')
    assert re_pattern.pattern == r'\btest\b'

    matcher.get_pattern('test')

def test_get_osm_id_and_type():
    assert matcher.get_osm_id_and_type('point', 1) == ('node', 1)
    assert matcher.get_osm_id_and_type('line', 1) == ('way', 1)
    assert matcher.get_osm_id_and_type('line', -1) == ('relation', 1)
    assert matcher.get_osm_id_and_type('polygon', 1) == ('way', 1)
    assert matcher.get_osm_id_and_type('polygon', -1) == ('relation', 1)

def test_planet_table_id():
    osm = {'type': 'node', 'id': '1'}
    assert matcher.planet_table_id(osm) == ('point', 1)

    osm = {'type': 'way', 'id': '1', 'tags': {}}
    assert matcher.planet_table_id(osm) == ('line', 1)

    osm = {'type': 'relation', 'id': '1', 'tags': {}}
    assert matcher.planet_table_id(osm) == ('line', -1)

    osm = {'type': 'way', 'id': '1', 'tags': {'way_area': 1}}
    assert matcher.planet_table_id(osm) == ('polygon', 1)

    osm = {'type': 'relation', 'id': '1', 'tags': {'way_area': 1}}
    assert matcher.planet_table_id(osm) == ('polygon', -1)

def test_simplify_tags():
    tags = ['building', 'building=yes', 'amenity=pub']
    assert matcher.simplify_tags(tags) == ['building', 'amenity=pub']
