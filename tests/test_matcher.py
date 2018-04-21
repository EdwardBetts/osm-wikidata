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

def test_filter_distant():
    close = {
        'address_match': None,
        'dist': 0.0,
        'identifier_match': False,
        'name': 'Martello Tower',
        'name_match': {'name': [('good', 'Martello Tower', [('label', 'en')])]},
        'osm_id': 108215711,
        'osm_type': 'way',
        'planet_table': 'polygon',
        'src_id': 108215711,
        'tags': {'building': 'yes',
                 'historic': 'fort',
                 'name': 'Martello Tower',
                 'way_area': '614.77'}
    }
    distant = {
        'address_match': None,
        'dist': 1228.6786059846,
        'identifier_match': False,
        'name': 'Martello Tower',
        'name_match': {'name': [('good', 'Martello Tower', [('label', 'en')])]},
        'osm_id': 108215724,
        'osm_type': 'way',
        'planet_table': 'polygon',
        'src_id': 108215724,
        'tags': {'building': 'yes',
                 'historic': 'fort',
                 'name': 'Martello Tower',
                 'way_area': '581.462'}
    }

    candidates = matcher.filter_distant([close, distant])
    assert len(candidates) == 1
    assert candidates[0] == close

    distant['dist'] = 900
    candidates = matcher.filter_distant([close, distant])

    assert len(candidates) == 2
    assert candidates == [close, distant]
