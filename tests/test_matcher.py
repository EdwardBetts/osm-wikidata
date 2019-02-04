from matcher import matcher
from matcher.model import Item, IsA, ItemCandidate
import os.path

class MockApp:
    config = {'DATA_DIR': os.path.normpath(os.path.split(__file__)[0] + '/../data')}

class MockDatabase:
    def execute(self, sql):
        pass

    def fetchone(self):
        pass

entity = {
  "claims": {
    "P17": [
      {
        "id": "q4866042$897CA57D-748B-4912-B429-733AACA0AD4A",
        "mainsnak": {
          "datatype": "wikibase-item",
          "datavalue": {
            "type": "wikibase-entityid",
            "value": { "entity-type": "item", "id": "Q30", "numeric-id": 30 }
          },
        },
        "rank": "normal",
      }
    ],
    "P31": [
      {
        "id": "Q4866042$6E346CB6-757C-4752-9141-5F69A45B7425",
        "mainsnak": {
          "datatype": "wikibase-item",
          "datavalue": {
            "type": "wikibase-entityid",
            "value": {
              "entity-type": "item",
              "id": "Q3469910",
              "numeric-id": 3469910
            }
          },
        },
        "rank": "normal",
        "type": "statement"
      }
    ],
    "P373": [
      {
        "id": "Q4866042$B2B1818A-D1F5-44AE-B389-5D80099DAE55",
        "mainsnak": {
          "datatype": "string",
          "datavalue": {
            "type": "string",
            "value": "Baryshnikov Arts Center"
          },
          "hash": "8c5f48a8cb6d4677f5360d05ff46bc05602b45cf",
          "property": "P373",
          "snaktype": "value"
        },
        "rank": "normal",
      }
    ],
    "P625": [
      {
        "id": "q4866042$43C29A80-E4F2-4397-802E-393D275B8FEB",
        "mainsnak": {
          "datatype": "globe-coordinate",
          "datavalue": {
            "type": "globecoordinate",
            "value": {
              "altitude": None,
              "globe": "http://www.wikidata.org/entity/Q2",
              "latitude": 40.756,
              "longitude": -73.9974,
              "precision": 0.001
            }
          },
        },
        "rank": "normal",
        "type": "statement"
      }
    ],
    "P856": [
      {
        "id": "Q4866042$73764361-9CF8-4475-8A80-936CDD79929E",
        "mainsnak": {
          "datatype": "url",
          "datavalue": {
            "type": "string",
            "value": "http://www.bacnyc.org/"
          },
        },
        "rank": "normal",
        "type": "statement"
      }
    ]
  },
  "descriptions": {
    "en": {
      "language": "en",
      "value": "Baryshnikob Arts Centre"
    },
    "nl": {
      "language": "nl",
      "value": "accommodatie voor uitvoerende kunst in New York, Verenigde Staten van Amerika"
    }
  },
  "id": "Q4866042",
  "labels": {
    "en": {
      "language": "en",
      "value": "Baryshnikov Arts Center"
    }
  },
  "ns": 0,
  "pageid": 4648064,
  "sitelinks": {
    "commonswiki": {
      "badges": [],
      "site": "commonswiki",
      "title": "Category:Baryshnikov Arts Center"
    },
    "enwiki": {
      "badges": [],
      "site": "enwiki",
      "title": "Baryshnikov Arts Center"
    }
  },
  "title": "Q4866042",
  "type": "item"
}

def test_get_pattern():
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

def test_item_match_sql(monkeypatch):
    item = Item(entity=entity, tags=['building'])
    monkeypatch.setattr(matcher, 'current_app', MockApp)
    sql = matcher.item_match_sql(item, 'test')
    assert "(tags ? 'building')" in sql

def find_item_matches(monkeypatch, osm_tags, item):
    def mock_run_sql(cur, sql, debug):
        if not sql.startswith('select * from'):
            return []
        return [('node', 1, None, osm_tags, 0)]

    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    mock_db = MockDatabase()

    return matcher.find_item_matches(mock_db, item, 'prefix')

def test_find_item_matches_mall(monkeypatch):
    osm_tags = {
        'landuse': 'retail',
        'name': 'Oxmoor Mall',
    }

    test_entity = {
        'claims': {},
        'labels': {
            'en': {'language': 'en', 'value': 'Oxmoor Center'},
        },
        'sitelinks': {
            'enwiki': {
                'site': 'enwiki',
                'title': 'Oxmoor Center',
            }
        },
    }

    tags = ['landuse=retail']
    item = Item(entity=test_entity, tags=tags)

    candidates = find_item_matches(monkeypatch, osm_tags, item)
    assert len(candidates) == 1
    return

def test_church_is_not_school(monkeypatch):
    test_entity = {
        'claims': {},
        'labels': {
            'en': {'language': 'en', 'value': "St. Paul's Catholic Church"},
        },
        'sitelinks': {},
    }

    tags = ['amenity=place_of_worship', 'religion=christian']
    item = Item(entity=test_entity, tags=tags)

    osm_tags = {
        'name': "Saint Paul's Catholic School",
        'height': '12',
        'amenity': 'school',
        'building': 'school',
        'religion': 'christian',
        'denomination': 'catholic',
    }

    def mock_run_sql(cur, sql, debug):
        if not sql.startswith('select * from'):
            return []
        return [('polygon', 1, None, osm_tags, 0)]

    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    mock_db = MockDatabase()

    candidates = matcher.find_item_matches(mock_db, item, 'prefix')
    assert len(candidates) == 0

def test_match_operator_at_start_of_name(monkeypatch):
    osm_tags = {
        'highway': 'services',
        'landuse': 'commercial',
        'name': 'Welcome Break Gordano Services',
        'operator': 'Welcome Break',
    }

    test_entity = {
        'claims': {},
        'labels': {
            'en': {'language': 'en', 'value': 'Gordano services'},
        },
        'sitelinks': {}
    }

    tags = ['highway=services']
    item = Item(entity=test_entity, tags=tags)

    def mock_run_sql(cur, sql, debug):
        if not sql.startswith('select * from'):
            return []
        return [('polygon', 64002602, None, osm_tags, 0)]

    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    mock_db = MockDatabase()

    candidates = matcher.find_item_matches(mock_db, item, 'prefix')
    assert len(candidates) == 1


def test_find_item_matches_parking(monkeypatch):
    osm_tags = {
        'amenity': 'parking',
        'building': 'yes',
        'fee': 'yes',
        'name': 'PlayhouseSquare Parking',
        'operator': 'PlayhouseSquare',
        'parking': 'multi-storey',
        'supervised': 'yes',
    }

    test_entity = {
        'claims': {},
        'labels': {
            'en': {'language': 'en', 'value': 'Playhouse Square'},
            'de': {'language': 'de', 'value': 'Playhouse Square'},
        },
        'sitelinks': {
            'commonswiki': {
                'site': 'commonswiki',
                'title': 'Category:Playhouse Square',
            },
            'enwiki': {
                'site': 'enwiki',
                'title': 'Playhouse Square',
            }
        },
    }

    tags = ['amenity=arts_centre', 'building']
    item = Item(entity=test_entity, tags=tags)

    def mock_run_sql(cur, sql, debug):
        if not sql.startswith('select * from'):
            return []
        return [('polygon', 116620439, None, osm_tags, 253.7)]

    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    mock_db = MockDatabase()

    candidates = matcher.find_item_matches(mock_db, item, 'prefix')
    assert len(candidates) == 0

def test_embassy_no_match(monkeypatch):
    osm_tags1 = {
        'name': 'Consulate General of Switzerland in San Francisco',
        'amenity': 'embassy',
        'country': 'CH',
        'addr:city': 'San Francisco',
        'addr:state': 'CA',
        'addr:street': 'Montgomery Street',
        'addr:postcode': '94104',
        'addr:housenumber': '456',
    }

    osm_tags2 = {
        'addr:housenumber': '456',
        'addr:street': 'Montgomery Street',
        'building': 'yes',
        'building:levels': '22',
        'height': '114',
        'name': 'Consulate General of Switzerland in San Francisco',
    }

    en_label = 'Consulate General of Israel to the Pacific Northwest Region'
    test_entity = {
        'claims': {
            'P969': [{
                'mainsnak': {
                    'datavalue': {'value': '456 Montgomery Street Suite #2100'},
                },
            }],
            'P17': [{
                'mainsnak': {'datavalue': {'value': {'id': 'Q30'}}},
            }],
            'P137': [{
                'mainsnak': {'datavalue': {'value': {'id': 'Q801'}}},
            }],
        },
        'labels': {'en': {'language': 'en', 'value': en_label}},
        'sitelinks': {},
    }

    extract = ('<p>The <b>Consulate General of Israel to the Pacific Northwest ' +
              'Region</b>, is one of Israel’s diplomatic missions abroad, located at ' +
              '456 Montgomery Street Suite #2100 in San Francisco, California, United ' +
              'States of America in the Financial District.</p>')

    tags = ['amenity=embassy']
    item = Item(entity=test_entity, tags=tags, extract=extract)

    def mock_run_sql(cur, sql, debug):
        if not sql.startswith('select * from'):
            return []
        return [('point', 1, None, osm_tags1, 0),
                ('polygon', 2, None, osm_tags2, 0)]

    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    mock_db = MockDatabase()

    candidates = matcher.find_item_matches(mock_db, item, 'prefix')
    assert len(candidates) == 0

def test_find_item_matches_pub(monkeypatch):
    osm_tags = {
        'amenity': 'university',
        'building': 'university',
        'name': 'Castle House',
    }

    test_entity = {
        'claims': {},
        'labels': {'en': {'language': 'en', 'value': 'The Castle Inn'}},
        'sitelinks': {},
    }

    tags = ['building', 'amenity=pub']
    item = Item(entity=test_entity, tags=tags)

    def mock_run_sql(cur, sql, debug):
        return [('polygon', -295355, None, osm_tags, 12.75)]

    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    mock_db = MockDatabase()

    candidates = matcher.find_item_matches(mock_db, item, 'prefix')
    assert len(candidates) == 0

def test_find_item_matches(monkeypatch):
    osm_tags = {
        'height': '44.9',
        'building': 'yes',
        'addr:street': 'West 37th Street',
        'nycdoitt:bin': '1087066',
        'addr:postcode': '10018',
        'addr:housenumber': '450',
    }

    def mock_run_sql(cur, sql, debug):
        if not sql.startswith('select * from'):
            return []
        return [('polygon', 265273006, None, osm_tags, 0.0)]
    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    extract = '''<p>The <b>Baryshnikov Arts Center</b> (<b>BAC</b>) is a foundation and arts complex opened by Mikhail Baryshnikov in 2005 at 450 West 37th Street between Ninth and Tenth Avenues in the Hell's Kitchen neighborhood of Manhattan, New York City. The top three floors of the complex are occupied by the Baryshnikov Arts Center, which provides space and production facilities for dance, music, theater, film, and visual arts. The building also houses the Orchestra of St. Luke's DiMenna Center for Classical Music.</p>'''
    item = Item(entity=entity, tags=['building'], extract=extract)

    mock_db = MockDatabase()

    expect = {
        'osm_type': 'way',
        'osm_id': 265273006,
        'name': None,
        'tags': osm_tags,
        'dist': 0.0,
        'planet_table':
        'polygon',
        'src_id': 265273006,
        'geom': None,
        'identifier_match': False,
        'address_match': True,
        'name_match': {},
        'matching_tags': {'building'},
    }

    candidates = matcher.find_item_matches(mock_db, item, 'prefix')
    assert len(candidates) == 1
    assert candidates[0] == expect

def test_name_and_location_better_than_address_and_building(monkeypatch):
    tower_tags = {'name': 'Reunion Tower', 'tourism': 'attraction'}
    hotel_tags = {
        'addr:housenumber': '300',
        'addr:street': 'Reunion Boulevard',
        'building': 'hotel'
    }

    test_entity = {
        'claims': {},
        'labels': {
            'en': {'language': 'en', 'value': 'Reunion Tower'},
        },
        'sitelinks': {},
    }

    extract = ('<p><b>Reunion Tower</b> is a 561 ft (171 m) observation tower and ' +
               'one of the most recognizable landmarks in Dallas, Texas. Located at ' +
               '300 Reunion Boulevard in the Reunion district of downtown Dallas.</p>')

    tags = ['man_made=tower', 'building=tower', 'height']
    item = Item(entity=test_entity, tags=tags, extract=extract)

    def mock_run_sql(cur, sql, debug):
        if sql.startswith('select * from'):
            return [('polygon', 29191381, None, hotel_tags, 0)]
        else:
            return [('point', 600482843, None, tower_tags, 7)]

    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    mock_db = MockDatabase()

    candidates = matcher.find_item_matches(mock_db, item, 'prefix', debug=True)
    assert len(candidates) == 2

def test_alcatraz_lighthouse(monkeypatch):
    lighthouse_tags = {
        'alt_name': 'United States Coast Guard Lighthouse',
        'building': 'yes',
        'man_made': 'lighthouse',
        'name': 'Alcatraz Island Lighthouse',
        'start_date': '1909',
        'wikidata': 'Q4712967',
    }
    island_tags = {'name': 'Alcatraz Island', 'tourism': 'attraction'}

    test_entity = {
        'claims': {},
        'labels': {
            'en': {'language': 'en', 'value': 'Alcatraz Island Light'},
        },
        'sitelinks': {
            'commonswiki': {
                'site': 'commonswiki',
                'title': 'Category:Alcatraz Island Lighthouse'
            },
            'enwiki': {
                'site': 'enwiki',
                'title': 'Alcatraz Island Light'
            },
        },
    }

    tags = ['tourism=attraction', 'building', 'man_made=lighthouse']
    item = Item(entity=test_entity, tags=tags)

    def mock_run_sql(cur, sql, debug):
        if not sql.startswith('select * from'):
            return []
        return [
            ('point', 265562462, None, island_tags, 151),
            ('polygon', 99202294, None, lighthouse_tags, 0),
        ]

    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    mock_db = MockDatabase()

    candidates = matcher.find_item_matches(mock_db, item, 'prefix', debug=True)
    assert len(candidates) == 2

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

def test_bad_building_match():

    item = Item()

    assert not matcher.bad_building_match({}, {}, item)

    name_match = {'name': [('good', 'Test', [('label', 'en')])]}

    osm_tags = {'amenity': 'parking'}
    assert matcher.bad_building_match(osm_tags, name_match, item)

    assert not matcher.bad_building_match({}, name_match, item)

    name_match = {'name': [('both_trimmed', 'Test', [('label', 'en')])]}
    assert matcher.bad_building_match({}, name_match, item)

    name_match = {
        'name': [('both_trimmed', 'Test', [('label', 'en')])],
        'old_name': [('good', 'Test', [('label', 'en')])]
    }

    assert not matcher.bad_building_match({}, name_match, item)

    name_match = {
        'name': [('both_trimmed', 'Test', [('label', 'en')])],
        'operator': [('wikidata_trimmed', 'Test', [('label', 'en')])]
    }

    assert matcher.bad_building_match({}, name_match, item)

    osm_tags = {
        'name': 'Westland London',
        'shop': 'furniture',
        'building': 'yes',
        'addr:street': 'Leonard Street',
        'addr:postcode': 'EC2A 4QX',
        'addr:housename': "St. Michael's Church",
    }

    name_match = {'addr:housename': [('good',
                                      'Church Of St Michael',
                                      [('label', 'en')])]}

    assert not matcher.bad_building_match(osm_tags, name_match, item)

    osm_tags = {
        'addr:city': 'Birmingham',
        'addr:housenumber': '42',
        'addr:postcode': 'B9 5QF',
        'addr:street': 'Yardley Green Road',
        'amenity': 'place_of_worship',
        'building': 'yes',
        'heritage': '2',
        'heritage:operator': 'Historic England',
        'listed_status': 'Grade II',
        'name': 'Masjid Noor-Us-Sunnah',
        'previous_name': 'Samson & Lion',
        'previous_use': 'pub',
        'religion': 'muslim',
    }

    name_match = {'previous_name': [('wikidata_trimmed',
                                     'Samson And Lion Public House',
                                     [('label', 'en')])]}

    assert not matcher.bad_building_match(osm_tags, name_match, item)

def test_cottage_church_bad_match(monkeypatch):
    church_tags = {
        'amenity': 'place_of_worship',
        'building': 'yes',
        'denomination': 'anglican',
        'name': 'Saint Anne',
        'religion': 'christian',
    }

    entity = {
        'claims': {},
        'labels': {
            'en': {'language': 'en', 'value': "St Anne's Cottage"},
        },
        'sitelinks': {}
    }

    cottage_item = {
        'claims': {},
        'labels': {
            'en': {'language': 'en', 'value': "cottage"},
        },
        'sitelinks': {}
    }

    isa = IsA(item_id=5783996, entity=cottage_item)

    tags = ['building', 'building=yes']
    item = Item(entity=entity, tags=tags, isa=[isa])

    def mock_run_sql(cur, sql, debug):
        if not sql.startswith('select * from'):
            return []
        return [
            ('polygon', 111491387, None, church_tags, 0),
        ]

    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    mock_db = MockDatabase()

    candidates = matcher.find_item_matches(mock_db, item, 'prefix', debug=True)
    assert len(candidates) == 0

def test_lifeboat_station_church_bad_match(monkeypatch):
    osm_tags = {
        'amenity': 'place_of_worship',
        'building': 'yes',
        'denomination': 'anglican',
        'name': "St Agnes'",
        'religion': 'christian',
    }

    entity = {
        'claims': {},
        'labels': {
            'en': {'language': 'en', 'value': 'St Agnes Lifeboat Station'},
        },
        'sitelinks': {}
    }

    tags = ['amenity=lifeboat_station', 'building', 'building=yes',
            'emergency=lifeboat_station']
    item = Item(entity=entity, tags=tags)

    def mock_run_sql(cur, sql, debug):
        if not sql.startswith('select * from'):
            return []
        return [
            ('polygon', 234155614, None, osm_tags, 0),
        ]

    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    mock_db = MockDatabase()

    candidates = matcher.find_item_matches(mock_db, item, 'prefix', debug=True)
    assert len(candidates) == 0

def test_castle_station_bad_match(monkeypatch):
    osm_tags = {
        'building': 'train_station',
        'name': 'Holyhead',
        'name:cy': 'Caergybi',
        'railway': 'station',
    }

    entity = {
        'claims': {},
        'labels': {
            'en': {'language': 'en', 'value': 'Caer Gybi'},
            'cy': {'language': 'cy', 'value': 'Caer Gybi (caer)'},
        },
        'sitelinks': {}
    }

    tags = ['historic=castle', 'building']
    item = Item(entity=entity, tags=tags)

    def mock_run_sql(cur, sql, debug):
        if not sql.startswith('select * from'):
            return []
        return [
            ('polygon', 158252670, None, osm_tags, 0),
        ]

    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    mock_db = MockDatabase()

    candidates = matcher.find_item_matches(mock_db, item, 'prefix', debug=True)
    assert len(candidates) == 0

def test_church_pub_bad_match(monkeypatch):
    osm_tags = {
        'amenity': 'pub',
        'building': 'commercial',
        'name': 'The Broadwater',
    }

    entity = {
        'claims': {
            'P373': [{
                'mainsnak': {
                    'datavalue': {'value': 'Broadwater Church, West Sussex'},
                }
            }]
        },
        'labels': {
            'en': {'language': 'en', 'value': "St. Mary's Church, Broadwater"},
        },
        'sitelinks': {
            'commonswiki': {
                'site': 'commonswiki',
                'title': 'Category:Broadwater Church, West Sussex',
            },
            'enwiki': {
                'site': 'enwiki',
                'title': "St Mary's Church, Broadwater",
            }
        },
    }

    tags = ['religion=christian', 'building=yes', 'building', 'amenity=place_of_worship',
            'building=shrine', 'building=temple', 'building=church']
    item = Item(entity=entity, tags=tags)

    def place_names():
        return ['West Sussex']
    monkeypatch.setattr(item, 'place_names', place_names)

    def mock_run_sql(cur, sql, debug):
        if not sql.startswith('select * from'):
            return []
        return [
            ('polygon', 91013361, None, osm_tags, 0),
        ]

    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    mock_db = MockDatabase()

    candidates = matcher.find_item_matches(mock_db, item, 'prefix', debug=True)
    assert len(candidates) == 0

def test_railway_station_cafe_bad_match(monkeypatch):
    osm_tags = {
        'addr:city': 'Gillingham',
        'addr:housename': 'Gillingham (Kent) Railway Station',
        'addr:postcode': 'ME7 1XE',
        'addr:street': 'Railway Street',
        'amenity': 'cafe',
        'building': 'yes',
        'cuisine': 'coffee_shop',
        'name': 'BeeZoo Coffee Shop',
    }

    entity = {
        'claims': {},
        'labels': {
            'en': {'language': 'en', 'value': "Gillingham railway station"},
        },
        'sitelinks': {
            'commonswiki': {
                'site': 'commonswiki',
                'title': 'Category:Gillingham (Kent) railway station',
            },
            'enwiki': {
                'site': 'enwiki',
                'title': 'Gillingham railway station (Kent)',
            },
            'nlwiki': {
                'site': 'nlwiki',
                'title': 'Station Gillingham (Kent)',
            },
            'simplewiki': {
                'site': 'simplewiki',
                'title': 'Gillingham (Kent) railway station',
            },
        },
    }

    tags = ['building=train_station', 'railway=station', 'railway=halt']
    item = Item(entity=entity, tags=tags)

    def mock_run_sql(cur, sql, debug):
        if not sql.startswith('select * from'):
            return []
        return [
            ('polygon', 1, None, osm_tags, 0),
        ]

    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    mock_db = MockDatabase()

    candidates = matcher.find_item_matches(mock_db, item, 'prefix', debug=True)
    assert len(candidates) == 0

def test_prefer_tag_match_over_building_only_match(monkeypatch):
    tags1 = {
        'name': 'Shepperton',
        'network': 'National Rail',
        'railway': 'station',
    }
    tags2 = {
        'building': 'yes',
        'name': 'Shepperton Station'
    }

    entity = {
        'claims': {},
        'labels': {
            'en': {'language': 'en', 'value': 'Shepperton railway station'},
            'nl': {'language': 'nl', 'value': 'station Shepperton'},
        },
        'sitelinks': {},
    }

    tags = ['building=train_station', 'railway=station', 'building']
    item = Item(entity=entity, tags=tags)

    def mock_run_sql(cur, sql, debug):
        if not sql.startswith('select * from'):
            return []
        return [
            ('point', 3397249904, None, tags1, 26.78),
            ('polygon', 246812406, None, tags2, 0),
        ]

    monkeypatch.setattr(matcher, 'run_sql', mock_run_sql)
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    mock_db = MockDatabase()

    candidates = matcher.find_item_matches(mock_db, item, 'prefix', debug=True)
    assert len(candidates) == 1
    c = candidates[0]
    del c['name_match']
    assert c == {
        'osm_type': 'node',
        'osm_id': 3397249904,
        'name': None,
        'tags': tags1,
        'dist': 26.78,
        'planet_table': 'point',
        'src_id': 3397249904,
        'geom': None,
        'identifier_match': False,
        'address_match': None,
        'matching_tags': {'railway=station'}
    }


def test_name_match_church(monkeypatch):
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    osm_tags = {
        'amenity': 'place_of_worship',
        'name': 'St Andrew',
    }

    entity = {
        "claims": {},
        "id": "Q23302351",
        "labels": {"en": {"value": "St Andrews, Great Finborough"}},
        "sitelinks": {},
    }

    categories = ['Church of England church buildings in Suffolk']

    item = Item(item_id=23302351,
                entity=entity,
                extract_names=["St Andrew's Church"],
                categories=categories)

    candidate = ItemCandidate(item=item, tags=osm_tags)

    for t in matcher.categories_to_tags(item.categories):
        item.tags.add(t)

    ret = matcher.check_item_candidate(candidate)
    assert 'reject' not in ret

def test_stable_shouldnt_match_house(monkeypatch):
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    osm_tags = {
        'addr:street': 'Back Lane',
        'building': 'house',
        'name': 'Nazeing Park',
    }

    entity = {
        'claims': {
            'P31': [{
                'mainsnak': {'datavalue': {'value': {'id': 'Q214252'}}},
            }],
        },
        'labels': {'en': {'value': 'Stable At Nazeing Park'}},
        'sitelinks': {},
    }

    cottage_item = {
        'claims': {},
        'labels': {
            'en': {'language': 'en', 'value': "stable"},
        },
        'sitelinks': {}
    }

    isa = IsA(item_id=214252, entity=cottage_item)

    tags = ['building=stable']
    item = Item(item_id=26404858, entity=entity, tags=tags, isa=[isa])

    candidate = ItemCandidate(item=item, tags=osm_tags)

    ret = matcher.check_item_candidate(candidate)
    assert 'reject' in ret

def test_hamlet_shouldnt_match_house(monkeypatch):
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    osm_tags = {'name': 'Pednor House', 'place': 'residence'}

    entity = {
        'claims': {
            'P31': [{
                'mainsnak': {'datavalue': {'value': {'id': 'Q5084'}}},
            }],
        },
        'labels': {'en': {'value': 'Pednor'}},
        'sitelinks': {},
    }

    categories = ['Hamlets in Buckinghamshire']

    item = Item(item_id=7159326,
                entity=entity,
                extract_names=['Pednor'],
                categories=categories)

    candidate = ItemCandidate(item=item, tags=osm_tags)

    for t in matcher.categories_to_tags(item.categories):
        item.tags.add(t)

    ret = matcher.check_item_candidate(candidate)
    assert 'reject' in ret

def test_station_shouldnt_match_school(monkeypatch):
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    osm_tags = {
        'addr:city': 'Cummersdale',
        'building': 'school',
        'name': 'Cummersdale School',
    }

    entity = {
        'claims': {
            'P31': [{
                'mainsnak': {'datavalue': {'value': {'id': 'Q55488'}}},
            }],
        },
        'labels': {'en': {'value': 'Cummersdale railway station'}},
        'sitelinks': {},
    }

    categories = ['Disused railway stations in Cumbria']

    item = Item(item_id=5194098,
                entity=entity,
                extract_names=['Cummersdale'],
                categories=categories)

    candidate = ItemCandidate(item=item, tags=osm_tags)

    for t in matcher.categories_to_tags(item.categories):
        item.tags.add(t)

    ret = matcher.check_item_candidate(candidate)
    assert 'reject' in ret

def test_no_match_cottage(monkeypatch):
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    osm_tags = {
        'addr:housename': 'Stonehaven',
        'addr:housenumber': '6',
        'addr:street': 'High St',
        'building': 'yes',
    }

    entity = {
        'claims': {
            'P31': [{
                'mainsnak': {'datavalue': {'value': {'id': 'Q5783996'}}},
            }],
        },
        'labels': {'en': {'value': 'Stonehaven Cottage'}},
        'sitelinks': {},
    }

    item = Item(item_id=26573554, entity=entity, tags=['building'])

    candidate = ItemCandidate(item=item, tags=osm_tags)

    ret = matcher.check_item_candidate(candidate)
    assert 'reject' in ret

def test_school_shouldnt_match_church(monkeypatch):
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    osm_tags = {
        'amenity': 'place_of_worship',
        'building': 'yes',
        'denomination': 'roman_catholic',
        'name': 'Our Lady of Lourdes',
        'religion': 'christian',
    }

    entity = {
        'claims': {
            'P31': [{
                'mainsnak': {'datavalue': {'value': {'id': 'Q3914'}}},
            }],
        },
        'labels': {'en': {'value': 'Our Lady of Lourdes School'}},
        'sitelinks': {},
    }

    categories = ['Catholic primary schools in the Archdiocese of Westminster',
                  'Primary schools in the London Borough of Enfield',
                  'Voluntary aided schools in London']

    item = Item(item_id=7110870,
                entity=entity,
                extract_names=[],
                categories=categories)

    candidate = ItemCandidate(item=item, tags=osm_tags)

    for t in matcher.categories_to_tags(item.categories):
        item.tags.add(t)

    ret = matcher.check_item_candidate(candidate)
    assert 'reject' in ret
