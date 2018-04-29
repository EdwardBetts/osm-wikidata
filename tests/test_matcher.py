from matcher import matcher
from matcher.model import Item
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

    def mock_run_sql(cur, sql, debug):
        rows = [('polygon', 59847542, None, osm_tags, 0)]
        return rows

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
        rows = [('polygon', 116620439, None, osm_tags, 253.7)]
        return rows

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
        rows = [('polygon', 265273006, None, osm_tags, 0.0)]
        return rows
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
    print(candidates[0])
    assert candidates[0] == expect

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
