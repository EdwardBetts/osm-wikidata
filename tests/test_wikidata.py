from matcher import wikidata
import json
import pytest

def test_wikidata():
    with pytest.raises(AssertionError):
        wikidata.WikidataItem('Q1', {})

    entity = {
        'labels': {
            'fr': {
                'language': 'fr',
                'value': 'tour Eiffel'
            },
            'de': {
                'language': 'de',
                'value': 'Eiffelturm'
            },
            'en': {
                'language': 'en',
                'value': 'Eiffel Tower'
            },
        },
        'sitelinks': {
            'enwiki': {
                'site': 'enwiki',
                'title': 'Eiffel Tower',
                'badges': []
            },
            'frwiki': {
                'site': 'frwiki',
                'title': 'Tour Eiffel',
                'badges': []
            },
            'dewiki': {
                'site': 'dewiki',
                'title': 'Eiffelturm',
                'badges': [
                    'Q17437796'
                ]
            },

        },
        'claims': {
            'P31': [
                {
                    'mainsnak': {
                        'property': 'P31',
                        'datavalue': {
                            'value': {
                                'entity-type': 'item',
                                'id': 'Q1440476',
                                'numeric-id': 1440476
                            },
                            'type': 'wikibase-entityid'
                        },
                        'snaktype': 'value',
                        'datatype': 'wikibase-item'
                    },
                    'rank': 'normal',
                    'id': 'Q243$2eb349bf-4089-fd98-7bd5-263a4b363fba',
                    'type': 'statement'
                },
                {
                    'mainsnak': {
                        'property': 'P31',
                        'datavalue': {
                            'value': {
                                'entity-type': 'item',
                                'id': 'Q1440300',
                                'numeric-id': 1440300
                            },
                            'type': 'wikibase-entityid'
                        },
                        'snaktype': 'value',
                        'datatype': 'wikibase-item'
                    },
                    'rank': 'preferred',
                    'id': 'Q243$1EB6EF73-DC08-4192-A2FC-C2E9C7F7F9E9',
                    'type': 'statement'
                },
                {
                    'mainsnak': {
                        'property': 'P31',
                        'datavalue': {
                            'value': {
                                'entity-type': 'item',
                                'id': 'Q2319498',
                                'numeric-id': 2319498
                            },
                            'type': 'wikibase-entityid'
                        },
                        'snaktype': 'value',
                        'datatype': 'wikibase-item'
                    },
                    'rank': 'normal',
                    'id': 'Q243$f5add39b-4ea4-f936-b9af-ac5c57440287',
                    'type': 'statement'
                }
            ],
            'P625': [{
                'mainsnak': {
                    'property': 'P625',
                    'datavalue': {
                        'value': {
                            'longitude': 2.2944,
                            'globe': 'http://www.wikidata.org/entity/Q2',
                            'altitude': None,
                            'latitude': 48.8583,
                        },
                        'type': 'globecoordinate'
                    },
                    'snaktype': 'value',
                    'datatype': 'globe-coordinate'
                },
                'rank': 'normal',
                'id': 'q243$39A2814F-32C8-415B-A7A6-1DDF4A7D1FFC',
                'type': 'statement',
            }]
        }
    }
    item = wikidata.WikidataItem('Q1', entity)
    assert item.has_coords
    assert item.has_earth_coords

    item._osm_keys = [
        {
            'item': {'value': 'http://www.wikidata.org/entity/Q56061', 'type': 'uri'},
            'itemLabel': {'value': 'administrative territorial entity', 'xml:lang': 'en', 'type': 'literal' },
            'tag': {'value': 'Key:admin_level', 'type': 'literal'},
        },
        {
            'item': {'value': 'http://www.wikidata.org/entity/Q6256', 'type': 'uri'},
            'itemLabel': {'value': 'country', 'xml:lang': 'en', 'type': 'literal'},
            'tag': {'value': 'Tag:place=country', 'type': 'literal'},
        }
    ]

    criteria = item.criteria()
    assert criteria == {'Key:admin_level', 'Tag:place=country'}

    assert item.get_oql(set(), 1000) is None

    expect = '''
[timeout:300][out:json];
(
    node(around:1000,48.85830,2.29440)[place=country][name];
    way(around:1000,48.85830,2.29440)[place=country][name];
    rel(around:1000,48.85830,2.29440)[place=country][name];
    node(around:1000,48.85830,2.29440)["admin_level"][name];
    way(around:1000,48.85830,2.29440)["admin_level"][name];
    rel(around:1000,48.85830,2.29440)["admin_level"][name];
);
out center tags;'''.strip()
    oql = item.get_oql(criteria, 1000)
    print(oql)
    assert oql == expect

    assert item.is_a == ['Q1440476', 'Q1440300', 'Q2319498']

    expect = {
        'Eiffel Tower': [('label', 'en'), ('sitelink', 'enwiki')],
        'Eiffelturm': [('label', 'de'), ('sitelink', 'dewiki')],
        'Tour Eiffel': [('sitelink', 'frwiki')],
        'tour Eiffel': [('label', 'fr')]
    }

    assert item.names == expect

    assert item.coords == (48.8583, 2.2944)
