from matcher import view, wikidata, overpass, matcher
from pprint import pprint
import werkzeug.exceptions
import pytest
import os.path

test_entity = {
            "pageid": 4187750,
            "ns": 0,
            "title": "Q4384193",
            "lastrevid": 667652500,
            "modified": "2018-04-21T09:58:36Z",
            "type": "item",
            "id": "Q4384193",
            "labels": {
                "pt": { "language": "pt", "value": "Golden Spike" },
                "ja": { "language": "ja", "value": "\u30b4\u30fc\u30eb\u30c7\u30f3\u30fb\u30b9\u30d1\u30a4\u30af" },
                "en": { "language": "en", "value": "Golden spike" },
                "de": { "language": "de", "value": "Golden Spike" },
                "zh-hant": { "language": "zh-hant", "value": "\u91d1\u8272\u9053\u91d8" },
                "zh-cn": { "language": "zh-cn", "value": "\u91d1\u8272\u9053\u9489" },
                "he": { "language": "he", "value": "\u05d9\u05ea\u05d3 \u05d4\u05d6\u05d4\u05d1" },
                "ru": { "language": "ru", "value": "\u0417\u043e\u043b\u043e\u0442\u043e\u0439 \u043a\u043e\u0441\u0442\u044b\u043b\u044c" }
            },
            "descriptions": {
                "en": {
                    "language": "en",
                    "value": "ceremony for the completion of the first railroad line to cross the USA"
                }
            },
            "aliases": {
                "pt": [
                    { "language": "pt", "value": "Last Spike" }
                ],
                "en": [
                    { "language": "en", "value": "Final spike" }
                ]
            },
            "claims": {
                "P625": [
                    {
                        "mainsnak": {
                            "snaktype": "value",
                            "property": "P625",
                            "hash": "bd172fb92f2d83d38caa1dc19663a97a3434e69f",
                            "datavalue": {
                                "value": {
                                    "latitude": 41.617963888889,
                                    "longitude": -112.55163055556,
                                    "globe": "http://www.wikidata.org/entity/Q2"
                                },
                                "type": "globecoordinate"
                            },
                            "datatype": "globe-coordinate"
                        },
                        "type": "statement",
                        "id": "q4384193$F0C579C4-9FA2-4D56-BD6B-47AC90E1E89D",
                        "rank": "normal"
                    }
                ],
                "P17": [
                    {
                        "mainsnak": {
                            "snaktype": "value",
                            "property": "P17",
                            "hash": "be4c6eafa2984964f04be85667263f5642ba1a72",
                            "datavalue": {
                                "value": {
                                    "entity-type": "item",
                                    "numeric-id": 30,
                                    "id": "Q30"
                                },
                                "type": "wikibase-entityid"
                            },
                            "datatype": "wikibase-item"
                        },
                        "type": "statement",
                        "id": "q4384193$D58BC385-8F19-4BE5-8DDB-AE71103D7267",
                        "rank": "normal",
                        "references": [
                            {
                                "hash": "fa278ebfc458360e5aed63d5058cca83c46134f1",
                                "snaks": {
                                    "P143": [
                                        {
                                            "snaktype": "value",
                                            "property": "P143",
                                            "hash": "e4f6d9441d0600513c4533c672b5ab472dc73694",
                                            "datavalue": {
                                                "value": {
                                                    "entity-type": "item",
                                                    "numeric-id": 328,
                                                    "id": "Q328"
                                                },
                                                "type": "wikibase-entityid"
                                            },
                                            "datatype": "wikibase-item"
                                        }
                                    ]
                                },
                                "snaks-order": [
                                    "P143"
                                ]
                            }
                        ]
                    }
                ],
                "P131": [
                    {
                        "mainsnak": {
                            "snaktype": "value",
                            "property": "P131",
                            "hash": "4f381ed7277186469905a1bbee29dc3625df9fa6",
                            "datavalue": {
                                "value": {
                                    "entity-type": "item",
                                    "numeric-id": 829,
                                    "id": "Q829"
                                },
                                "type": "wikibase-entityid"
                            },
                            "datatype": "wikibase-item"
                        },
                        "type": "statement",
                        "id": "q4384193$6B698108-CDEB-4916-9E91-86972595B132",
                        "rank": "normal",
                        "references": [
                            {
                                "hash": "fa278ebfc458360e5aed63d5058cca83c46134f1",
                                "snaks": {
                                    "P143": [
                                        {
                                            "snaktype": "value",
                                            "property": "P143",
                                            "hash": "e4f6d9441d0600513c4533c672b5ab472dc73694",
                                            "datavalue": {
                                                "value": {
                                                    "entity-type": "item",
                                                    "numeric-id": 328,
                                                    "id": "Q328"
                                                },
                                                "type": "wikibase-entityid"
                                            },
                                            "datatype": "wikibase-item"
                                        }
                                    ]
                                },
                                "snaks-order": [
                                    "P143"
                                ]
                            }
                        ]
                    }
                ],
                "P646": [
                    {
                        "mainsnak": {
                            "snaktype": "value",
                            "property": "P646",
                            "hash": "e463f1d52a2575058a64100f5b9d0b6a069ddb07",
                            "datavalue": {
                                "value": "/m/020swm",
                                "type": "string"
                            },
                            "datatype": "external-id"
                        },
                        "type": "statement",
                        "id": "Q4384193$FBC9D5F4-1A2C-452A-9EB7-6D673B3B4260",
                        "rank": "normal",
                        "references": [
                            {
                                "hash": "2b00cb481cddcac7623114367489b5c194901c4a",
                                "snaks": {
                                    "P248": [
                                        {
                                            "snaktype": "value",
                                            "property": "P248",
                                            "hash": "a94b740202b097dd33355e0e6c00e54b9395e5e0",
                                            "datavalue": {
                                                "value": {
                                                    "entity-type": "item",
                                                    "numeric-id": 15241312,
                                                    "id": "Q15241312"
                                                },
                                                "type": "wikibase-entityid"
                                            },
                                            "datatype": "wikibase-item"
                                        }
                                    ],
                                    "P577": [
                                        {
                                            "snaktype": "value",
                                            "property": "P577",
                                            "hash": "fde79ecb015112d2f29229ccc1ec514ed3e71fa2",
                                            "datavalue": {
                                                "value": {
                                                    "time": "+2013-10-28T00:00:00Z",
                                                    "timezone": 0,
                                                    "before": 0,
                                                    "after": 0,
                                                    "precision": 11,
                                                    "calendarmodel": "http://www.wikidata.org/entity/Q1985727"
                                                },
                                                "type": "time"
                                            },
                                            "datatype": "time"
                                        }
                                    ]
                                },
                                "snaks-order": [
                                    "P248",
                                    "P577"
                                ]
                            }
                        ]
                    }
                ],
                "P31": [
                    {
                        "mainsnak": {
                            "snaktype": "value",
                            "property": "P31",
                            "hash": "3ec68f33f95bdb50058c06227353a39dcd8de59e",
                            "datavalue": {
                                "value": {
                                    "entity-type": "item",
                                    "numeric-id": 3010369,
                                    "id": "Q3010369"
                                },
                                "type": "wikibase-entityid"
                            },
                            "datatype": "wikibase-item"
                        },
                        "type": "statement",
                        "id": "Q4384193$cca8ce14-49a0-f233-db49-8491e4497fe4",
                        "rank": "normal"
                    },
                    {
                        "mainsnak": {
                            "snaktype": "value",
                            "property": "P31",
                            "hash": "b40f27005120e4368024d643046c8e10755831b0",
                            "datavalue": {
                                "value": {
                                    "entity-type": "item",
                                    "numeric-id": 4989906,
                                    "id": "Q4989906"
                                },
                                "type": "wikibase-entityid"
                            },
                            "datatype": "wikibase-item"
                        },
                        "type": "statement",
                        "id": "Q4384193$a0c63eb7-4fb0-09e3-ee74-969affa1560c",
                        "rank": "normal"
                    }
                ],
                "P373": [
                    {
                        "mainsnak": {
                            "snaktype": "value",
                            "property": "P373",
                            "hash": "7a608dabbbf73509efe29c3d05d0933ab9dae75d",
                            "datavalue": {
                                "value": "Golden Spike National Historic Site",
                                "type": "string"
                            },
                            "datatype": "string"
                        },
                        "type": "statement",
                        "id": "Q4384193$447A0DC7-4FE5-444B-999F-524AB76DA694",
                        "rank": "normal",
                        "references": [
                            {
                                "hash": "fa278ebfc458360e5aed63d5058cca83c46134f1",
                                "snaks": {
                                    "P143": [
                                        {
                                            "snaktype": "value",
                                            "property": "P143",
                                            "hash": "e4f6d9441d0600513c4533c672b5ab472dc73694",
                                            "datavalue": {
                                                "value": {
                                                    "entity-type": "item",
                                                    "numeric-id": 328,
                                                    "id": "Q328"
                                                },
                                                "type": "wikibase-entityid"
                                            },
                                            "datatype": "wikibase-item"
                                        }
                                    ]
                                },
                                "snaks-order": [
                                    "P143"
                                ]
                            }
                        ]
                    }
                ]
            },
            "sitelinks": {
                "enwiki": {
                    "site": "enwiki",
                    "title": "Golden spike",
                    "badges": []
                },
                "jawiki": {
                    "site": "jawiki",
                    "title": "\u30b4\u30fc\u30eb\u30c7\u30f3\u30fb\u30b9\u30d1\u30a4\u30af",
                    "badges": []
                },
                "nowiki": {
                    "site": "nowiki",
                    "title": "Golden Spike",
                    "badges": []
                },
                "ptwiki": {
                    "site": "ptwiki",
                    "title": "Golden Spike",
                    "badges": []
                },
                "ruwiki": {
                    "site": "ruwiki",
                    "title": "\u0417\u043e\u043b\u043e\u0442\u043e\u0439 \u043a\u043e\u0441\u0442\u044b\u043b\u044c",
                    "badges": []
                }
            }
        }

expected_reply = {
 'found_matches': True,
 'osm': [{'distance': 3,
          'existing': False,
          'id': 1834851585,
          'lat': 41.6179818,
          'lon': -112.55159,
          'match': True,
          'tags': {'historic': 'monument', 'name': 'Golden Spike'},
          'type': 'node'}],
 'response': 'ok',
 'search': {'criteria': ['Tag:historic=monument', 'Tag:tourism=artwork'],
            'radius': 1000},
 'wikidata': {'aliases': {'en': [{'language': 'en', 'value': 'Final spike'}],
                          'pt': [{'language': 'pt', 'value': 'Last Spike'}]},
              'item': 'Q4384193',
              'labels': {'de': {'language': 'de', 'value': 'Golden Spike'},
                         'en': {'language': 'en', 'value': 'Golden spike'},
                         'he': {'language': 'he', 'value': 'יתד הזהב'},
                         'ja': {'language': 'ja', 'value': 'ゴールデン・スパイク'},
                         'pt': {'language': 'pt', 'value': 'Golden Spike'},
                         'ru': {'language': 'ru', 'value': 'Золотой костыль'},
                         'zh-cn': {'language': 'zh-cn', 'value': '金色道钉'},
                         'zh-hant': {'language': 'zh-hant', 'value': '金色道釘'}},
              'lat': 41.617963888889,
              'lon': -112.55163055556,
              'sitelinks': {'enwiki': {'site': 'enwiki',
                                       'title': 'Golden spike'},
                            'jawiki': {'site': 'jawiki', 'title': 'ゴールデン・スパイク'},
                            'nowiki': {'site': 'nowiki',
                                       'title': 'Golden Spike'},
                            'ptwiki': {'site': 'ptwiki',
                                       'title': 'Golden Spike'},
                            'ruwiki': {'site': 'ruwiki',
                                       'title': 'Золотой костыль'}}}
}

def test_api_get(monkeypatch):
    monkeypatch.delattr('requests.sessions.Session.request')
    with pytest.raises(werkzeug.exceptions.NotFound):
        view.api_get(30, {}, 1000)

    monkeypatch.setattr(wikidata,
                        'get_entities',
                        lambda ids: [{'labels': {'en': {'language': 'en',
                                                        'value': 'Utah'}}}])

    monkeypatch.setattr(wikidata.WikidataItem,
                        'report_broken_wikidata_osm_tags',
                        lambda self: None)

    # monkeypatch.setattr(model.Item.query, 'get', lambda self, item_id: None)

    class MockQuery:
        def get(self):
            pass

    monkeypatch.setattr(view.Item, 'query', MockQuery)
    monkeypatch.setattr(overpass, 'get_existing', lambda qid: [])

    def item_query(oql, qid, radius):
        return [{
            'type': 'node',
            'id': 1834851585,
            'lat': 41.6179818,
            'lon': -112.55159,
            'tags': {'historic': 'monument', 'name': 'Golden Spike'}
        }]
    monkeypatch.setattr(overpass, 'item_query', item_query)

    item_id = 4384193
    qid = f'Q{item_id}'
    entity = wikidata.WikidataItem(qid, test_entity)
    entity._osm_keys = [{
        'item': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q4989906'},
        'tag': {'type': 'literal', 'value': 'Tag:historic=monument'},
        'itemLabel': {'xml:lang': 'en', 'type': 'literal', 'value': 'monument'}
    }, {
        'item': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q838948'},
        'tag': {'type': 'literal', 'value': 'Tag:tourism=artwork'},
        'itemLabel': {'xml:lang': 'en', 'type': 'literal', 'value': 'work of art'}
    }]

    data_dir = os.path.normpath(os.path.split(__file__)[0] + '/../data')
    class MockApp:
        config = {'DATA_DIR': data_dir}

    monkeypatch.setattr(matcher, 'current_app', MockApp)
    ret = view.api_get(wikidata_id=4384193, entity=entity, radius=1000)

    assert ret == expected_reply

    def get_existing_rate_limited(qid):
        raise overpass.RateLimited

    monkeypatch.setattr(overpass, 'get_existing', get_existing_rate_limited)

    ret = view.api_get(wikidata_id=4384193, entity=entity, radius=1000)
    del ret['wikidata']
    pprint(ret)

    rate_limited_error = {
        'error': 'overpass rate limited',
        'found_matches': False,
        'response': 'error',
        'search': {
            'criteria': ['Tag:historic=monument', 'Tag:tourism=artwork'],
            'radius': 1000,
        }
    }

    assert ret == rate_limited_error

    del entity.claims['P625']
    ret = view.api_get(wikidata_id=4384193, entity=entity, radius=1000)

    expect_no_coords = {
        'error': 'no coordinates',
        'found_matches': False,
        'response': 'error',
        'search': {
            'criteria': ['Tag:historic=monument', 'Tag:tourism=artwork'],
            'radius': 1000
        },
    }
    del ret['wikidata']

    assert ret == expect_no_coords
