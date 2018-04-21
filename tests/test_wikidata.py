from matcher import wikidata
import pytest
import vcr

test_entity = {
    'labels': {
        'fr': {'language': 'fr', 'value': 'tour Eiffel'},
        'de': {'language': 'de', 'value': 'Eiffelturm'},
        'en': {'language': 'en', 'value': 'Eiffel Tower'},
    },
    'sitelinks': {
        'enwiki': {'site': 'enwiki', 'title': 'Eiffel Tower', 'badges': []},
        'frwiki': {'site': 'frwiki', 'title': 'Tour Eiffel', 'badges': []},
        'dewiki': {'site': 'dewiki', 'title': 'Eiffelturm', 'badges': ['Q17437796']},

    },
    'aliases': {
        'en': [
            {'language': 'en', 'value': 'Tour Eiffel'},
            {'language': 'en', 'value': 'The Eiffel Tower'},
        ],
    },
    'claims': {
        'P17': [{
            'mainsnak': {
                'snaktype': 'value',
                'property': 'P17',
                'hash': 'c05db5d169c2560b9799d567a803d20a392f53e4',
                'datavalue': {
                    'value': {
                        'entity-type': 'item',
                        'numeric-id': 142,
                        'id': 'Q142'
                    },
                    'type': 'wikibase-entityid'
                },
                'datatype': 'wikibase-item'
            },
            'type': 'statement',
            'id': 'q243$6032A251-115E-4835-B758-361FB095D13C',
            'rank': 'normal',
        }],
        'P31': [
            {
                'mainsnak': {
                    'property': 'P31',
                    'datavalue': {
                        'value': {'entity-type': 'item', 'id': 'Q1440476', 'numeric-id': 1440476},
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
                        'value': {'entity-type': 'item', 'id': 'Q1440300', 'numeric-id': 1440300},
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
                        'value': {'entity-type': 'item', 'id': 'Q2319498', 'numeric-id': 2319498},
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
        'P373': [{
            'mainsnak': {
                'snaktype': 'value',
                'property': 'P373',
                'hash': 'a132d2636fb4c3b7181fb0a4fb947691301fc03d',
                'datavalue': {'value': 'Eiffel Tower', 'type': 'string'},
                'datatype': 'string'
            },
            'type': 'statement',
            'id': 'q243$ed81c2ec-4645-7468-130c-f1aed6577093',
            'rank': 'normal'
        }],
        'P625': [{
            'mainsnak': {
                'property': 'P625',
                'datavalue': {
                    'value': {
                        'latitude': 48.8583,
                        'longitude': 2.2944,
                        'globe': 'http://www.wikidata.org/entity/Q2',
                        'altitude': None,
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

def test_wikidata():
    with pytest.raises(AssertionError):
        wikidata.WikidataItem('Q1', {})

    item = wikidata.WikidataItem('Q1', test_entity)
    assert item.has_coords
    assert item.has_earth_coords

    item._osm_keys = [
        {
            'item': {'value': 'http://www.wikidata.org/entity/Q56061', 'type': 'uri'},
            'itemLabel': {'value': 'administrative territorial entity', 'xml:lang': 'en', 'type': 'literal'},
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

    expect = '''[timeout:300][out:json];
(
    node(around:1000,48.85830,2.29440)["admin_level"][~"^(addr:housenumber|.*name.*)$"~".",i];
    way(around:1000,48.85830,2.29440)["admin_level"][~"^(addr:housenumber|.*name.*)$"~".",i];
    rel(around:1000,48.85830,2.29440)["admin_level"][~"^(addr:housenumber|.*name.*)$"~".",i];
    node(around:1000,48.85830,2.29440)[place=country][name];
    way(around:1000,48.85830,2.29440)[place=country][name];
    rel(around:1000,48.85830,2.29440)[place=country][name];
);
out center tags;'''.strip()

    oql = item.get_oql(criteria, 1000)
    assert oql == expect

    assert item.is_a == ['Q1440476', 'Q1440300', 'Q2319498']

    expect = {
        'Eiffel Tower': [('label', 'en'),
                         ('sitelink', 'enwiki'),
                         ('commonscat', None)],
        'Eiffelturm': [('label', 'de'), ('sitelink', 'dewiki')],
        'tour Eiffel': [('label', 'fr'), ('sitelink', 'frwiki')],
        'The Eiffel Tower': [('alias', 'en')],
        'Tour Eiffel': [('alias', 'en')],
    }

    assert item.names == expect

    assert item.coords == (48.8583, 2.2944)

    assert item.claims == test_entity['claims']
    assert item.labels == test_entity['labels']
    assert item.sitelinks == test_entity['sitelinks']
    assert item.aliases == test_entity['aliases']

    sitelinks = [
        {'code': 'en',
         'lang': 'English',
         'title': 'Eiffel Tower',
         'url': 'https://en.wikipedia.org/wiki/Eiffel_Tower'},
        {'code': 'fr',
         'lang': 'French',
         'title': 'Tour Eiffel',
         'url': 'https://fr.wikipedia.org/wiki/Tour_Eiffel'},
        {'code': 'de',
         'lang': 'German',
         'title': 'Eiffelturm',
         'url': 'https://de.wikipedia.org/wiki/Eiffelturm'}
    ]
    assert item.get_sitelinks() == sitelinks
    assert item.remove_badges() is None

    assert item.first_claim_value('P31')['id'] == 'Q1440476'

    assert item.languages_from_country() == ['fr']

    assert item.label() == 'tour Eiffel'
    assert item.label('fr') == 'tour Eiffel'
    assert item.label('en') == 'Eiffel Tower'

    assert not item.is_proposed()

@pytest.mark.skip(reason="keeps breaking, not helpful")
@vcr.use_cassette(decode_compressed_response=True)
def test_get_enwiki_query(app):
    bbox = (51.4478819, 51.4660988, -2.6318114, -2.6078598)

    expect = '''
SELECT ?place ?placeLabel (SAMPLE(?location) AS ?location) ?article WHERE {
    SERVICE wikibase:box {
        ?place wdt:P625 ?location .
        bd:serviceParam wikibase:cornerWest "Point(-2.6318114 51.4478819)"^^geo:wktLiteral .
        bd:serviceParam wikibase:cornerEast "Point(-2.6078598 51.4660988)"^^geo:wktLiteral .
    }
    ?article schema:about ?place .
    ?article schema:inLanguage "en" .
    ?article schema:isPartOf <https://en.wikipedia.org/> .
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q18340550 } .          # ignore timeline articles
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q13406463 } .          # ignore list articles
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q17362920 } .          # ignore Wikimedia duplicated pages
    FILTER NOT EXISTS { ?place wdt:P31/wdt:P279* wd:Q192611 } .  # ignore constituencies
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?place ?placeLabel ?article'''

    q = wikidata.get_enwiki_query(*bbox)

    assert q == expect

    rows = wikidata.run_query(q)
    expect = eval(open('tests/wikidata_rows').read())

    assert expect == rows

def test_wikidata_label():
    # should move this into another file
    entity_data = {
        'aliases': {},
        'claims': {'P131': [{'id': 'q1889816$4DF6C120-47C7-4856-A8C1-26F93A84E649',
                             'mainsnak': {'datatype': 'wikibase-item',
                                          'datavalue': {'type': 'wikibase-entityid',
                                                        'value': {'entity-type': 'item',
                                                                  'id': 'Q26430',
                                                                  'numeric-id': 26430}},
                                          'hash': 'c45783f168a532e9ed62dcda19b497446618fa4b',
                                          'property': 'P131',
                                          'snaktype': 'value'},
                             'rank': 'normal',
                             'references': [{'hash': '732ec1c90a6f0694c7db9a71bf09fe7f2b674172',
                                             'snaks': {'P143': [{'datatype': 'wikibase-item',
                                                                 'datavalue': {'type': 'wikibase-entityid',
                                                                               'value': {'entity-type': 'item',
                                                                                         'id': 'Q10000',
                                                                                         'numeric-id': 10000}},
                                                                 'hash': '9123b0de1cc9c3954366ba797d598e4e1ea4146f',
                                                                 'property': 'P143',
                                                                 'snaktype': 'value'}]},
                                             'snaks-order': ['P143']}],
                             'type': 'statement'}],
                   'P17': [{'id': 'q1889816$1D3E62C9-2B10-424B-A180-C119E4AB7D4D',
                            'mainsnak': {'datatype': 'wikibase-item',
                                         'datavalue': {'type': 'wikibase-entityid',
                                                       'value': {'entity-type': 'item',
                                                                 'id': 'Q55',
                                                                 'numeric-id': 55}},
                                         'hash': '3255495294545a1c2713e0724b0d3ba98a5f16db',
                                         'property': 'P17',
                                         'snaktype': 'value'},
                            'rank': 'normal',
                            'references': [{'hash': '732ec1c90a6f0694c7db9a71bf09fe7f2b674172',
                                            'snaks': {'P143': [{'datatype': 'wikibase-item',
                                                                'datavalue': {'type': 'wikibase-entityid',
                                                                              'value': {'entity-type': 'item',
                                                                                        'id': 'Q10000',
                                                                                        'numeric-id': 10000}},
                                                                'hash': '9123b0de1cc9c3954366ba797d598e4e1ea4146f',
                                                                'property': 'P143',
                                                                'snaktype': 'value'}]},
                                            'snaks-order': ['P143']}],
                            'type': 'statement'}],
                   'P18': [{'id': 'Q1889816$47AEBDCD-2306-470F-88E4-7ED30FD9155A',
                            'mainsnak': {'datatype': 'commonsMedia',
                                         'datavalue': {'type': 'string',
                                                       'value': 'Tilburg '
                                                                'natuurmuseum.jpg'},
                                         'hash': '236bcdf13b5fa66b0f10f8e48769faa95965b115',
                                         'property': 'P18',
                                         'snaktype': 'value'},
                            'rank': 'normal',
                            'type': 'statement'}],
                   'P276': [{'id': 'Q1889816$ED59D2D3-6719-45CD-8F84-CF4AEDB5288E',
                             'mainsnak': {'datatype': 'wikibase-item',
                                          'datavalue': {'type': 'wikibase-entityid',
                                                        'value': {'entity-type': 'item',
                                                                  'id': 'Q9871',
                                                                  'numeric-id': 9871}},
                                          'hash': 'f468c117fbbfda1291a36e6d2b7069845ace66c0',
                                          'property': 'P276',
                                          'snaktype': 'value'},
                             'rank': 'normal',
                             'type': 'statement'}],
                   'P31': [{'id': 'Q1889816$44319409-53FA-44B0-A440-FD264058C57C',
                            'mainsnak': {'datatype': 'wikibase-item',
                                         'datavalue': {'type': 'wikibase-entityid',
                                                       'value': {'entity-type': 'item',
                                                                 'id': 'Q33506',
                                                                 'numeric-id': 33506}},
                                         'hash': '4cb858cccdb4e3c1fbe0aac4e40c2715bed4b17c',
                                         'property': 'P31',
                                         'snaktype': 'value'},
                            'rank': 'normal',
                            'type': 'statement'}],
                   'P373': [{'id': 'Q1889816$F0747E27-A72C-4D49-AE4A-FC9ECB2A18A5',
                             'mainsnak': {'datatype': 'string',
                                          'datavalue': {'type': 'string',
                                                        'value': 'Natuurmuseum '
                                                                 'Brabant'},
                                          'hash': '2eeb327c20810fb7e4fcbbe9784238f3ed85daa7',
                                          'property': 'P373',
                                          'snaktype': 'value'},
                             'rank': 'normal',
                             'references': [{'hash': '732ec1c90a6f0694c7db9a71bf09fe7f2b674172',
                                             'snaks': {'P143': [{'datatype': 'wikibase-item',
                                                                 'datavalue': {'type': 'wikibase-entityid',
                                                                               'value': {'entity-type': 'item',
                                                                                         'id': 'Q10000',
                                                                                         'numeric-id': 10000}},
                                                                 'hash': '9123b0de1cc9c3954366ba797d598e4e1ea4146f',
                                                                 'property': 'P143',
                                                                 'snaktype': 'value'}]},
                                             'snaks-order': ['P143']}],
                             'type': 'statement'}],
                   'P625': [{'id': 'q1889816$778364F3-522E-4EF9-A1C7-D347BCE3BF6B',
                             'mainsnak': {'datatype': 'globe-coordinate',
                                          'datavalue': {'type': 'globecoordinate',
                                                        'value': {'altitude': None,
                                                                  'globe': 'http://www.wikidata.org/entity/Q2',
                                                                  'latitude': 51.56,
                                                                  'longitude': 5.081389,
                                                                  'precision': 1e-05}},
                                          'hash': '4ece46f50ca12fb534f04dcd94605a63f84d7db0',
                                          'property': 'P625',
                                          'snaktype': 'value'},
                             'rank': 'normal',
                             'references': [{'hash': '732ec1c90a6f0694c7db9a71bf09fe7f2b674172',
                                             'snaks': {'P143': [{'datatype': 'wikibase-item',
                                                                 'datavalue': {'type': 'wikibase-entityid',
                                                                               'value': {'entity-type': 'item',
                                                                                         'id': 'Q10000',
                                                                                         'numeric-id': 10000}},
                                                                 'hash': '9123b0de1cc9c3954366ba797d598e4e1ea4146f',
                                                                 'property': 'P143',
                                                                 'snaktype': 'value'}]},
                                             'snaks-order': ['P143']}],
                             'type': 'statement'}]},
        'descriptions': {'he': {'language': 'he', 'value': 'מוזיאון בהולנד'},
                         'nl': {'language': 'nl', 'value': 'museum in Tilburg'}},
        'id': 'Q1889816',
        'labels': {'nl': {'language': 'nl', 'value': 'Natuurmuseum Brabant'},
                   'zh-cn': {'language': 'zh-cn', 'value': '布拉班特自然博物馆'},
                   'zh-hant': {'language': 'zh-hant', 'value': '布拉班特自然博物館'}},
        'lastrevid': 427447206,
        'modified': '2017-01-12T03:46:25Z',
        'ns': 0,
        'pageid': 1819963,
        'sitelinks': {'nlwiki': {'badges': [],
                                 'site': 'nlwiki',
                                 'title': 'Natuurmuseum Brabant'}},
        'title': 'Q1889816',
        'type': 'item',
    }
    # entity = wikidata.WikidataItem('Q1889816', entity_data)

def test_no_label():
    entity = wikidata.WikidataItem('Q123', {'labels': {}})
    assert entity.label() is None

def test_english_label():
    entity_data = {
        'claims': {},
        'labels': {'en': {'language': 'en', 'value': 'London'},
                   'fr': {'language': 'fr', 'value': 'Londres'}},
    }

    entity = wikidata.WikidataItem('Q84', entity_data)
    assert entity.label() == 'London'

def test_label_language_param():
    entity_data = {
        'claims': {},
        'labels': {'en': {'language': 'en', 'value': 'London'},
                   'fr': {'language': 'fr', 'value': 'Londres'}},
    }

    entity = wikidata.WikidataItem('Q84', entity_data)
    assert entity.label(lang='fr') == 'Londres'

def test_non_english_label():
    entity_data = {
        'claims': {},
        'labels': {'nl': {'language': 'nl', 'value': 'Natuurmuseum Brabant'}}
    }

    entity = wikidata.WikidataItem('Q1889816', entity_data)
    assert entity.label() == 'Natuurmuseum Brabant'

def test_names_from_entity():
    names = wikidata.names_from_entity(test_entity)

    expect = {
        'The Eiffel Tower': [('alias', 'en')],
        'Eiffel Tower': [('label', 'en'),
                         ('sitelink', 'enwiki'),
                         ('commonscat', None)],
        'Eiffelturm': [('label', 'de'), ('sitelink', 'dewiki')],
        'tour Eiffel': [('label', 'fr'), ('sitelink', 'frwiki')],
        'Tour Eiffel': [('alias', 'en')],
    }

    assert dict(names) == expect

def test_flatten_criteria():
    assert wikidata.flatten_criteria([]) == set()

def test_enwiki_url_to_title():
    start = 'https://en.wikipedia.org/wiki/'
    with pytest.raises(AssertionError):
        wikidata.enwiki_url_to_title('test')
    assert wikidata.enwiki_url_to_title(start + 'Example') == 'Example'
    assert wikidata.enwiki_url_to_title(start + 'A_B') == 'A B'
    assert wikidata.enwiki_url_to_title(start + 'A_%28B%29') == 'A (B)'

def test_wd_to_qid():
    with pytest.raises(AssertionError):
        wikidata.wd_to_qid({'type': 'uri', 'value': 'test'})

    assert not wikidata.wd_to_qid({'type': 'pnode', 'value': 'test'})

    wd = {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q42'}

    assert wikidata.wd_to_qid(wd) == 'Q42'

def test_wd_uri_to_qid():
    uri = 'http://www.wikidata.org/entity/Q42'
    assert wikidata.wd_uri_to_id(uri) == 42

def test_drop_tag_prefix():
    assert wikidata.drop_tag_prefix('Tag:name=test') == 'name=test'
    assert wikidata.drop_tag_prefix('Key:name') == 'name'

    assert wikidata.drop_tag_prefix('Key:name=test') is None
    assert wikidata.drop_tag_prefix('Tag:name') is None
    assert wikidata.drop_tag_prefix('test') is None

def test_entity_label():
    entity = {'labels': {'en': {'value': 'test'}}}
    assert wikidata.entity_label(entity) == 'test'

    entity = {'labels': {'fr': {'value': 'abc'}}}
    assert wikidata.entity_label(entity) == 'abc'

def test_claim_value():
    assert wikidata.claim_value({'mainsnak': {}}) is None
    c = {'mainsnak': {'datavalue': {'value': 'test'}}}
    assert wikidata.claim_value(c) == 'test'

def test_get_item_labels_query():
    with pytest.raises(AssertionError):
        wikidata.get_item_labels_query([])

    expect = '''
SELECT ?item ?itemLabel
WHERE {
  VALUES (?item) { (wd:Q30) (wd:Q99) }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}'''

    assert wikidata.get_item_labels_query(['Q30', 'Q99']) == expect

def test_parse_enwiki_query():
    assert wikidata.parse_enwiki_query([]) == {}

    rows = [{
        'article': {
            'type': 'uri',
            'value': 'https://en.wikipedia.org/wiki/Eiffel_Tower'},
        'location': {
            'datatype': 'http://www.opengis.net/ont/geosparql#wktLiteral',
            'type': 'literal',
            'value': 'Point(2.2953 48.858)'},
        'place': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q243'},
        'placeLabel': {'type': 'literal', 'value': 'Eiffel Tower', 'xml:lang': 'en'}
    }, {
        'article': {
            'type': 'uri',
            'value': 'https://en.wikipedia.org/wiki/Champ_de_Mars'},
        'location': {
            'datatype': 'http://www.opengis.net/ont/geosparql#wktLiteral',
            'type': 'literal',
            'value': 'Point(2.298333333 48.856111111)'},
        'place': {'type': 'uri', 'value': 'http://www.wikidata.org/entity/Q217925'},
        'placeLabel': {'type': 'literal', 'value': 'Champ de Mars', 'xml:lang': 'en'}
    }]

    expect = {
        'Q217925': {
            'enwiki': 'Champ de Mars',
            'location': 'Point(2.298333333 48.856111111)',
            'query_label': 'Champ de Mars',
            'tags': set()
        },
        'Q243': {
            'enwiki': 'Eiffel Tower',
            'location': 'Point(2.2953 48.858)',
            'query_label': 'Eiffel Tower',
            'tags': set()
        }
    }

    assert wikidata.parse_enwiki_query(rows) == expect
