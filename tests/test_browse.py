from matcher import browse, wikidata, nominatim
from collections import OrderedDict

def test_qid_to_search_string_country():
    label = 'United States of America'
    entity = {
        'labels': {'en': {'value': label}},
        'claims': {
            'P31': [{'mainsnak': {'datavalue': {'value': {'id': 'Q6256'}}}}]
        },
    }
    assert browse.qid_to_search_string('Q30', entity) == label

def test_qid_to_search_string(monkeypatch):
    qid = 'Q984466'
    entity = {
        'labels': {'en': {'value': 'Murray Hill'}},
    }

    def full_name_dict(qid):
        return {
            'name': 'Murray Hill',
            'up': 'Union County',
            'country_qid': 'Q30',
            'country_name': 'United States of America',
            'up_country_qid': 'Q30',
            'up_country_name': 'United States of America'
        }

    monkeypatch.setattr(wikidata, 'up_one_level', full_name_dict)
    q = browse.qid_to_search_string(qid, entity)

    assert q == 'Murray Hill, Union County, United States of America'

    def empty_name_dict(qid):
        return {}

    monkeypatch.setattr(wikidata, 'up_one_level', empty_name_dict)
    q = browse.qid_to_search_string(qid, entity)

    assert q == 'Murray Hill'

def test_hit_from_qid(monkeypatch):
    hit = {
        'place_id': '493588',
        'osm_type': 'node',
        'osm_id': '158817196',
        'boundingbox': ['40.6953293', '40.6954293',
                        '-74.4010349', '-74.4009349'],
        'lat': '40.6953793',
        'lon': '-74.4009849',
        'display_name': 'Murray Hill, New Providence, Union County, New Jersey, 07974, United States of America',
        'category': 'place',
        'type': 'neighbourhood',
        'geotext': 'POINT(-74.4009849 40.6953793)',
        'extratags': OrderedDict([
            ('wikidata', 'Q984466'),
            ('wikipedia', 'en:Murray Hill, New Jersey')
        ]),
    }

    def mock_lookup(q):
        return [hit]

    monkeypatch.setattr(nominatim, 'lookup', mock_lookup)

    qid = 'Q984466'
    hit = browse.hit_from_qid(qid, q='Murray Hill, Union County, USA')
    assert hit['osm_type'] == 'node'
