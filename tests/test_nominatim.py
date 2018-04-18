from matcher import nominatim
from matcher.place import Place
from collections import OrderedDict
import vcr
import json

hit_json = '''
  {
    "place_id": "179656437",
    "licence": "Data \u00a9 OpenStreetMap contributors, ODbL 1.0. http://www.openstreetmap.org/copyright",
    "osm_type": "relation",
    "osm_id": "5746665",
    "boundingbox": ["51.3972838", "51.5444317", "-2.7183704", "-2.5104192"],
    "lat": "51.4538022",
    "lon": "-2.5972985",
    "display_name": "Bristol, City of Bristol, South West England, England, United Kingdom",
    "place_rank": 16,
    "category": "place",
    "type": "city",
    "importance": 0.76973987710212,
    "icon": "http://nominatim.openstreetmap.org/images/mapicons/poi_place_city.p.20.png",
    "address": {
      "city": "Bristol",
      "county": "City of Bristol",
      "state_district": "South West England",
      "state": "England",
      "country": "United Kingdom",
      "country_code": "gb"
    },
    "geotext": "",
    "extratags": {
      "place": "city",
      "wikidata": "Q23154",
      "population": "421300"
    },
    "namedetails": {
      "name": "Bristol",
      "name:cy": "Bryste",
      "name:en": "Bristol",
      "name:eo": "Bristolo",
      "name:lt": "Bristolis"
    }
  }
'''

@vcr.use_cassette()
def test_nominatim_lookup(app):
    ret = nominatim.lookup('Bristol')
    assert len(ret) == 20

    expect = ['address', 'boundingbox', 'category', 'display_name',
              'extratags', 'geotext', 'icon', 'importance', 'lat',
              'licence', 'lon', 'namedetails', 'osm_id', 'osm_type',
              'place_id', 'place_rank', 'type']

    assert sorted(ret[0].keys()) == expect

def test_place_from_nominatim():
    hit = json.loads(hit_json, object_pairs_hook=OrderedDict)
    place = Place.from_nominatim(hit)
    assert place.osm_type == 'relation'
    expect_address = [
        ('city', 'Bristol'),
        ('county', 'City of Bristol'),
        ('state_district', 'South West England'),
        ('state', 'England'),
        ('country', 'United Kingdom'),
        ('country_code', 'gb'),
    ]
    assert len(expect_address) == len(place.address)
    for i in range(len(expect_address)):
        a = place.address[i]
        expect = expect_address[i]
        assert a['type'] == expect[0]
        assert a['name'] == expect[1]
    # assert list(place.address.items()) == expect_address

    place.address = []
    assert len(place.address) == 0
