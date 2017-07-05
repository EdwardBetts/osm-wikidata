from matcher import nominatim
import vcr

@vcr.use_cassette()
def test_nominatim_lookup(app):
    ret = nominatim.lookup('Bristol')
    assert len(ret) == 20

    expect = ['address', 'boundingbox', 'category', 'display_name',
              'extratags', 'geotext', 'icon', 'importance', 'lat',
              'licence', 'lon', 'namedetails', 'osm_id', 'osm_type',
              'place_id', 'place_rank', 'type']

    assert sorted(ret[0].keys()) == expect
