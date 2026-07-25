from matcher.model import Item
from matcher.place import Place, bbox_chunk, bbox_chunk_dimensions
from matcher import database

def simple_place():
    place = Place(place_id=1,
                  osm_type='way',
                  osm_id=1,
                  display_name='test place',
                  category='test',
                  type='test',
                  place_rank=1,
                  south=0, west=0, north=0, east=0)
    return place

def filter_tags(tags):
    ''' Filter out lifecycle prefixes like was: and disused: '''

    prefixes = ('disused', 'was', 'abandoned', 'demolished',
                'destroyed', 'ruins', 'historic')

    return {tag for tag in tags
            if not any(tag.startswith(prefix + ':') for prefix in prefixes)}

def test_add_tags_to_items(app):
    place = simple_place()

    item = Item(item_id=1,
                tags={'amenity=library'},
                location='Point(-2.62071 51.454)',
                categories=['Museums'])
    place.items.append(item)
    database.session.add(place)
    database.session.commit()

    place.add_tags_to_items()
    expect = {
        'tourism=attraction',
        'tourism=gallery',
        'tourism=museum',
        'historic=museum',
        'building=museum',
        'amenity=library',
    }

    assert filter_tags(item.tags) == expect
    assert filter_tags(place.all_tags) == expect

def test_place_country_code(app):
    place = simple_place()
    place.address = [{'type': 'state', 'name': 'New York'},
                     {'type': 'country', 'name': 'USA'},
                     {'type': 'country_code', 'name': 'us'}]
    assert place.country_code == 'us'
    assert place.get_address_key('missing key') is None

    place = simple_place()
    place.address = {'state': 'New York',
                     'country': 'USA',
                     'country_code': 'us'}
    assert place.country_code == 'us'
    assert place.get_address_key('missing key') is None


def test_uk_name_for_change_comment_uses_place_and_constituent_country():
    place = simple_place()
    place.display_name = 'Cambridge'
    place.namedetails = {'name:en': 'Cambridge'}
    place.address = [
        {'type': 'city', 'name': 'Cambridge'},
        {'type': 'ISO3166-2-lvl6', 'name': 'GB-CAM'},
        {
            'type': 'state_district',
            'name': 'Cambridgeshire and Peterborough',
        },
        {'type': 'state', 'name': 'England'},
        {'type': 'ISO3166-2-lvl4', 'name': 'GB-ENG'},
        {'type': 'country', 'name': 'United Kingdom'},
        {'type': 'country_code', 'name': 'gb'},
    ]

    assert place.name_for_change_comment == 'Cambridge, England'


def test_name_for_change_comment_omits_iso_country_codes():
    place = simple_place()
    place.display_name = 'Springfield'
    place.namedetails = {'name:en': 'Springfield'}
    place.address = [
        {'type': 'city', 'name': 'Springfield'},
        {'type': 'ISO3166-2-lvl4', 'name': 'XX-ABC'},
        {'type': 'state', 'name': 'North Province'},
        {'type': 'country', 'name': 'Exampleland'},
        {'type': 'country_code', 'name': 'xx'},
    ]

    assert (
        place.name_for_change_comment
        == 'Springfield, North Province, Exampleland'
    )


def test_wikidata_chunk_size_uses_smaller_unchunked_area_threshold():
    place = simple_place()
    place.area = 999 * 1000 * 1000
    assert place.wikidata_chunk_size() == 1

    place.area = 1000 * 1000 * 1000
    assert place.wikidata_chunk_size() == 2


def test_bbox_chunk_square():
    bbox = (0, 3, 0, 3)

    assert bbox_chunk_dimensions(bbox, 3) == (3, 3)
    assert len(bbox_chunk(bbox, 3)) == 9


def test_bbox_chunk_wide():
    bbox = (0, 2, 0, 4)
    chunks = bbox_chunk(bbox, 3)

    assert bbox_chunk_dimensions(bbox, 3) == (2, 4)
    assert len(chunks) == 8
    assert chunks[0] == (0, 1, 0, 1)
    assert chunks[-1] == (1, 2, 3, 4)


def test_bbox_chunk_tall():
    bbox = (0, 4, 0, 2)

    assert bbox_chunk_dimensions(bbox, 3) == (4, 2)
    assert len(bbox_chunk(bbox, 3)) == 8


def test_bbox_chunk_accounts_for_longitude_scale():
    bbox = (60, 63, 0, 6)

    assert bbox_chunk_dimensions(bbox, 3) == (3, 3)


def test_bbox_chunk_minimum_size():
    bbox = (0, 10, 0, 1)

    assert bbox_chunk_dimensions(bbox, 0) == (1, 1)
    assert bbox_chunk(bbox, 0) == [(0, 10, 0, 1)]
