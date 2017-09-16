from matcher.model import Item
from matcher.place import Place
from matcher import database

def test_add_tags_to_items(app):
    place = Place(place_id=1,
                  osm_type='way',
                  osm_id=1,
                  display_name='test place',
                  category='test',
                  type='test',
                  place_rank=1,
                  south=0, west=0, north=0, east=0)

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

    assert item.tags == expect

    assert place.all_tags == expect
