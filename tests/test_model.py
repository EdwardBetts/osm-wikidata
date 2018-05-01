from matcher.model import Item
from matcher import matcher
import os.path

class MockApp:
    config = {'DATA_DIR': os.path.normpath(os.path.split(__file__)[0] + '/../data')}

def test_item_first_paragraph():
    extract = '<p><b>Gotham Bar and Grill</b> is a New American restaurant located at 12 East 12th Street (between Fifth Avenue and University Place), in Greenwich Village in Manhattan, in New York City. It opened in 1984.</p>\n<p>It is owned by American chef Alfred Portale, one of the founders of New American cuisine, who is also its chef. He arrived at the restaurant in 1985.</p>\n<p></p>'
    item = Item(extracts={'enwiki': extract})
    first = item.first_paragraph()
    assert first == '<p><b>Gotham Bar and Grill</b> is a New American restaurant located at 12 East 12th Street (between Fifth Avenue and University Place), in Greenwich Village in Manhattan, in New York City. It opened in 1984.</p>'

    extract = '''<p><span></span></p>

<p><b>IFC Center</b> is an art house movie theater in Greenwich Village, New York City in the United States of America. Located at 323 Sixth Avenue (Also known as 323 Avenue of the Americas) at West 3rd Street, it was formerly the Waverly Theater, a well- known art house movie theater. IFC Center is owned by AMC Networks (known until July 1, 2011 as Rainbow Media), the entertainment company that owns the cable channels AMC, IFC, WE tv and Sundance Channel and the film company IFC Films.</p>
<p>AMC Networks has positioned the theater as an extension of its cable channel IFC (Independent Film Channel) as IFC will own the building.</p>
<p></p>'''
    item = Item(extracts={'enwiki': extract})
    first = item.first_paragraph()
    assert first == '<p><b>IFC Center</b> is an art house movie theater in Greenwich Village, New York City in the United States of America. Located at 323 Sixth Avenue (Also known as 323 Avenue of the Americas) at West 3rd Street, it was formerly the Waverly Theater, a well- known art house movie theater. IFC Center is owned by AMC Networks (known until July 1, 2011 as Rainbow Media), the entertainment company that owns the cable channels AMC, IFC, WE tv and Sundance Channel and the film company IFC Films.</p>'

def test_settlement_not_building():
    test_entity = {
        'claims': {},
        'labels': {'en': {'language': 'en', 'value': 'Capistrano Beach'}},
        'sitelinks': {},
    }

    tags = ['place=neighbourhood', 'landuse=residential']
    item = Item(entity=test_entity, tags=tags)

    assert item.calculate_tags() == set(tags)

def test_calculate_tags(monkeypatch):
    monkeypatch.setattr(matcher, 'current_app', MockApp)

    test_entity = {
        'claims': {
            'P31': [{
                'mainsnak': {
                    'datatype': 'wikibase-item',
                    'datavalue': {
                        'type': 'wikibase-entityid',
                        'value': {
                              'entity-type': 'item',
                              'id': 'Q15243209',
                              'numeric-id': 15243209
                        }
                    },
                },
            }],
        },
        'labels': {
            'en': {'language': 'en', 'value': 'City Hall Historic District'},
        },
        'sitelinks': {},
    }
    tags = {'historic', 'boundary=protected_area', 'landuse=residential',
            'boundary=administrative', 'place', 'protect_class=22',
            'admin_level'}
    item = Item(entity=test_entity, tags=tags)
    result = item.calculate_tags()
    assert 'building' not in result
    assert result == tags | {'leisure=park'}
