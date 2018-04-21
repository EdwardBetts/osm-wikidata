from matcher.overpass import oql_from_tag, oql_for_area, group_tags
from pprint import pprint

tags = ['admin_level', 'amenity=arts_centre',
        'amenity=astronomical_observatory', 'amenity=bar', 'amenity=clock',
        'amenity=college', 'amenity=community_centre', 'amenity=concert_hall',
        'amenity=conference_centre', 'amenity=courthouse',
        'amenity=grave_yard', 'amenity=hospital', 'amenity=library',
        'amenity=marketplace', 'amenity=monastery', 'amenity=music_venue',
        'site=school', 'site=station', 'site=university', 'sport', 'tourism',
        'type=bridge', 'type=site', 'waterway=lock_gate']

def test_oql_from_tag():
    ret = oql_from_tag('site', filters='area.a')

    assert ret == ['\n    rel(area.a)[site][~"^(addr:housenumber|.*name.*)$"~".",i];']

def test_oql_for_area():
    bbox = 'bbox:52.157942,0.068639,52.237230,0.184552'
    oql = oql_for_area('rel', 295355, ['amenity=library'], bbox, '')
    expect = '''
[timeout:600][out:xml][bbox:bbox:52.157942,0.068639,52.237230,0.184552];
area(3600295355) -> .a;
(
node(area.a)["amenity"="library"];
way(area.a)["amenity"="library"];
rel(area.a)["amenity"="library"];
) -> .b;
(
    rel(295355);
    node.b[~"^(addr:housenumber|.*name.*)$"~".",i];
    way.b[~"^(addr:housenumber|.*name.*)$"~".",i];
    rel.b[~"^(addr:housenumber|.*name.*)$"~".",i];

);
(._;>;);
out;'''

    assert oql == expect

    oql = oql_for_area('rel', 295355, tags, bbox, '')

    expect = '''
[timeout:600][out:xml][bbox:bbox:52.157942,0.068639,52.237230,0.184552];
area(3600295355) -> .a;
(
node(area.a)["admin_level"];
way(area.a)["admin_level"];
rel(area.a)["admin_level"];
node(area.a)["amenity"~"^(arts_centre|astronomical_observatory|bar|clock|college|community_centre|concert_hall|conference_centre|courthouse|grave_yard|hospital|library|marketplace|monastery|music_venue)$"];
way(area.a)["amenity"~"^(arts_centre|astronomical_observatory|bar|clock|college|community_centre|concert_hall|conference_centre|courthouse|grave_yard|hospital|library|marketplace|monastery|music_venue)$"];
rel(area.a)["amenity"~"^(arts_centre|astronomical_observatory|bar|clock|college|community_centre|concert_hall|conference_centre|courthouse|grave_yard|hospital|library|marketplace|monastery|music_venue)$"];
rel(area.a)["site"~"^(school|station|university)$"];
node(area.a)["sport"];
way(area.a)["sport"];
rel(area.a)["sport"];
node(area.a)["tourism"];
way(area.a)["tourism"];
rel(area.a)["tourism"];
rel(area.a)["type"~"^(bridge|site)$"];
node(area.a)["waterway"="lock_gate"];
way(area.a)["waterway"="lock_gate"];
rel(area.a)["waterway"="lock_gate"];
) -> .b;
(
    rel(295355);
    node.b[~"^(addr:housenumber|.*name.*)$"~".",i];
    way.b[~"^(addr:housenumber|.*name.*)$"~".",i];
    rel.b[~"^(addr:housenumber|.*name.*)$"~".",i];

);
(._;>;);
out;'''

    assert oql == expect

def test_group_tags():
    ret = group_tags(tags)

    expect = {
        'admin_level': [],
        'amenity': ['arts_centre',
                    'astronomical_observatory',
                    'bar',
                    'clock',
                    'college',
                    'community_centre',
                    'concert_hall',
                    'conference_centre',
                    'courthouse',
                    'grave_yard',
                    'hospital',
                    'library',
                    'marketplace',
                    'monastery',
                    'music_venue'],
        'site': ['school', 'station', 'university'],
        'sport': [],
        'tourism': [],
        'type': ['bridge', 'site'],
        'waterway': ['lock_gate']
    }

    assert ret == expect
