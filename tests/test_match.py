from matcher import match
import pytest

def test_prefix_name_match():
    osm = 'National Museum of Mathematics (MoMath)'
    wd = 'National Museum of Mathematics'
    assert match.prefix_name_match(osm, wd) == '(MoMath)'

    osm = 'NationalMuseumOfMathematics (MoMath)'
    wd = 'National Museum of Mathematics'
    assert match.prefix_name_match(osm, wd) == '(MoMath)'

def test_tidy_name():
    same = 'no change'
    assert match.tidy_name(same) == same
    assert match.tidy_name("saint andrew's") == "st andrew'"

    assert match.tidy_name('the old shop') == 'old shop'

    assert match.tidy_name(' ? ') == '?'

    assert match.tidy_name(' s ') == 's'

def test_match_with_words_removed():
    same = 'no change'
    assert match.match_with_words_removed(same, same, ['test'])
    assert not match.match_with_words_removed(same, same, [])

    wd = 'norwich bus station'
    osm = 'norwich'
    assert match.match_with_words_removed(osm, wd, ['bus station'])

    assert match.match_with_words_removed('Vif',
                                          'gare de Vif',
                                          ['gare de'])

def test_initials_match():
    n1 = 'TIAT'
    n2 = 'This Is A Test'

    assert match.initials_match(n1, n2)
    assert match.initials_match(n1 + ' station', n2, endings=['station'])

    n1 = 'T.I.A.T.'
    n2 = 'This Is A Test'

    assert match.initials_match(n1, n2)

    n1 = 'TIAT'
    n2 = 'This is a test'

    assert match.initials_match(n1, n2)

    assert not match.initials_match('bad', 'Bad Match Here')

    assert not match.initials_match('TO', 'to short')

    n1 = 'ТГПУ'
    n2 = 'Томский государственный педагогический университет'

    assert match.initials_match(n1, n2)

def test_no_alpha():
    assert not match.no_alpha('abc')
    assert not match.no_alpha('123abc')
    assert match.no_alpha('123')
    assert match.no_alpha('')

def test_normalize_name():
    assert match.normalize_name('TEST TEST') == 'testtest'
    assert match.normalize_name('testtest') == 'testtest'

def test_has_address():
    assert not match.has_address({})
    assert match.has_address({'addr:full': '1 Station Road'})
    assert match.has_address({'addr:housenumber': '1'})

def test_check_identifiers():
    assert match.check_identifier({}, {}) is False

    identifiers = {'iata': [(('PDX',), 'IATA airport code')]}

    assert match.check_identifier({'iata': 'PDX'}, identifiers)
    assert not match.check_identifier({'iata': 'LAX'}, identifiers)
    assert not match.check_identifier({}, identifiers)

    tag = 'seamark:light:reference'
    identifiers = {tag: [(('D123',), 'Admiralty number')]}

    assert match.check_identifier({tag: 'D 123'}, identifiers)

    url = 'http://test.org'
    identifiers = {'website': [((url,), 'website')]}
    assert match.check_identifier({'website': url}, identifiers)

    url = 'https://www.test.org'
    assert match.check_identifier({'website': url}, identifiers)

def test_split_on_upper():
    name = 'Phillips Chapel CME Church'
    parts = list(match.split_on_upper(name))
    assert parts == ['Phillips', 'Chapel', 'C', 'M', 'E', 'Church']

def test_match_name_containing_initials():
    n1 = 'Phillips Chapel CME Church'
    n2 = ' Phillips Chapel Christian Methodist Episcopal Church'
    assert match.name_containing_initials(n1, n2)

    n1 = 'Phillips Chapel C.M.E. Church'
    n2 = ' Phillips Chapel Christian Methodist Episcopal Church'
    assert match.name_containing_initials(n1, n2)

    n1 = 'Phillips Chapel CME Church'
    n2 = ' PC Christian Methodist Episcopal Church'
    assert match.name_containing_initials(n1, n2)

    assert not match.name_containing_initials("(St John's College)", 'LMBC')

    assert not match.name_containing_initials('1', '1-й общественный совет')

def test_name_match_numbers():
    assert match.name_match('Manhattan Community Board 1',
                            'Manhattan Community Board 1')

    assert not match.name_match('Manhattan Community Board 11',
                                'Manhattan Community Board 1')

    assert not match.name_match('Manhattan Community Board 1',
                                'Manhattan Community Board 11')

    assert not match.name_containing_initials('Manhattan Community Board 1',
                                              'Manhattan Community Board 11')

    osm_tags = {
        'name': 'Manhattan Community Board 11',
    }
    wikidata_names = {
        '1-й общественный совет': [('label', 'ru')],
        'Manhattan Community Board 1': [('label', 'en'),
                                        ('sitelink', 'enwiki'),
                                        ('extract', 'enwiki')],
    }
    assert not match.check_for_match(osm_tags, wikidata_names)

def test_russian_doesnt_match_number():
    assert not match.name_match_main('1', '1-й общественный совет')

def test_name_match():
    assert not match.name_match('', '')
    assert match.name_match('test', 'test')
    assert match.name_match('the old shop', 'old shop')

    assert not match.name_match('test', '')
    assert not match.name_match('', 'test')
    assert match.name_match('test', 'test.')
    assert match.name_match('test.', 'test')

    assert not match.name_match('test', '.')
    assert not match.name_match('.', 'test')

    assert not match.name_match('aaa', 'bbb')

    assert not match.name_match('aaa', 'the ')

    assert match.name_match('aaa-bbb', 'aaa bbb')
    assert match.name_match('the old shop', 'old shop')
    assert match.name_match('the bull', 'bull public house',
                            ['public house'])
    assert match.name_match('TIAT', 'This Is A Test')

    assert match.name_match('John Smith', 'Statue of John Smith')
    assert match.name_match('John Smith', 'Tomb of John Smith')

    name = "St John's Church"
    assert match.name_match(name, name + ' And Attached Railings')

    assert match.name_match('Church building', 'Church')
    assert match.name_match('Church', 'Church building')

    assert match.name_match('Lake Test', 'Test', ['lake'])
    assert match.name_match('Test', 'Lake Test', ['lake'])

    assert match.name_match('Test', 'Test, Washington, DC')

    assert match.name_match('aaa bbb', 'bbb aaa')

    assert match.name_match('Vif', 'gare de Vif', endings=['gare'])
    assert match.name_match('Vif', 'station Vif', endings=['station'])

    osm = 'St Peter & St Paul'
    wd = 'St Peter and St Paul, Bromley'
    assert match.name_match(osm, wd)

    osm = 'New York Skyports Incorporated Seaplane Base'
    wikidata = 'New York Skyports Inc. Seaplane Base'
    assert match.name_match(osm, wikidata)

    osm = 'Disneyland Pacific Hotel; Pacific Hotel'
    wikidata = 'Disneyland Pacific Hotel'
    assert match.name_match(osm, wikidata)

    osm = 'Leeds Bradford International'
    wikidata = 'Leeds Bradford International Airport'
    trim = ['airport', 'international airport']
    assert match.name_match(osm, wikidata, endings=trim)

    osm = 'Bresso'
    wikidata = 'Aeroporto di Bresso'
    trim = ['aeroporto']
    assert match.name_match(osm, wikidata, endings=trim)

    assert match.name_match('Rainbow Grocery Coop',
                            'Rainbow Grocery Cooperative')

def test_match_with_words_removed_both():
    osm = 'Oxmoor Mall'.lower()
    wd = 'Oxmoor Center'.lower()
    endings = ['mall', 'center']
    m = match.match_with_words_removed(osm, wd, endings)
    assert m.match_type.name == 'both_trimmed'

def test_name_match_trim_both():
    m = match.name_match('Oxmoor Mall', 'Oxmoor Center',
                          endings=['mall', 'center'])
    assert m.match_type.name == 'both_trimmed'

    m = match.name_match('Castle House', 'The Castle Inn',
                         endings=['house', 'inn'])
    assert m.match_type.name == 'both_trimmed'

def test_name_contains_initials():
    osm = 'RGC – Rainbow Grocery Coop'
    name = match.drop_initials(osm)
    assert name == 'Rainbow Grocery Coop'

    osm = 'R.G.C. – Rainbow Grocery Coop'
    name = match.drop_initials(osm)
    assert name == 'Rainbow Grocery Coop'

    osm = 'Rainbow Grocery Coop RGC'
    name = match.drop_initials(osm)
    assert name == 'Rainbow Grocery Coop'

    osm = 'Rainbow Grocery Coop (RGC)'
    name = match.drop_initials(osm)
    assert name == 'Rainbow Grocery Coop'

def test_name_match_initials_then_name():
    osm = 'RGC – Rainbow Grocery Coop'
    wd = 'Rainbow Grocery Coop'
    assert match.name_match(osm, wd)

def test_name_match_trim_to_empty():
    osm = 'Hall'
    wd = 'Post Office'
    endings = ['hall', 'post office']

    assert not match.match_with_words_removed(osm.lower(),
                                              wd.lower(),
                                              endings)

    assert not match.name_match(osm, wd, endings=endings)

def test_name_match_roman_catholic():
    assert match.name_match("St. Paul's Roman Catholic Church",
                            "St. Paul's Catholic Church")

def test_match_name_abbreviation():
    wikidata_names = [
        'Bishop Justus Church of England School',
        'Bishop Justus CE School',
    ]

    for wd in wikidata_names:
        assert match.name_match('Bishop Justus CofE School ', wd)

    assert match.name_match('St Peter', 'Saint Peter')
    assert match.name_match('Test Roman Catholic church', 'Test RC church')

@pytest.mark.skip(reason="todo")
def test_match_name_parish_church():
    osm = 'Church of St Peter & St Paul'
    wd = 'St Peter and St Paul, Bromley'
    assert match.name_match(osm, wd, ['church of'])

    osm = 'Bromley Parish Church of St Peter & St Paul'
    wd = 'St Peter and St Paul, Bromley'
    assert match.name_match(osm, wd, ['Parish Church of'])

def test_get_names():
    assert match.get_names({}) == {}
    assert match.get_names({'name': 'test'}) == {'name': 'test'}
    assert match.get_names({'operator': 'test'}) == {'operator': 'test'}
    assert match.get_names({'name:left': 'test'}) == {}

@pytest.mark.skip(reason="get_wikidata_names is unused code")
def test_get_wikidata_names():
    item = {'labels': {}}
    assert match.get_wikidata_names(item) == {}

    item = {'labels': {'en': 'test'}}
    expect = {'test': [('label', 'en')]}
    assert dict(match.get_wikidata_names(item)) == expect

    item = {'labels': {'en': 'test', 'ar': 'test'}}
    assert dict(match.get_wikidata_names(item)) == expect

    item = {
        'labels': {'en': 'test', 'ar': 'test'},
        'sitelinks': {'enwiki': 'test', 'arwiki': 'test'},
    }
    expect = {'test': [('label', 'en'), ('sitelink', 'enwiki')]}
    assert dict(match.get_wikidata_names(item)) == expect

def test_check_name_matches_address():
    assert not match.check_name_matches_address({}, [])

    tags = {'addr:housenumber': '12', 'addr:street': 'Station Road'}
    assert match.check_name_matches_address(tags, ['12 Station Road'])
    assert match.check_name_matches_address(tags, ['12, Station Road'])
    assert match.check_name_matches_address(tags, ['Number 12 Station Road'])
    tags = {'addr:housenumber': '12-14', 'addr:street': 'Station Road'}
    assert match.check_name_matches_address(tags, ['Nos 12-14 Station Road'])

    assert not match.check_name_matches_address(tags, ['Station Road'])

    tags = {'addr:full': '12 Station Road'}
    assert match.check_name_matches_address(tags, ['12 Station Road'])

    tags = {'addr:full': 'Station Road'}
    assert not match.check_name_matches_address(tags, ['12 Station Road'])

def test_check_name_matches_address_postcode():
    tags = {
        'addr:housenumber': '12',
        'addr:street': 'Buckingham Street',
    }
    assert match.check_name_matches_address(tags, ['12, Buckingham Street Wc2'])

    tags = {
        'addr:housenumber': '12',
        'addr:street': 'Buckingham Street',
        'addr:postcode': 'WC2N 6DF',
    }
    assert match.check_name_matches_address(tags, ['12, Buckingham Street Wc2'])

    tags = {
        'addr:housenumber': '12',
        'addr:street': 'Buckingham Street',
        'addr:postcode': 'EC1X 1AA',
    }
    assert not match.check_name_matches_address(tags, ['12, Buckingham Street Wc2'])

    tags = {'addr:full': '12 Buckingham Street'}
    assert match.check_name_matches_address(tags, ['12, Buckingham Street Wc2'])

def test_check_for_address_in_extract():
    osm_tags = {
        'addr:street': 'West 43rd Street',
        'addr:housenumber': '4',
    }

    extract = ('Aeolian Hall was a concert hall in midtown Manhattan in ' +
               'New York City, located on the third floor of ' +
               '29-33 West 42nd Street (also 34 West 43rd Street, from the ' +
               'other side) across the street from Bryant Park.')

    assert not match.check_for_address_in_extract(osm_tags, extract)

    osm_tags = {'addr:street': 'Station Road', 'addr:housenumber': '10'}
    extract = 'Test House, located at 10 Station Road is a test.'
    assert match.check_for_address_in_extract(osm_tags, extract)

    extract = ('The Pinball Hall of Fame is a museum for pinball machines ' +
               'that opened in Paradise, Nevada in January 2006. It is ' +
               'located at 1610 E Tropicana Ave.')
    osm_tags = {
        'addr:city': 'Las Vegas',
        'addr:street': 'East Tropicana Avenue',
        'addr:postcode': '89119',
        'addr:housenumber': '1610',
    }
    assert match.check_for_address_in_extract(osm_tags, extract)

def test_check_for_match():
    assert match.check_for_match({}, []) == {}

    osm_tags = {'addr:city': 'Rome', 'name': 'test', 'alt_name': 'test'}
    wd_names = {'test': [('label', 'en')]}

    expect = {
        'alt_name': [('good', 'test', [('label', 'en')])],
        'name': [('good', 'test', [('label', 'en')])],
    }

    assert match.check_for_match(osm_tags, wd_names) == expect

    osm_tags = {'name': 'Burgers and Cupcakes'}
    wd_names = {
        'Baryshnikov Arts Center': [('label', 'en')],
        'BAC': [('extract', 'en')],
    }
    assert match.check_for_match(osm_tags, wd_names) == {}

    del wd_names['Baryshnikov Arts Center']
    assert match.check_for_match(osm_tags, wd_names)

    osm_tags = {'name': 'National Museum of Mathematics (MoMath)'}
    wd_names = {
        'National Museum of Mathematics': [('label', 'en')],
        'Momath': [('alias', 'en')],
        'Museum of Mathematics': [('alias', 'en')],
    }

    expect = {
        'name': [('prefix', 'National Museum of Mathematics', [('label', 'en')])],
    }

    assert match.check_for_match(osm_tags, wd_names) == expect

def test_get_all_matches():
    tags = {'name': 'test'}
    names = {'test': [('label', 'en'), ('sitelink', 'enwiki')]}
    match_list = match.get_all_matches(tags, names)
    assert len(match_list) == 1
    m = match_list[0]
    assert m.osm_name == 'test'
    assert m.osm_key == 'name'
    assert m.wikidata_name == 'test'
    assert m.wikidata_source == [('label', 'en'), ('sitelink', 'enwiki')]

@pytest.mark.skip(reason="broken code")
def test_get_all_matches_address():
    tags = {'addr:housenumber': '12', 'addr:street': 'Station Road'}
    names = {'12 Station Road': [('label', 'en')]}
    match_list = match.get_all_matches(tags, names)
    assert len(match_list) == 1

def test_match_operator_at_start_of_name():
    osm_tags = {
        'highway': 'services',
        'landuse': 'commercial',
        'name': 'Welcome Break Gordano Services',
        'operator': 'Welcome Break',
    }

    wd_names = {'Gordano services': [('label', 'en')]}
    expect = {'name': [('good', 'Gordano services', [('label', 'en')])]}

    assert match.check_for_match(osm_tags, wd_names) == expect
