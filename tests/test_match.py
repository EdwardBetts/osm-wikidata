from matcher import match

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
    assert match.match_with_words_removed(wd, osm, ['bus station'])

def test_initials_match():
    n1 = 'TIAT'
    n2 = 'This Is A Test'

    assert match.initials_match(n1, n2)
    assert match.initials_match(n1 + ' station', n2, endings=['station'])

    n1 = 'T.I.A.T.'
    n2 = 'This Is A Test'

    assert match.initials_match(n1, n2)
    assert not match.initials_match('bad', 'Bad Match Here')

    assert not match.initials_match('TO', 'to short')

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

def test_get_osm_id_and_type():
    assert match.get_osm_id_and_type('point', 1) == ('node', 1)
    assert match.get_osm_id_and_type('line', 1) == ('way', 1)
    assert match.get_osm_id_and_type('line', -1) == ('relation', 1)
    assert match.get_osm_id_and_type('polygon', 1) == ('way', 1)
    assert match.get_osm_id_and_type('polygon', -1) == ('relation', 1)

def test_name_match_main():
    assert match.name_match_main('test', 'test')
    assert match.name_match_main('the old shop', 'old shop')

    assert not match.name_match_main('test', '')
    assert not match.name_match_main('', 'test')
    assert match.name_match_main('test', 'test.')
    assert match.name_match_main('test.', 'test')

    assert not match.name_match_main('test', '.')
    assert not match.name_match_main('.', 'test')

    assert not match.name_match_main('aaa', 'bbb')

    assert not match.name_match_main('aaa', 'the ')

    assert match.name_match_main('aaa-bbb', 'aaa bbb')
    assert match.name_match_main('the old shop', 'old shop')
    assert match.name_match_main('the bull', 'bull public house',
                                 ['public house'])
    assert match.name_match_main('TIAT', 'This Is A Test')

