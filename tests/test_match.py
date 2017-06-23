from matcher.match import tidy_name, match_with_words_removed, initials_match, MatchType

def test_tidy_name():
    same = 'no change'
    assert tidy_name(same) == same
    assert tidy_name("saint andrew's") == "st andrew'"

def test_match_with_words_removed():
    same = 'no change'
    assert match_with_words_removed(same, same, ['test'])

    wd = 'norwich bus station'
    osm = 'norwich'
    assert match_with_words_removed(wd, osm, ['bus station'])

def test_initials_match():
    n1 = 'TIAT'
    n2 = 'This Is A Test'

    assert initials_match(n1, n2)
    assert initials_match(n1 + ' station', n2, endings=['station'])

    n1 = 'T.I.A.T.'
    n2 = 'This Is A Test'

    assert initials_match(n1, n2)

    assert not initials_match('bad', 'Bad Match Here')
