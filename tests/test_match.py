from matcher.match import tidy_name, match_with_words_removed

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

