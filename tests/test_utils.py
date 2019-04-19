from matcher import utils
import pytest

def test_normalize_url():
    expect = 'test.org'
    samples = ['test.org', 'www.test.org',
               'http://test.org', 'https://test.org',
               'http://www.test.org', 'https://www.test.org']

    for sample in samples:
        assert utils.normalize_url(sample) == expect

def test_capfirst():
    assert utils.capfirst(None) is None
    assert utils.capfirst('') == ''
    assert utils.capfirst('test') == 'Test'
    assert utils.capfirst('Test') == 'Test'
    assert utils.capfirst('TEST') == 'TEST'
    assert utils.capfirst('tEST') == 'TEST'
    assert utils.capfirst('test test') == 'Test test'

def test_flatten():
    sample = [[1, 2], [3, 4]]
    assert utils.flatten(sample) == [1, 2, 3, 4]

def test_drop_start():
    assert utils.drop_start('aaabbb', 'aaa') == 'bbb'

    with pytest.raises(AssertionError):
        assert utils.drop_start('aaabbb', 'ccc')

def test_remove_start():
    assert utils.remove_start('aaabbb', 'aaa') == 'bbb'
    assert utils.remove_start('aaabbb', 'ccc') == 'aaabbb'

def test_display_distance():
    units = 'km_and_metres'
    assert utils.display_distance(units, 10) == '10 m'
    assert utils.display_distance(units, 500) == '500 m'
    assert utils.display_distance(units, 1000) == '1.00 km'

    units = 'miles_and_feet'
    assert utils.display_distance(units, 500) == '1,640 feet'
    assert utils.display_distance(units, 1000) == '0.62 miles'
    assert utils.display_distance(units, 10_000) == '6.21 miles'

    units = 'miles_and_yards'
    assert utils.display_distance(units, 500) == '547 yards'
    assert utils.display_distance(units, 1000) == '0.62 miles'
    assert utils.display_distance(units, 10_000) == '6.21 miles'

    units = 'miles_and_metres'
    assert utils.display_distance(units, 500) == '500 metres'
    assert utils.display_distance(units, 1000) == '0.62 miles'
    assert utils.display_distance(units, 10_000) == '6.21 miles'

def test_is_in_range():
    address_range = 'Numbers 51 And 53 And Attached Front Railings'
    address = '51 Park Street, Bristol (whole facade)'
    assert utils.is_in_range(address_range, address)
