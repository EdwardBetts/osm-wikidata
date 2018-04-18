from matcher import utils

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

def test_remove_start():
    assert utils.remove_start('aaabbb', 'aaa') == 'bbb'
    assert utils.remove_start('aaabbb', 'ccc') == 'aaabbb'
