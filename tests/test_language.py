from matcher.language import get_language_label

def test_get_language_label():
    assert get_language_label('en') == 'English'
    assert get_language_label('fake') == 'fake'
