#!/usr/bin/python3
from collections import defaultdict
from unidecode import unidecode
from num2words import num2words
from .utils import normalize_url, any_upper

import re

from enum import Enum

re_strip_non_chars = re.compile(r'[^-@\w]', re.U)
re_strip_non_chars_and_dash = re.compile(r'[^@\w]', re.U)
re_non_char_start = re.compile(r'^[^@\w]*', re.U)
re_non_letter_start = re.compile(r'^[^A-Z]+', re.I | re.U)
re_keep_commas = re.compile(r'[^@\w, ]', re.U)
re_number_start = re.compile(r'^(?:House at )?(?:(?:Number|No)s?\.? )?(\d[-\d]*,? .*$)')
re_article = re.compile(r'^(\W*)(the|le|la|les)[- ]')
re_uk_postcode_start = re.compile(r'^[a-z][a-z]\d+[a-z]?$', re.I)
re_digits = re.compile(r'\d+')
re_plural = re.compile(r'(?<=.)e?s+\b')
re_ss = re.compile(r'\bss\b')
re_st = re.compile(r'\bst\b')
re_ss_or_st = re.compile(r'\bs[st]\b')

re_ordinal_number = re.compile(r'([0-9]+)(?:st|nd|rd|th)\b', re.I)

re_strip_words = re.compile(r'([ -])(?:the|and|a|an|at|of|de|di|le|la|les|von|pw\.)(?=\1)')

MatchType = Enum('Match', ['good', 'wikidata_trimmed', 'both_trimmed', 'trim',
                           'address', 'initials', 'initials_trim'])

road_abbr = {
    'avenue': 'ave',
    'street': 'st',
    'road': 'rd',
    'boulevard': 'blvd',
    'drive': 'dr',
    'lane': 'ln',
    'square': 'sq',
}

directions = {
    'north': 'n',
    'south': 's',
    'east': 'e',
    'west': 'w',
    'northwest': 'nw',
    'northeast': 'ne',
    'southwest': 'sw',
    'southeast': 'se',
}

abbr = {**road_abbr, **directions}

re_abbr = re.compile(r'\b(' + '|'.join(abbr.keys()) + r')\b', re.I)

re_address_common_end = re.compile('^(.+)(' + '|'.join(abbr.keys()) + '|plaza)$', re.I)

re_road_end = re.compile('^(.+)(' + '|'.join(list(road_abbr.keys()) + list(road_abbr.values())) + ') *$', re.I)

bad_name_fields = {'tiger:name_base', 'name:right',
                   'name:left', 'gnis:county_name',
                   'openGeoDB:name', 'addr:street:name',
                   'name:source'}

def no_alpha(s):
    return all(not c.isalpha() for c in s)

class Match(object):
    def __init__(self, match_type, debug=None):
        self.match_type = match_type
        self.debug = debug
        self.wikidata_name = None
        self.wikidata_source = None
        self.osm_name = None
        self.osm_key = None

def tidy_name(n):
    # expects to be passed a name in lowercase
    n = unidecode(n).strip().rstrip("'")
    n = n.replace(' no. ', ' number ')
    n = n.replace('saint ', 'st ')
    n = n.replace('mount ', 'mt ')
    n = n.replace(' mountain', ' mtn')
    n = n.replace(' county', ' co')
    n = n.replace(' church of england ', ' ce ')
    n = n.replace(' cofe ', ' ce ')
    n = n.replace(' c of e ', ' ce ')
    n = n.replace(' @ ', ' at ')
    n = n.replace(' roman catholic ', ' rc ')
    n = n.replace(' catholic ', ' rc ')
    n = n.replace(' nicolas', ' nicholas')
    n = n.replace(' bartholemew', ' bartholomew')
    n = n.replace(' preparatory school', ' prep school')
    n = n.replace(' incorporated', ' inc')
    n = n.replace(' cooperative', ' coop')
    n = n.replace(' co-operative', ' coop')
    n = n.replace(' hotel and country club', ' hotel')
    n = n.replace(' hotel and spa', ' hotel')
    n = n.replace(' missionary baptist', ' baptist')

    if n.endswith("'s"):
        n = n[:-2]

    n = re_plural.sub('', n)

    n = n.replace('ss', 's')

    n = n.replace('center', 'centre').replace('theater', 'theatre')
    return n

def drop_article(n):
    m = re_article.match(n)
    if m:
        return m.group(1) + n[m.end():]
    return n

def strip_words(n):
    return re_strip_words.sub(lambda m: m.group(1), n)

def initials_match(n1, n2, endings=None):
    n1_lc = n1.lower()
    terms = [term for term in n2.split() if term[0].isalnum()]
    initials = ''.join(term[0] for term in terms).upper()
    if len(initials) < 3 or len(n1) < 3:
        return
    if initials == n1:
        return Match(MatchType.initials)
    if initials == ''.join(c for c in n1 if c.isalnum()):
        return Match(MatchType.initials)
    if any(initials == trim for trim in
            [n1[:-len(end)].strip() for end in endings or [] if n1_lc.endswith(end.lower())]):
        return Match(MatchType.initials_trim)

    filter_words = {'of', 'de', 'di', 'at', 'i'}
    lc_terms = set(term.lower() for term in terms)
    for word in filter_words:
        if word not in lc_terms:
            continue
        ret = initials_match(n1, ' '.join(t for t in terms if t.lower() != word))
        if ret:
            return ret

def match_with_words_removed(osm, wd, words):
    if not words:
        return
    wd_char_only = re_strip_non_chars_and_dash.sub('', wd)
    osm_char_only = re_strip_non_chars_and_dash.sub('', osm)
    words = [re_strip_non_chars.sub('', w) for w in words]
    osm_versions = {osm_char_only.replace(word, '')
                    for word in words} | {osm_char_only}
    wd_versions = {wd_char_only.replace(word, '')
                    for word in words} | {wd_char_only}

    best_match = None
    for osm_filtered in osm_versions:
        if not osm_filtered:
            continue
        for wd_filtered in wd_versions:
            if not wd_filtered or osm_filtered != wd_filtered:
                continue
            if wd_filtered == wd_char_only:
                return Match(MatchType.good, 'match with words removed')
            match_type = (MatchType.both_trimmed
                          if osm_filtered != osm_char_only
                          else MatchType.wikidata_trimmed)
            best_match = Match(match_type, 'match with words removed')
    return best_match

def strip_non_chars_match(osm, wd, strip_dash=False):
    pattern = (re_strip_non_chars_and_dash
               if strip_dash
               else re_strip_non_chars)

    wc_stripped = pattern.sub('', wd)
    osm_stripped = pattern.sub('', osm)

    return wc_stripped and osm_stripped and wc_stripped == osm_stripped

def prefix_name_match(osm, wd):
    wd_lc = wd.lower()
    osm_lc = osm.lower()

    if osm_lc.startswith(wd_lc):
        return osm[len(wd):].strip()

    space = osm.find(' ')
    while space != -1:
        osm_start = osm_lc[:space]
        if strip_non_chars_match(osm_start, wd_lc):
            return osm[space:].strip()
        space = osm.find(' ', space + 1)

def check_for_intials_match(initials, name):
    if any(c.islower() for c in initials):
        return
    if len([c for c in initials if c.isupper()]) < 2:
        return
    x = bool(initials_match(initials, name))
    return x

def strip_non_char_start(s):
    return re_non_char_start.sub('', s)

def strip_non_letter_start(s):
    return re_non_letter_start.sub('', s)

def drop_initials(name):
    first_space = name.find(' ')
    if first_space == -1:
        return
    tail = strip_non_char_start(name[first_space:])

    if check_for_intials_match(name[:first_space], tail):
        return tail

    last_space = name.rfind(' ')
    if last_space == first_space:
        return
    head = strip_non_char_start(name[:last_space])
    if check_for_intials_match(name[last_space:], head):
        return head

def split_on_upper(name):
    upper_positions = [num for num, char in enumerate(name) if char.isupper()]

    xpos = 0
    for pos in upper_positions:
        text = name[xpos:pos].rstrip()
        if text:
            yield text
        xpos = pos
    text = name[xpos:].rstrip()
    if text:
        yield text

def split_on_upper_and_tidy(name):
    parts = [re_strip_non_chars.sub('', part) for part in split_on_upper(name)]
    return [part for part in parts if part]

def name_containing_initials(n1, n2):
    if not any_upper(n1) or not any_upper(n2):
        return False
    n1_split = split_on_upper_and_tidy(n1)
    n2_split = split_on_upper_and_tidy(n2)

    if len(n1_split) != len(n2_split) or len(n1_split) < 3:
        endings = [' centre', ' center']
        for end in endings:
            if not n1.lower().endswith(end):
                continue
            m = name_containing_initials(n1[:-len(end)], n2)
            if m:
                return m

        for end in endings:
            if not n2.lower().endswith(end):
                continue
            m = name_containing_initials(n1, n2[:-len(end)])
            if m:
                return m

        return False

    for part1, part2 in zip(n1_split, n2_split):
        if part1 == part2:
            continue
        if part1.isdigit() or part2.isdigit():
            return False
        if len(part1) == 1 and part2[0] == part1:
            continue
        if len(part2) == 1 and part1[0] == part2:
            continue
        return False
    return True

def plural_word_name_in_other_name(n1, n2):
    return (' ' not in n1 and
            ' ' in n2 and n1.endswith('s') and
            n1 not in n2 and
            n1[:-1] in n2)

def two_saints(n1, n2):
    return (all(' and ' in n or ' & ' in n for n in (n1, n2)) and
           ((re_ss.search(n1) and re_st.search(n2)) or
           (re_st.search(n1) and re_ss.search(n2))))

def name_match_main(osm, wd, endings=None, debug=False):
    if not wd or not osm:
        return

    wd, osm = wd.strip(), osm.strip()

    if wd == osm:
        return Match(MatchType.good, 'identical')

    osm_lc, wd_lc = osm.lower(), wd.lower()

    if two_saints(osm_lc, wd_lc):
        osm_lc = re_ss_or_st.sub('', osm_lc)
        wd_lc = re_ss_or_st.sub('', wd_lc)

    historic = ' (historic)'
    if osm_lc.endswith(historic):
        osm = osm[:-len(historic)]
        osm_lc = osm_lc[:-len(historic)]

    if wd_lc == osm_lc:
        return Match(MatchType.good, 'identical except case')

    if set(osm_lc.split()) == set(wd_lc.split()):
        return Match(MatchType.good, 'matching term sets')

    if strip_non_chars_match(osm_lc, wd_lc, strip_dash=True):
        return Match(MatchType.good, 'strip non chars and dash')

    if name_containing_initials(osm, wd):
        return Match(MatchType.good, 'name containing initials')

    if endings:
        at_pos = wd_lc.find(' at ')
        if at_pos != -1:
            start = wd_lc[:at_pos]
            if start in endings:
                endings.remove(start)

    m = (initials_match(osm, wd, endings) or
            initials_match(wd, osm, endings))
    if m:
        return m

    if strip_non_chars_match(osm_lc, wd_lc):
        return Match(MatchType.good, 'strip non chars')

    # tidy names, but don't drop lead article yet
    wd_tidy1 = tidy_name(wd_lc)
    osm_tidy1 = tidy_name(osm_lc)

    if not wd_tidy1 or not osm_tidy1:
        return

    if wd_tidy1 == osm_tidy1:
        return Match(MatchType.good, 'tidy')

    def number_to_words_match(n1, n2):
        if not any(c.isdigit() for c in n1):
            return False
        n1_words = re_digits.sub(lambda m: num2words(int(m.group(0))), n1)

        return n1_words.replace('-', ' ') == n2.replace('-', ' ')

    if (number_to_words_match(wd_tidy1, osm_tidy1) or
            number_to_words_match(osm_tidy1, wd_tidy1)):
        return Match(MatchType.good, 'number to words')

    wd_tidy2 = strip_words(wd_tidy1)
    osm_tidy2 = strip_words(osm_tidy1)

    if wd_tidy2 == osm_tidy2:
        return Match(MatchType.good, 'strip words')

    wd_tidy = drop_article(wd_tidy2)
    osm_tidy = drop_article(osm_tidy2)

    wd_names = {wd_tidy, wd_tidy1, wd_tidy2}
    osm_names = {osm_tidy, osm_tidy1, osm_tidy2}

    if wd_tidy == osm_tidy:
        return Match(MatchType.good, 'drop article')

    m = match_with_words_removed(osm_lc, wd_lc, endings)
    if m:
        if 'church' in osm_lc and 'church' in wd_lc:
            m.match_type = MatchType.good
            m.debug = 'words removed church'
        return m

    plural_in_other_name = (plural_word_name_in_other_name(osm_lc, wd_lc) or
                            plural_word_name_in_other_name(wd_lc, osm_lc))

    if endings:
        tidy_endings = [tidy_name(e) for e in endings]
        m = match_with_words_removed(osm_tidy, wd_tidy, tidy_endings)
        if m and not plural_in_other_name:
            return m

    for osm_name in osm_names:
        for wd_name in wd_names:
            if strip_non_chars_match(osm_name, wd_name, strip_dash=True):
                return Match(MatchType.good, 'strip non chars and dash after tidy')

    if 'washington, d' in wd_tidy:  # special case for Washington, D.C.
        wd_tidy = wd_tidy.replace('washington, d', 'washington d')

    for wd_name in wd_names:
        comma = wd_name.rfind(', ')
        for osm_name in osm_names:
            osm_char_only = re_strip_non_chars.sub('', osm_name)
            if comma != -1 and not osm_char_only.isdigit():
                wc_part1 = wd_name[:comma]
                if wc_part1 == osm_name or strip_non_chars_match(osm_name, wc_part1):
                    return Match(MatchType.good, 'comma strip 1')

    if wd_tidy.split() == list(reversed(osm_tidy.split())):
        return Match(MatchType.good, 'tidy name terms reversed')

    wd_tidy = re_keep_commas.sub('', wd_tidy)
    osm_tidy = re_keep_commas.sub('', osm_tidy)

    comma = wd_tidy.rfind(', ')
    if comma != -1 and not osm_tidy.isdigit():
        if wd_tidy[:comma] == osm_tidy:
            return Match(MatchType.good, 'comma strip 2')

    wd_tidy = re_strip_non_chars.sub('', wd_tidy)
    osm_tidy = re_strip_non_chars.sub('', osm_tidy)

    if plural_in_other_name:
        return

    generic = ['companybuilding', 'building', 'complex', 'office']

    for end in generic + list(endings or []):
        if wd_tidy.endswith(end) and wd_tidy[:-len(end)] == osm_tidy:
            return Match(MatchType.trim)
        if wd_tidy.startswith(end) and wd_tidy[len(end):] == osm_tidy:
            return Match(MatchType.trim)
        if osm_tidy.endswith(end) and osm_tidy[:-len(end)] == wd_tidy:
            return Match(MatchType.trim)
        if osm_tidy.startswith(end) and osm_tidy[len(end):] == wd_tidy:
            return Match(MatchType.trim)
    return

def strip_place_name(name, place_name):
    for word in 'of', 'de', 'di', 'at', 'i':
        search = f' {word} {place_name}'
        if search in name:
            return name.replace(search, '')
    if place_name + 's ' in name:
        return name.replace(place_name + 's ', '')

    return name.replace(place_name, '')

def more_place_name_varients(place_names):
    place_names = set(place_names)
    for n in set(place_names):
        for e in 'city', 'county':
            if n.lower().endswith(' ' + e) and len(n) > len(e) + 1:
                place_names.add(n[:-(len(e) + 1)])
    return place_names

def match_two_streets(osm, wd, endings=None, **kwargs):
    endings = set(endings or [])
    osm_and_list = [sep for sep in ('&', ' and ', ' And ') if sep in osm]
    if len(osm_and_list) != 1:
        return

    wd_and_list = [sep for sep in ('&', ' and ', ' And ') if sep in wd]
    if len(wd_and_list) != 1:
        return

    osm_part1, _, osm_part2 = [n.strip() for n in osm.partition(osm_and_list[0])]
    wd_part1, _, wd_part2 = [n.strip() for n in wd.partition(wd_and_list[0])]

    part1_endings = endings.copy()
    for n in osm_part1, wd_part1:
        m = re_road_end.match(n)
        if m:
            part1_endings.add(m.group(2).lower())

    part1 = name_match_main(osm_part1, wd_part1, endings=part1_endings, **kwargs)
    if not part1:
        return

    part2_endings = endings.copy()
    for n in osm_part2, wd_part2:
        m = re_road_end.match(n)
        if m:
            part2_endings.add(m.group(2).lower())

    part2 = name_match_main(osm_part2, wd_part2, endings=part2_endings, **kwargs)
    if part2:
        return part1

def name_road_end_match(osm, wd, **kwargs):
    osm = osm.strip()
    wd = wd.strip()
    if not (osm and osm[0].isdigit() and wd and wd[0].isdigit()):
        return
    m_osm = re_road_end.match(osm)
    m_wd = re_road_end.match(wd)
    if not m_osm and not m_wd:
        return
    x_osm = m_osm.group(1) if m_osm else osm
    x_wd = m_wd.group(1) if m_wd else wd
    return name_match_main(x_osm, x_wd, **kwargs)

def name_match(osm, wd, endings=None, debug=False, place_names=None):
    match = name_match_main(osm, wd, endings, debug)
    if match:
        return match

    for osm_prefix in 'old ', 'the old ', 'former ', 'disused ', 'alte ':
        if osm.lower().startswith(osm_prefix):
            match = name_match_main(osm[len(osm_prefix):], wd, endings, debug)
            if match:
                return match

    match = match_two_streets(osm, wd, endings=endings, debug=debug)
    if match:
        return match

    match = name_road_end_match(osm, wd, endings=endings, debug=debug)
    if match:
        print('name_road_end_match')
        return match

    terms = ['cottages', 'buildings', 'houses']
    # OSM might have building number, while Wikidata doesn't
    # Example: '1-3 Rectory Cottages' matches 'Rectory Cottages'
    if osm and osm[0].isdigit() and any(t in wd.lower() for t in terms):
        no_number_osm = strip_non_letter_start(osm)
        match = name_match_main(no_number_osm, wd, endings, debug)
        if match:
            match.debug = ((match.debug + ' ' if match.debug else '') +
                          '+ strip non letter start')
            return match

    osm_no_intitals = drop_initials(osm)
    if osm_no_intitals:
        match = name_match_main(osm_no_intitals, wd, endings, debug)
        if match:
            match.debug = ((match.debug + ' ' if match.debug else '') +
                          '+ drop initials')
            return match

    for start in 'Tomb of ', 'Statue of ', 'Memorial to ':
        if wd.startswith(start) and name_match_main(osm, wd[len(start):], endings):
            return Match(MatchType.trim, start.lower().strip())

    start = 'site of'
    if osm.lower().startswith(start):
        if name_match_main(osm[len(start):], wd, endings):
            return Match(MatchType.trim, 'site of')

    end = ' And Attached Railings'.lower()
    if wd.lower().endswith(end) and name_match_main(osm, wd[:-len(end)], endings):
        return Match(MatchType.trim, 'and attached railings')

    if place_names:
        for place_name in more_place_name_varients(place_names):
            if not (place_name in osm or place_name in wd):
                continue
            match = name_match_main(strip_place_name(osm, place_name),
                                    strip_place_name(wd, place_name),
                                    endings, debug)
            if match:
                return match

    if ';' not in osm:
        return
    for osm_name in osm.split(';'):
        match = name_match(osm_name.strip(), wd, endings=endings, debug=debug,
                           place_names=place_names)
        if match:
            return match

def normalize_name(name):
    name = re_ordinal_number.sub(lambda m: num2words(int(m.group(1)), to='ordinal'), name)
    return re_strip_non_chars.sub('', name.lower())

def has_address(osm_tags):
    return any('addr:' + part in osm_tags for part in ('housenumber', 'full'))

def any_url_match(osm_value, values):
    osm_url = normalize_url(osm_value)
    return any(osm_url == normalize_url(wd_url) for wd_url in values)

def check_identifier(osm_tags, item_identifiers):
    if not item_identifiers:
        return False
    for k, v in item_identifiers.items():
        for values, label in v:
            values = set(values) | {i.replace(' ', '') for i in values if ' ' in i}
            osm_value = osm_tags.get(k)
            if not osm_value:
                continue
            if osm_value in values:
                return True
            if ' ' in osm_value and osm_value.replace(' ', '') in values:
                return True
            if label == 'website' and any_url_match(osm_value, values):
                return True
            if (osm_value.isdigit() and
                    any(v.isdigit() and int(osm_value) == int(v) for v in values)):
                return True
    return False


re_range_start = re.compile(r'\d+ ?([-–+&]|and) ?$')

def check_for_address_in_extract(osm_tags, extract):
    if not extract or not has_address(osm_tags):
        return

    def address_in_extract(address):
        address = re_abbr.sub(lambda m: '(' + m.group(1) + '|' + abbr[m.group(1).lower()] + r'\.?)', re.escape(address))
        # address = re_directions.sub(lambda m: '(' + m.group(1) + '|' + m.group(1)[0] + ')', address)

        m = re.search(r'\b' + address, extract, re.I)
        if not m:
            m = re.search(r'\b' + address, extract.replace(',', ''), re.I)
        return bool(m) and not re_range_start.search(extract[:m.start()])

    if 'addr:housenumber' in osm_tags and 'addr:street' in osm_tags:
        address = osm_tags['addr:housenumber'] + ' ' + osm_tags['addr:street']
        if address_in_extract(address):
            return True

    if 'addr:full' in osm_tags and address_in_extract(osm_tags['addr:full']):
        return True

def check_name_matches_address(osm_tags, wikidata_names):
    if not has_address(osm_tags):
        return
    # if 'addr:housenumber' not in osm_tags or 'addr:street' not in osm_tags:
    #     return
    number_start_iter = (re_number_start.match(name)
                         for name in wikidata_names
                         if not name.lower().endswith(' building'))
    number_start = {m.group(1) for m in number_start_iter if m}
    if not number_start:
        return

    strip_comma = [name[:name.rfind(',')]
                   for name in set(number_start)
                   if ',' in name]
    number_start.update(n for n in strip_comma if not n.isdigit())
    norm_number_start = {normalize_name(name) for name in number_start}

    postcode = osm_tags.get('addr:postcode')
    if postcode:
        postcode = postcode.lower()

    if 'addr:housenumber' in osm_tags and 'addr:street' in osm_tags:
        osm_address = osm_tags['addr:housenumber'] + ' ' + osm_tags['addr:street']
        norm_osm_address = normalize_name(osm_address)
        if any(name == norm_osm_address for name in norm_number_start):
            return True

        for i in number_start:
            name, _, postcode_start = i.rpartition(' ')

            if postcode and not postcode.startswith(postcode_start.lower()):
                continue

            if (re_uk_postcode_start.match(postcode_start) and
                    normalize_name(name) == norm_osm_address):
                return True

        if any(name.startswith(norm_osm_address) or norm_osm_address.startswith(name)
               for name in norm_number_start):
            return  # not sure

        m = re_address_common_end.match(norm_osm_address)
        if m:
            short = m.group(1)
            if any(name.startswith(short) for name in norm_number_start):
                return

    if 'addr:full' in osm_tags:
        osm_address = normalize_name(osm_tags['addr:full'])
        if any(osm_address.startswith(name) for name in norm_number_start):
            return True

        for i in number_start:
            name, _, postcode_start = i.rpartition(' ')

            if (re_uk_postcode_start.match(postcode_start) and
                    normalize_name(name) == osm_address):
                return True

    # if we find a name from wikidata matches the OSM name we can be more relaxed
    # about the address
    name_match = 'name' in osm_tags and any(n == osm_tags['name'] for n in number_start)

    return None if name_match else False

def get_names(osm_tags):
    return {k: v for k, v in osm_tags.items()
             if ('name' in k and k not in bad_name_fields) or k == 'operator'}

def intials_matches_other_wikidata_name(initials, wikidata_names):
    return any(w != initials and initials_match(initials, w)
               for w in wikidata_names.keys())

def check_for_match(osm_tags, wikidata_names, endings=None, place_names=None, trim_house=True):
    endings = set(endings or [])
    if trim_house:
        endings.add('house')

    names = get_names(osm_tags)
    operator = names['operator'].lower() if 'operator' in names else None
    if not names or not wikidata_names:
        return {}

    if 'addr:city' in osm_tags:
        city = osm_tags['addr:city'].lower()
        endings = set(endings or [])
        endings |= {
            city,
            'in ' + city,  # English / German / Dutch
            'w ' + city,   # Polish
            'à ' + city,   # French
            'en ' + city,  # Spanish
            'em ' + city,  # Portuguese
            'v ' + city,   # Czech
            'i ' + city,   # Danish / Norwegian / Swedish
            'a ' + city,   # Italian
        }

    name = defaultdict(list)
    cache = {}
    for w, source in wikidata_names.items():
        for osm_key, o in names.items():
            if (o, w) in cache:
                result = cache[(o, w)]
                if not result:
                    continue
            else:
                m = name_match(o, w, endings, place_names=place_names)
                if not m and operator and o.lower().startswith(operator):
                    m = name_match(o[len(operator):].rstrip(), w, endings,
                                   place_names=place_names)
                    if m and m.match_type in (MatchType.both_trimmed,
                                              MatchType.wikidata_trimmed):
                        continue
                if not m:
                    cache[(o, w)] = None
                    continue
                # if we had to trim both names and the OSM name is from the
                # operator tag it doesn't count
                if (m.match_type == MatchType.both_trimmed and
                        osm_key == 'operator'):
                    continue
                result = (m.match_type.name, w, source)
            if (result[0] == 'initials' and
                    intials_matches_other_wikidata_name(w, wikidata_names)):
                continue
            name[osm_key].append(result)
    if name:
        return dict(name)

    for w, source in wikidata_names.items():
        for osm_key, o in names.items():
            left_over = prefix_name_match(o, w)
            if not left_over:
                continue
            for second_w, second_source in wikidata_names.items():
                if second_w == w:
                    continue
                m = name_match(left_over, second_w, place_names=place_names)
                if not m:
                    continue
                name[osm_key].append(('prefix', w, source))
                break

    return dict(name)

def get_all_matches(osm_tags, wikidata_names, endings=None):
    names = get_names(osm_tags)

    matches = []
    for w, source in wikidata_names.items():
        for osm_key, o in names.items():
            m = name_match(o, w, endings)
            if m:
                m.wikidata_name = w
                m.wikidata_source = source
                m.osm_name = o
                m.osm_key = osm_key
                matches.append(m)

    # FIXME this code is broken
    # address_match = check_name_matches_address(osm_tags, wikidata_names)
    # if address_match:
    #     m = address_match
    #     m.wikidata_name = w
    #     m.wikidata_source = source
    #     m.osm_name = o
    #     m.osm_key = osm_key
    #     matches.append(m)
    return matches
