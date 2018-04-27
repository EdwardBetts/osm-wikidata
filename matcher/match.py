#!/usr/bin/python3
from collections import defaultdict
from unidecode import unidecode
from .utils import remove_start, normalize_url

import re

from enum import Enum

re_strip_non_chars = re.compile(r'[^-@\w]', re.U)
re_keep_commas = re.compile(r'[^@\w, ]', re.U)
re_number_start = re.compile('^(?:(?:Number|No)s?\.? )?(\d[-\d]*,? .*$)')
re_uk_postcode_start = re.compile('^[a-z][a-z]\d+[a-z]?$', re.I)

MatchType = Enum('Match', ['good', 'trim', 'address', 'initials', 'initials_trim'])

abbr = {
    'avenue': 'ave',
    'street': 'st',
    'road': 'rd',
    'boulevard': 'blvd',
    'drive': 'dr',
    'lane': 'ln',
    'square': 'sq',
    'north': 'n',
    'south': 's',
    'east': 'e',
    'west': 'w',
}
re_abbr = re.compile(r'\b(' + '|'.join(abbr.keys()) + r')\b', re.I)

bad_name_fields = {'tiger:name_base', 'name:right',
                   'name:left', 'gnis:county_name', 'openGeoDB:name'}

def no_alpha(s):
    return all(not c.isalpha() for c in s)

class Match(object):
    def __init__(self, match_type):
        self.match_type = match_type
        self.wikidata_name = None
        self.wikidata_source = None
        self.osm_name = None
        self.osm_key = None

def tidy_name(n):
    # expects to be passed a name in lowercase
    n = n.replace('saint ', 'st ')
    n = n.replace(' church of england ', ' ce ')
    n = n.replace(' cofe ', ' ce ')
    n = n.replace(' c of e ', ' ce ')
    n = n.replace(' roman catholic ', ' rc ')
    n = n.replace(' preparatory school', ' prep school')
    n = n.replace(' incorporated', ' inc')
    n = n.replace(' cooperative', ' coop')
    n = n.replace(' co-operative', ' coop')
    if len(n) > 1 and n[-1] == 's':
        n = n[:-1]
    if not n.lstrip().startswith('s '):
        n = n.replace('s ', ' ').replace("s' ", '')
    for word in ('the', 'and', 'at', 'of', 'de', 'di', 'le', 'la', 'les',
                 'von', 'pw.'):
        n = n.replace(' {} '.format(word), ' ')
    if n.startswith('the '):
        n = n[4:]
    n = n.replace('center', 'centre').replace('theater', 'theatre')

    decoded = unidecode(n).strip()
    if not any(c.isalnum() for c in decoded):
        return n.strip()
    return decoded

def initials_match(n1, n2, endings=None):
    n1_lc = n1.lower()
    initals = ''.join(term[0] for term in n2.split()).upper()
    if len(initals) < 3 or len(n1) < 3:
        return
    if initals == n1:
        return Match(MatchType.initials)
    if initals == ''.join(c for c in n1 if c.isalnum()):
        return Match(MatchType.initials)
    if any(initals == trim for trim in
            [n1[:-len(end)].strip() for end in endings or [] if n1_lc.endswith(end.lower())]):
        return Match(MatchType.initials_trim)

def match_with_words_removed(osm, wd, words):
    if not words:
        return False
    x_wd = re_strip_non_chars.sub('', wd)
    x_osm = re_strip_non_chars.sub('', osm)
    words = [re_strip_non_chars.sub('', w) for w in words]
    return any(x_wd.replace(word, '') == x_osm.replace(word, '')
               for word in words)

def strip_non_chars_match(osm, wd):
    wc_stripped = re_strip_non_chars.sub('', wd)
    osm_stripped = re_strip_non_chars.sub('', osm)
    return wc_stripped and osm_stripped and wc_stripped == osm_stripped

def name_match_main(osm, wd, endings=None, debug=False):
    if not wd or not osm:
        return

    if wd == osm:
        return Match(MatchType.good)

    wd_lc = wd.lower()
    osm_lc = osm.lower()

    m = initials_match(osm, wd, endings) or initials_match(wd, osm, endings)
    if m:
        return m

    if strip_non_chars_match(osm_lc, wd_lc):
        return Match(MatchType.good)

    if endings and match_with_words_removed(osm_lc, wd_lc, endings):
        return Match(MatchType.good)

    wd_lc = tidy_name(wd_lc)
    osm_lc = tidy_name(osm_lc)

    if not wd_lc or not osm_lc:
        return

    if endings and match_with_words_removed(osm_lc, wd_lc, [tidy_name(e) for e in endings]):
        return Match(MatchType.good)

    if wd_lc == osm_lc:
        return Match(MatchType.good)
    if 'washington, d' in wd_lc:  # special case for Washington, D.C.
        wd_lc = wd_lc.replace('washington, d', 'washington d')
    comma = wd_lc.rfind(', ')
    if comma != -1 and not osm_lc.isdigit():
        wc_part1 = wd_lc[:comma]
        if wc_part1 == osm_lc or strip_non_chars_match(osm_lc, wc_part1):
            return Match(MatchType.good)
    if wd_lc.split() == list(reversed(osm_lc.split())):
        return Match(MatchType.good)

    wd_lc = re_keep_commas.sub('', wd_lc)
    osm_lc = re_keep_commas.sub('', osm_lc)

    comma = wd_lc.rfind(', ')
    if comma != -1 and not osm_lc.isdigit():
        if wd_lc[:comma] == osm_lc:
            return Match(MatchType.good)
        if remove_start(wd_lc[:comma], 'the ') == remove_start(osm_lc, 'the '):
            return Match(MatchType.good)

    wd_lc = re_strip_non_chars.sub('', wd_lc)
    osm_lc = re_strip_non_chars.sub('', osm_lc)
    if wd_lc == osm_lc:
        return Match(MatchType.good)

    if wd_lc.startswith('the'):
        wd_lc = wd_lc[3:]
    if osm_lc.startswith('the'):
        osm_lc = osm_lc[3:]
    if wd_lc == osm_lc:
        return Match(MatchType.good)

    for end in ['building', 'complex'] + list(endings or []):
        if wd_lc.endswith(end) and wd_lc[:-len(end)] == osm_lc:
            return Match(MatchType.trim)
        if wd_lc.startswith(end) and wd_lc[len(end):] == osm_lc:
            return Match(MatchType.trim)
        if osm_lc.endswith(end) and osm_lc[:-len(end)] == wd_lc:
            return Match(MatchType.trim)
        if osm_lc.startswith(end) and osm_lc[len(end):] == wd_lc:
            return Match(MatchType.trim)
    return

def name_match(osm, wd, endings=None, debug=False):
    match = name_match_main(osm, wd, endings, debug)
    if match:
        return match

    for start in 'Tomb of ', 'Statue of ', 'Memorial to ':
        if wd.startswith(start) and name_match_main(osm, wd[len(start):], endings):
            return Match(MatchType.trim)

    end = ' And Attached Railings'.lower()
    if wd.lower().endswith(end) and name_match_main(osm, wd[:-len(end)], endings):
        return Match(MatchType.trim)

    if ';' not in osm:
        return
    for osm_name in osm.split(';'):
        match = name_match(osm_name.strip(), wd, endings=endings, debug=debug)
        if match:
            return match

def normalize_name(name):
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
    return False

def check_for_address_in_extract(osm_tags, extract):
    if not extract or not has_address(osm_tags):
        return

    def address_in_extract(address):
        address = re_abbr.sub(lambda m: '(' + m.group(1) + '|' + abbr[m.group(1).lower()] + ')', re.escape(address))
        # address = re_directions.sub(lambda m: '(' + m.group(1) + '|' + m.group(1)[0] + ')', address)

        print(address)

        return bool(re.search(r'\b' + address, extract, re.I))

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
    number_start = {m.group(1) for m in (re_number_start.match(name) for name in wikidata_names) if m}
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
        osm_address = normalize_name(osm_tags['addr:housenumber'] +
                                     osm_tags['addr:street'])
        if any(name == osm_address for name in norm_number_start):
            return True
        for i in number_start:
            name, _, postcode_start = i.rpartition(' ')

            if postcode and not postcode.startswith(postcode_start.lower()):
                continue

            if (re_uk_postcode_start.match(postcode_start) and
                    normalize_name(name) == osm_address):
                return True

    if 'addr:full' in osm_tags:
        osm_address = normalize_name(osm_tags['addr:full'])
        if any(osm_address.startswith(name) for name in norm_number_start):
            return True

        for i in number_start:
            name, _, postcode_start = i.rpartition(' ')

            if (re_uk_postcode_start.match(postcode_start) and
                    normalize_name(name) == osm_address):
                return True

    return False

def get_names(osm_tags):
    return {k: v for k, v in osm_tags.items()
             if ('name' in k and k not in bad_name_fields) or k == 'operator'}

def intials_matches_other_wikidata_name(initials, wikidata_names):
    return any(w != initials and initials_match(initials, w)
               for w in wikidata_names.keys())

def check_for_match(osm_tags, wikidata_names, endings=None):
    names = get_names(osm_tags)
    if not names or not wikidata_names:
        return {}

    if 'addr:city' in osm_tags:
        city = osm_tags['addr:city'].lower()
        if endings is None:
            endings = set()
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
                m = name_match(o, w, endings)
                if not m:
                    cache[(o, w)] = None
                    continue
                result = (m.match_type.name, w, source)
            if (result[0] == 'initials' and
                    intials_matches_other_wikidata_name(w, wikidata_names)):
                continue
            name[osm_key].append(result)

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
