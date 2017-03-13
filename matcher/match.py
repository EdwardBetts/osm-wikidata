#!/usr/bin/python3
from collections import defaultdict
from unidecode import unidecode

import re

from enum import Enum

entity_endings = defaultdict(dict)
re_strip_non_chars = re.compile(r'[^@\w]', re.U)

MatchType = Enum('Match', ['good', 'trim', 'address', 'initials', 'initials_trim'])

class Match(object):
    def __init__(self, match_type):
        self.match_type = match_type

def tidy_name(n):
    n = n.replace('saint ', 'st ')
    if len(n) > 1 and n[-1] == 's':
        n = n[:-1]
    if not n.startswith('s '):
        n = n.replace('s ', ' ').replace("s' ", '')
    for word in 'the', 'and', 'at', 'of', 'de', 'le', 'la', 'les':
        n = n.replace(' {} '.format(word), ' ')
    n = n.replace('center', 'centre').replace('theater', 'theatre')
    return unidecode(n).strip()

def initials_match(n1, n2, endings=None):
    n1_lc = n1.lower()
    initals = ''.join(term[0] for term in n2 if term[0].isupper())
    if len(initals) < 3 or len(n1) < 3:
        return
    if initals == n1:
        return Match(MatchType.initials)
    if any(initals == trim for trim in [n1[:-len(end)].strip()
           for end in endings or [] if n1_lc.endswith(end.lower())]):
        return Match(MatchType.initials_trim)

def name_match_main(osm, wd, endings=None):
    wd_lc = wd.lower()
    osm_lc = osm.lower()
    if not wd or not osm:
        return

    m = initials_match(osm, wd, endings) or initials_match(wd, osm, endings)
    if m:
        return m

    if re_strip_non_chars.sub('', wd_lc) == re_strip_non_chars.sub('', osm_lc):
        return Match(MatchType.good)
    wd_lc = tidy_name(wd_lc)
    osm_lc = tidy_name(osm_lc)
    if not wd_lc or not osm_lc:
        return
    if wd_lc == osm_lc:
        # print ('{} == {} was: {}'.format(wd_lc, osm_lc, osm))
        return Match(MatchType.good)
    if 'washington, d' in wd_lc:  # special case for Washington, D.C.
        wd_lc = wd_lc.replace('washington, d', 'washington d')
    comma = wd_lc.rfind(', ')
    if comma != -1 and wd_lc[:comma] == osm_lc:
        return Match(MatchType.good)
    if wd_lc.split() == list(reversed(osm_lc.split())):
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

    for end in ['building'] + list(endings or []):
        if wd_lc.endswith(end) and wd_lc[:-len(end)] == osm_lc:
            return Match(MatchType.trim)
        if wd_lc.startswith(end) and wd_lc[len(end):] == osm_lc:
            return Match(MatchType.trim)
        if osm_lc.endswith(end) and osm_lc[:-len(end)] == wd_lc:
            return Match(MatchType.trim)
        if osm_lc.startswith(end) and osm_lc[len(end):] == wd_lc:
            return Match(MatchType.trim)
    return

def name_match(osm, wd, endings=None):
    start = 'Statue of '
    match = name_match_main(osm, wd, endings)
    if match:
        return match
    if wd.startswith(start) and name_match_main(osm, wd[len(start):], endings):
        return Match(MatchType.trim)

def normalize_name(name):
    return re_strip_non_chars.sub('', name.lower())

def check_name_matches_address(osm_tags, wikidata):
    if not any('addr' + part in osm_tags for part in ('housenumber', 'full')):
        return
    # if 'addr:housenumber' not in osm_tags or 'addr:street' not in osm_tags:
    #     return
    number_start = {name for name in wikidata['names'] if name[0].isdigit()}
    if not number_start:
        return
    number_start.update(name[:name.rfind(',')] for name in set(number_start) if ',' in name)
    number_start = {normalize_name(name) for name in number_start}
    if 'addr:housenumber' in osm_tags and 'addr:street' in osm_tags:
        osm_address = normalize_name(osm_tags['addr:housenumber'] + osm_tags['addr:street'])
        if any(name == osm_address for name in number_start):
            return Match(MatchType.address)
    if 'addr:full' in osm_tags:
        osm_address = normalize_name(osm_tags['addr:full'])
        if any(name in osm_address for name in number_start):
            return Match(MatchType.address)

def get_wikidata_names(item):
    skip_lang = {'ar', 'arc', 'pl'}
    # print(len(item['sitelinks']), len(item['labels']))
    names = defaultdict(list)
    # only include aliases if there are less than 10 other names
    if len(item.get('sitelinks', {})) < 6 and len(item['labels']) < 6:
        for k, v in item.get('aliases', {}).items():
            if k in skip_lang:
                continue
            if len(v) > 3:
                continue
            for name in v:
                names[name].append(('alias', k))
    for k, v in item['labels'].items():
        if k in skip_lang:
            continue
        names[v].append(('label', k))
    for k, v in item.get('sitelinks', {}).items():
        if k + 'wiki' in skip_lang:
            continue
        names[v].append(('sitelink', k))
    return names

def check_for_match(osm_tags, wikidata):
    bad_name_fields = {'tiger:name_base', 'old_name', 'name:right',
                       'name:left', 'gnis:county_name', 'openGeoDB:name'}

    endings = set()
    names = {k: v for k, v in osm_tags.items()
             if 'name' in k and k not in bad_name_fields}
    for tag in osm_tags.items():
        for cat in wikidata['cats']:
            for t in tag, (tag[0], None):
                if t in entity_endings[cat]:
                    endings.update(entity_endings[cat][t])

    best = None
    for w, source in wikidata['names'].items():
        for o in names.values():
            m = name_match(o, w, endings)
            if m and m.match_type == MatchType.good:
                # print(source, '  match: {} == {}'.format(o, w))
                return m
            elif m and m.match_type == MatchType.trim:
                best = Match(MatchType.trim)

    address_match = check_name_matches_address(osm_tags, wikidata)
    return address_match or best

def get_osm_id_and_type(source_type, source_id):
    if source_type == 'point':
        return ('node', source_id)
    if source_id > 0:
        return ('way', source_id)
    return ('relation', -source_id)

def build_hstore_query(tags):
    tags = [tuple(tag.split('=')) if ('=' in tag) else (tag, None) for tag in tags]
    return ' or '.join("((tags->'{}') = '{}')".format(k, v) if v else "(tags ? '{}')".format(k) for k, v in tags)
