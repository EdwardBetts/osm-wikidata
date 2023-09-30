"""Functions for interacting with Wikidata."""

import json
import os
import re
import typing
from collections import defaultdict
from time import time
from urllib.parse import unquote

import requests
import requests.exceptions
from flask import render_template, render_template_string, request

from . import (
    Entity,
    commons,
    language,
    mail,
    match,
    matcher,
    overpass,
    user_agent_headers,
)
from .language import get_language_label
from .utils import cache_filename, drop_start
from .wikidata_api import QueryError, QueryTimeout, get_entities, get_entity

report_missing_values = False
wd_entity = "http://www.wikidata.org/entity/Q"
enwiki = "https://en.wikipedia.org/wiki/"
skip_tags = {
    "route:road",
    "route=road",
    "highway=primary",
    "highway=road",
    "highway=service",
    "highway=motorway",
    "highway=trunk",
    "highway=unclassified",
    "highway",
    "landuse" "name",
    "website",
    "addr:street",
    "type=associatedStreet",
    "type=waterway",
    "waterway=river",
}

edu = [
    "Tag:amenity=college",
    "Tag:amenity=university",
    "Tag:amenity=school",
    "Tag:office=educational_institution",
]
tall = ["Key:height", "Key:building:levels"]

extra_keys = {
    "Q3914": [
        "Tag:building=school",
        "Tag:building=college",
        "Tag:amenity=college",
        "Tag:office=educational_institution",
    ],  # school
    "Q322563": edu,  # vocational school
    "Q383092": edu,  # film school
    "Q1021290": edu,  # music school
    "Q1244442": edu,  # school building
    "Q1469420": edu,  # adult education centre
    "Q2143781": edu,  # drama school
    "Q2385804": edu,  # educational institution
    "Q5167149": edu,  # cooking school
    "Q7894959": edu,  # University Technical College
    "Q47530379": edu,  # agricultural college
    "Q11303": tall,  # skyscraper
    "Q18142": tall,  # high-rise building
    "Q33673393": tall,  # multi-storey building
    "Q641226": ["Tag:leisure=stadium"],  # arena
    "Q2301048": ["Tag:aeroway=helipad"],  # special airfield
    "Q622425": ["Tag:amenity=pub", "Tag:amenity=music_venue"],  # nightclub
    "Q187456": ["Tag:amenity=pub", "Tag:amenity=nightclub"],  # bar
    "Q16917": ["Tag:amenity=clinic", "Tag:building=clinic"],  # hospital
    "Q330284": ["Tag:amenity=market"],  # marketplace
    "Q5307737": ["Tag:amenity=pub", "Tag:amenity=bar"],  # drinking establishment
    "Q875157": ["Tag:tourism=resort"],  # resort
    "Q174782": [
        "Tag:leisure=park",
        "Tag:highway=pedestrian",
        "Tag:foot=yes",
        "Tag:area=yes",
        "Tag:amenity=market",
        "Tag:leisure=common",
    ],  # square
    "Q34627": ["Tag:religion=jewish"],  # synagogue
    "Q16970": ["Tag:religion=christian"],  # church
    "Q32815": ["Tag:religion=islam"],  # mosque
    "Q811979": ["Key:building"],  # architectural structure
    "Q11691": ["Key:building"],  # stock exchange
    "Q1329623": [
        "Tag:amenity=arts_centre",  # cultural centre
        "Tag:amenity=community_centre",
    ],
    "Q856584": ["Tag:amenity=library"],  # library building
    "Q11315": ["Tag:landuse=retail"],  # shopping mall
    "Q39658032": ["Tag:landuse=retail"],  # open air shopping centre
    "Q277760": ["Tag:historic=folly", "Tag:historic=city_gate"],  # gatehouse
    "Q180174": ["Tag:historic=folly"],  # folly
    "Q15243209": [
        "Tag:leisure=park",
        "Tag:boundary=national_park",
    ],  # historic district
    "Q3010369": ["Tag:historic=monument"],  # opening ceremony
    "Q123705": ["Tag:place=suburb"],  # neighbourhood
    "Q256020": ["Tag:amenity=pub"],  # inn
    "Q41253": ["Tag:amenity=theatre"],  # movie theater
    "Q17350442": ["Tag:amenity=theatre"],  # venue
    "Q156362": ["Tag:amenity=winery"],  # winery
    "Q14092": ["Tag:leisure=fitness_centre", "Tag:leisure=sports_centre"],  # gymnasium
    "Q27686": [
        "Tag:tourism=hostel",  # hotel
        "Tag:tourism=guest_house",
        "Tag:building=hotel",
    ],
    "Q11707": [
        "Tag:amenity=cafe",
        "Tag:amenity=fast_food",
        "Tag:shop=deli",
        "Tag:shop=bakery",
        "Key:cuisine",
    ],  # restaurant
    "Q2360219": ["Tag:amenity=embassy"],  # permanent mission
    "Q27995042": ["Tag:protection_title=Wilderness Area"],  # wilderness area
    "Q838948": ["Tag:historic=memorial", "Tag:historic=monument"],  # work of art
    "Q23413": ["Tag:place=locality"],  # castle
    "Q28045079": [
        "Tag:historic=archaeological_site",
        "Tag:site_type=fortification",
        "Tag:embankment=yes",
    ],  # contour fort
    "Q744099": [
        "Tag:historic=archaeological_site",
        "Tag:site_type=fortification",
        "Tag:embankment=yes",
    ],  # hillfort
    "Q515": ["Tag:border_type=city"],  # city
    "Q1254933": ["Tag:amenity=university"],  # astronomical observatory
    "Q1976594": ["Tag:landuse=industrial"],  # science park
    "Q190928": ["Tag:landuse=industrial"],  # shipyard
    "Q4663385": [
        "Tag:historic=train_station",  # former railway station
        "Tag:railway=historic_station",
    ],
    "Q11997323": ["Tag:emergency=lifeboat_station"],  # lifeboat station
    "Q16884952": [
        "Tag:castle_type=stately",
        "Tag:building=country_house",
    ],  # country house
    "Q1343246": [
        "Tag:castle_type=stately",
        "Tag:building=country_house",
    ],  # English country house
    "Q4919932": ["Tag:castle_type=stately"],  # stately home
    "Q1763828": ["Tag:amenity=community_centre"],  # multi-purpose hall
    "Q3469910": ["Tag:amenity=community_centre"],  # performing arts center
    "Q57660343": ["Tag:amenity=community_centre"],  # performing arts building
    "Q163740": [
        "Tag:amenity=community_centre",  # nonprofit organization
        "Tag:amenity=social_facility",
        "Key:social_facility",
    ],
    "Q41176": ["Key:building:levels"],  # building
    "Q44494": ["Tag:historic=mill"],  # mill
    "Q56822897": ["Tag:historic=mill"],  # mill building
    "Q2175765": ["Tag:public_transport=stop_area"],  # tram stop
    "Q179700": [
        "Tag:memorial=statue",  # statue
        "Tag:memorial:type=statue",
        "Tag:historic=memorial",
    ],
    "Q1076486": ["Tag:landuse=recreation_ground"],  # sports venue
    "Q988108": [
        "Tag:amenity=community_centre",  # club
        "Tag:community_centre=club_home",
    ],
    "Q55004558": ["Tag:service=yard", "Tag:landuse=railway"],  # car barn
    "Q19563580": ["Tag:landuse=railway"],  # rail yard
    "Q134447": ["Tag:generator:source=nuclear"],  # nuclear power plant
    "Q1258086": [
        "Tag:leisure=park",
        "Tag:boundary=national_park",
    ],  # National Historic Site
    "Q32350958": ["Tag:leisure=bingo"],  # Bingo hall
    "Q53060": ["Tag:historic=gate", "Tag:tourism=attraction"],  # gate
    "Q3947": [
        "Tag:tourism=hotel",  # house
        "Tag:building=hotel",
        "Tag:tourism=guest_house",
    ],
    "Q847017": ["Tag:leisure=sports_centre"],  # sports club
    "Q820477": ["Tag:landuse=quarry", "Tag:gnis:feature_type=Mine"],  # mine
    "Q77115": ["Tag:leisure=sports_centre"],  # community center
    "Q35535": ["Tag:amenity=police"],  # police
    "Q16560": ["Tag:tourism=attraction", "Tag:historic=yes"],  # palace
    "Q131734": ["Tag:amenity=pub", "Tag:industrial=brewery"],  # brewery
    "Q828909": [
        "Tag:landuse=commercial",
        "Tag:landuse=industrial",
        "Tag:historic=dockyard",
    ],  # wharf
    "Q10283556": ["Tag:landuse=railway"],  # motive power depot
    "Q18674739": ["Tag:leisure=stadium"],  # event venue
    "Q20672229": ["Tag:historic=archaeological_site"],  # friary
    "Q207694": ["Tag:museum=art"],  # art museum
    "Q22698": [
        "Tag:leisure=dog_park",
        "Tag:amenity=market",
        "Tag:place=square",
        "Tag:leisure=common",
    ],  # park
    "Q738570": ["Tag:place=suburb"],  # central business district
    "Q1133961": ["Tag:place=suburb"],  # commercial district
    "Q935277": ["Tag:gnis:ftype=Playa", "Tag:natural=sand"],  # salt pan
    "Q14253637": ["Tag:gnis:ftype=Playa", "Tag:natural=sand"],  # dry lake
    "Q63099748": [
        "Tag:tourism=hotel",  # hotel building
        "Tag:building=hotel",
        "Tag:tourism=guest_house",
    ],
    "Q2997369": [
        "Tag:leisure=park",
        "Tag:highway=pedestrian",
        "Tag:foot=yes",
        "Tag:area=yes",
        "Tag:amenity=market",
        "Tag:leisure=common",
    ],  # plaza
    "Q130003": [
        "Tag:landuse=winter_sports",  # ski resort
        "Tag:site=piste",
        "Tag:leisure=resort",
        "Tag:landuse=recreation_ground",
    ],
}

# search for items in bounding box that have an English Wikipedia article
wikidata_enwiki_query = """
SELECT ?place ?placeLabel (SAMPLE(?location) AS ?location) ?article WHERE {
    SERVICE wikibase:box {
        ?place wdt:P625 ?location .
        bd:serviceParam wikibase:cornerWest "Point({{ west }} {{ south }})"^^geo:wktLiteral .
        bd:serviceParam wikibase:cornerEast "Point({{ east }} {{ north }})"^^geo:wktLiteral .
    }
    ?article schema:about ?place .
    ?article schema:inLanguage "en" .
    ?article schema:isPartOf <https://en.wikipedia.org/> .
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q18340550 } .          # ignore timeline article
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q13406463 } .          # ignore list article
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q17362920 } .          # ignore Wikimedia duplicated page
    FILTER NOT EXISTS { ?place wdt:P31/wdt:P279* wd:Q192611 } .  # ignore constituency
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q811683 } .            # ignore proposed building or structure
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?place ?placeLabel ?article
"""

# search for items in bounding box that have an English Wikipedia article
# look for coordinates in the headquarters location (P159)
wikidata_enwiki_hq_query = """
SELECT ?place ?placeLabel (SAMPLE(?location) AS ?location) ?article WHERE {
    ?place p:P159 ?statement .
    SERVICE wikibase:box {
        ?statement pq:P625 ?location .
        bd:serviceParam wikibase:cornerWest "Point({{ west }} {{ south }})"^^geo:wktLiteral .
        bd:serviceParam wikibase:cornerEast "Point({{ east }} {{ north }})"^^geo:wktLiteral .
    }
    ?article schema:about ?place .
    ?article schema:inLanguage "en" .
    ?article schema:isPartOf <https://en.wikipedia.org/> .
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?place ?placeLabel ?article
"""

wikidata_point_query = """
SELECT ?place (SAMPLE(?location) AS ?location) ?article WHERE {
    SERVICE wikibase:around {
        ?place wdt:P625 ?location .
        bd:serviceParam wikibase:center "Point({{ lon }} {{ lat }})"^^geo:wktLiteral .
        bd:serviceParam wikibase:radius "{{ '{:.1f}'.format(radius) }}" .
    }
    ?article schema:about ?place .
    ?article schema:inLanguage "en" .
    ?article schema:isPartOf <https://en.wikipedia.org/> .
}
GROUP BY ?place ?article
"""

wikidata_subclass_osm_tags = """
SELECT DISTINCT ?item ?itemLabel ?tag
WHERE
{
  {
    wd:{{qid}} wdt:P31/wdt:P279* ?item .
    ?item ((p:P1282/ps:P1282)|wdt:P641/(p:P1282/ps:P1282)|wdt:P140/(p:P1282/ps:P1282)|wdt:P366/(p:P1282/ps:P1282)) ?tag .
  }
  UNION
  {
      wd:{{qid}} wdt:P1435 ?item .
      ?item (p:P1282/ps:P1282) ?tag
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}"""

# search for items in bounding box that have OSM tags in the subclass tree
wikidata_item_tags = """
SELECT ?place ?placeLabel (SAMPLE(?location) AS ?location) ?address ?street ?item ?itemLabel ?tag WHERE {
    SERVICE wikibase:box {
        ?place wdt:P625 ?location .
        bd:serviceParam wikibase:cornerWest "Point({{ west }} {{ south }})"^^geo:wktLiteral .
        bd:serviceParam wikibase:cornerEast "Point({{ east }} {{ north }})"^^geo:wktLiteral .
    }
    ?place wdt:P31/wdt:P279* ?item .
    ?item ((p:P1282/ps:P1282)|wdt:P641/(p:P1282/ps:P1282)|wdt:P140/(p:P1282/ps:P1282)|wdt:P366/(p:P1282/ps:P1282)) ?tag .
    OPTIONAL { ?place wdt:P969 ?address } .
    OPTIONAL { ?place wdt:P669 ?street } .
    FILTER NOT EXISTS { ?item wdt:P31 wd:Q18340550 } .           # ignore timeline article
    FILTER NOT EXISTS { ?item wdt:P31 wd:Q13406463 } .           # ignore list article
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q17362920 } .          # ignore Wikimedia duplicated page
    FILTER NOT EXISTS { ?place wdt:P31/wdt:P279* wd:Q192611 } .  # ignore constituency
    FILTER NOT EXISTS { ?place wdt:P31 wd:Q811683 } .            # ignore proposed building or structure
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?place ?placeLabel ?address ?street ?item ?itemLabel ?tag
"""

# search for items in bounding box that have OSM tags in the subclass tree
# look for coordinates in the headquarters location (P159)
wikidata_hq_item_tags = """
SELECT ?place ?placeLabel (SAMPLE(?location) AS ?location) ?address ?street ?item ?itemLabel ?tag WHERE {
    ?place p:P159 ?statement .
    SERVICE wikibase:box {
        ?statement pq:P625 ?location .
        bd:serviceParam wikibase:cornerWest "Point({{ west }} {{ south }})"^^geo:wktLiteral .
        bd:serviceParam wikibase:cornerEast "Point({{ east }} {{ north }})"^^geo:wktLiteral .
    }
    ?place wdt:P31/wdt:P279* ?item .
    ?item ((p:P1282/ps:P1282)|wdt:P641/(p:P1282/ps:P1282)|wdt:P140/(p:P1282/ps:P1282)|wdt:P366/(p:P1282/ps:P1282)) ?tag .
    OPTIONAL { ?place wdt:P969 ?address } .
    OPTIONAL { ?place wdt:P669 ?street } .
    FILTER NOT EXISTS { ?place wdt:P31/wdt:P279* wd:Q192611 } .     # ignore constituencies
    SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
GROUP BY ?place ?placeLabel ?address ?street ?item ?itemLabel ?tag
"""

# Q15893266 == former entity
# Q56061 == administrative territorial entity

next_level_query = """
SELECT DISTINCT ?item ?itemLabel ?itemDescription
                ?startLabel
                (SAMPLE(?pop) AS ?pop)
                (SAMPLE(?area) AS ?area)
                (GROUP_CONCAT(DISTINCT ?isa) as ?isa_list)
WHERE {
  VALUES ?start { wd:QID } .
  ?start wdt:P31/wdt:P279* ?subclass .
  ?subclass wdt:P150 ?nextlevel .
  ?item wdt:P131 ?start .
  ?item wdt:P31/wdt:P279* ?nextlevel .
  ?item wdt:P31/wdt:P279* wd:Q56061 .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q15893266 } .
  FILTER NOT EXISTS { ?item wdt:P576 ?end } .
  OPTIONAL { ?item wdt:P1082 ?pop } .
  OPTIONAL { ?item p:P2046/psn:P2046/wikibase:quantityAmount ?area } .
  OPTIONAL { ?item wdt:P31 ?isa } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "LANGUAGE" }
}
GROUP BY ?item ?itemLabel ?itemDescription ?startLabel
ORDER BY ?itemLabel
"""

next_level_query3 = """
SELECT DISTINCT ?item ?itemLabel ?itemDescription
                ?startLabel
                (SAMPLE(?pop) AS ?pop)
                (SAMPLE(?area) AS ?area)
                (GROUP_CONCAT(?isa) as ?isa_list)
WHERE {
  VALUES ?start { wd:QID } .
  VALUES (?item) { PLACES }
  OPTIONAL { ?item wdt:P1082 ?pop } .
  OPTIONAL { ?item p:P2046/psn:P2046/wikibase:quantityAmount ?area } .
  OPTIONAL { ?item wdt:P31 ?isa } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "LANGUAGE" }
}
GROUP BY ?item ?itemLabel ?itemDescription ?startLabel
ORDER BY ?itemLabel
"""

next_level_has_part_query = """
SELECT DISTINCT ?item ?itemLabel ?itemDescription
                ?startLabel
                (SAMPLE(?pop) AS ?pop)
                (SAMPLE(?area) AS ?area)
                (GROUP_CONCAT(DISTINCT ?isa) as ?isa_list)
WHERE {
  VALUES ?start { wd:QID } .
  ?start wdt:P527 ?item .
  ?item wdt:P31/wdt:P279* wd:Q56061 .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q15893266 } .
  FILTER NOT EXISTS { ?item wdt:P576 ?end } .
  OPTIONAL { ?item wdt:P1082 ?pop } .
  OPTIONAL { ?item p:P2046/psn:P2046/wikibase:quantityAmount ?area } .
  OPTIONAL { ?item wdt:P31 ?isa } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "LANGUAGE" }
}
GROUP BY ?item ?itemLabel ?itemDescription ?startLabel
ORDER BY ?itemLabel
"""

item_labels_query = """
SELECT ?item ?itemLabel
WHERE {
  VALUES ?item { ITEMS }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}"""

item_types = """
SELECT DISTINCT ?item ?type WHERE {
  VALUES ?item { ITEMS }
  {
      ?item wdt:P31/wdt:P279* ?type .
      ?type ((p:P1282/ps:P1282)|wdt:P641/(p:P1282/ps:P1282)|wdt:P140/(p:P1282/ps:P1282)|wdt:P366/(p:P1282/ps:P1282)) ?tag .
      FILTER(?tag != 'Key:amenity' && ?tag != 'Key:room' && ?tag != 'Key:man_made' && ?tag != 'Key:location')
  } UNION {
      ?item wdt:P31 ?type .
      VALUES (?type) { TYPES }
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
"""

item_types_tree = """
SELECT DISTINCT ?item ?itemLabel ?country ?countryLabel ?type ?typeLabel WHERE {
  {
    VALUES ?top { ITEMS }
    ?top wdt:P31/wdt:P279* ?item .
    ?item wdt:P279 ?type .
    ?type wdt:P279* ?subtype .
    ?subtype ((p:P1282/ps:P1282)|wdt:P641/(p:P1282/ps:P1282)|wdt:P140/(p:P1282/ps:P1282)|wdt:P366/(p:P1282/ps:P1282)) ?tag .
  } UNION {
    VALUES ?item { ITEMS }
    ?item wdt:P31 ?type .
  }
  OPTIONAL { ?item wdt:P17 ?country }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
"""

subclasses = """
SELECT DISTINCT ?item ?itemLabel ?type ?typeLabel WHERE {
  VALUES ?item { ITEMS }
  VALUES ?type { ITEMS }
  ?item wdt:P279* ?type .
  FILTER (?item != ?type)
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en" }
}
"""

# administrative territorial entity of a single country (Q15916867)

#           'Q349084'],    # England  -> district of England
admin_area_map = {
    "Q21": [
        "Q1136601",  # England  -> unitary authority of England
        "Q211690",  # |           London borough
        "Q1002812",  # |           metropolitan borough
        "Q643815",
    ],  # |           (non-)metropolitan county of England
    "Q22": ["Q15060255"],  # Scotland          -> council area
    "Q25": ["Q15979307"],  # Wales            -> principal area of Wales
    "Q26": ["Q17364572"],  # Northern Ireland -> district of Northern Ireland
}

next_level_query2 = """
SELECT DISTINCT ?item ?itemLabel
                ?startLabel
                (SAMPLE(?pop) AS ?pop)
                (SAMPLE(?area) AS ?area)
                (GROUP_CONCAT(?isa) as ?isa_list)
WHERE {
  VALUES ?start { wd:QID } .
  TYPES
  # metropolitan borough of the County of London (old)
  FILTER NOT EXISTS { ?item wdt:P31 wd:Q9046617 } .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q19953632 } .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q15893266 } .
  FILTER NOT EXISTS { ?item wdt:P576 ?end } .
  OPTIONAL { ?item wdt:P1082 ?pop } .
  OPTIONAL { ?item p:P2046/psn:P2046/wikibase:quantityAmount ?area } .
  OPTIONAL { ?item wdt:P31 ?isa } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "LANGUAGE" }
}
GROUP BY ?item ?itemLabel ?startLabel
ORDER BY ?itemLabel
"""

small_island_nations = {
    "Q672",  # Tuvalu
}

small_island_nations_query = """
SELECT DISTINCT ?item ?itemLabel
                ?startLabel
                (SAMPLE(?pop) AS ?pop)
                (SAMPLE(?area) AS ?area)
                (GROUP_CONCAT(?isa) as ?isa_list)
WHERE {
  VALUES ?start { wd:QID } .
  ?item wdt:P17 ?start .
  ?item wdt:P31/wdt:P279* wd:Q205895 .  # landform
  FILTER NOT EXISTS { ?item wdt:P576 ?end } .
  OPTIONAL { ?item wdt:P1082 ?pop } .
  OPTIONAL { ?item p:P2046/psn:P2046/wikibase:quantityAmount ?area } .
  OPTIONAL { ?item wdt:P31 ?isa } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "LANGUAGE" }
}
GROUP BY ?item ?itemLabel ?startLabel
ORDER BY ?itemLabel
"""

countries_in_continent_query = """
SELECT DISTINCT ?item
                ?itemLabel
                ?startLabel
                (SAMPLE(?pop) AS ?pop)
                (SAMPLE(?area) AS ?area)
                (GROUP_CONCAT(?isa) as ?isa_list)
WHERE {
  VALUES ?start { wd:QID } .
  VALUES (?region) {
    (wd:Q3624078)  # sovereign state
    (wd:Q161243)   # dependent territory
    (wd:Q179164)   # unitary state
    (wd:Q1763527)  # constituent country
    (wd:Q734818)   # condominium
    (wd:Q82794)    # geographic region
  }

  ?item wdt:P30 ?start .
  ?item p:P31 ?statement .
  ?statement ps:P31 ?region .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q15893266 } .
  FILTER NOT EXISTS { ?item wdt:P576 ?end } .
  OPTIONAL { ?item wdt:P1082 ?pop } .
  OPTIONAL { ?item p:P2046/psn:P2046/wikibase:quantityAmount ?area } .
  OPTIONAL { ?item wdt:P31 ?isa } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "LANGUAGE" }
}
GROUP BY ?item ?itemLabel ?startLabel
ORDER BY ?itemLabel
"""

# walk place hierarchy grabbing labels and country names
located_in_query = """
SELECT ?item ?itemLabel ?country ?countryLabel WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  VALUES ?start { wd:QID } .
  ?start wdt:P131* ?item .
  OPTIONAL { ?item wdt:P17 ?country.}
}
"""

up_one_level_query = """
SELECT ?startLabel ?itemLabel ?country1 ?country1Label ?country2 ?country2Label ?isa WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  VALUES ?start { wd:QID } .
  OPTIONAL { ?start wdt:P17 ?country1 }
  OPTIONAL {
    ?start wdt:P131 ?item .
    ?item wdt:P31 ?isa .
  }
  OPTIONAL { ?item wdt:P17 ?country2 }
}
"""

next_level_type_map = {
    "Q48091": [  # region of England
        "Q1136601",  # unitary authority of England
        "Q211690",  # London borough
        "Q1002812",  # metropolitan borough
        "Q643815",  # (non-)metropolitan county of England
        "Q180673",
    ],  # ceremonial county of England
    "Q1136601": [  # unitary authority area in England
        "Q1115575",  # civil parish
        "Q1195098",  # ward
        "Q589282",  # ward or electoral division of the United Kingdom
    ],
    "Q1187580": ["Q1115575"],  # civil parish
}

next_level_by_type = """
SELECT DISTINCT ?item ?itemLabel
                ?startLabel
                (SAMPLE(?pop) AS ?pop)
                (SAMPLE(?area) AS ?area)
                (GROUP_CONCAT(?isa) as ?isa_list)
WHERE {
  VALUES ?start { wd:QID } .
  TYPES
  ?item wdt:P131 ?start .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q19953632 } .
  FILTER NOT EXISTS { ?item wdt:P31/wdt:P279* wd:Q15893266 } .
  FILTER NOT EXISTS { ?item wdt:P576 ?end } .
  OPTIONAL { ?item wdt:P1082 ?pop } .
  OPTIONAL { ?item p:P2046/psn:P2046/wikibase:quantityAmount ?area } .
  OPTIONAL { ?item wdt:P31 ?isa } .
  SERVICE wikibase:label { bd:serviceParam wikibase:language "LANGUAGE" }
}
GROUP BY ?item ?itemLabel ?startLabel
ORDER BY ?itemLabel
"""

instance_of_query = """
SELECT DISTINCT ?item ?itemLabel ?countryLabel (SAMPLE(?location) AS ?location) WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en" }
  ?item wdt:P31/wdt:P279* wd:QID .
  OPTIONAL { ?item wdt:P17 ?country }
  OPTIONAL { ?item wdt:P625 ?location }
}
GROUP BY ?item ?itemLabel ?countryLabel
"""

continents_with_country_count_query = """
SELECT ?continent
       ?continentLabel
       ?continentDescription
       ?banner
       (COUNT(?country) AS ?count)
WHERE {
  SERVICE wikibase:label { bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }
  ?country wdt:P30 ?continent .
  ?country wdt:P31 wd:Q6256 .
  ?continent wdt:P948 ?banner
}
GROUP BY ?continent ?continentLabel ?continentDescription ?banner
ORDER BY ?continentLabel
"""

wikidata_query_api_url = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"


class Row(typing.TypedDict):
    """Wikidata query result row."""

    population: int | None
    area: int | None
    label: str
    description: str | None
    start: str
    item_id: int
    qid: str
    isa: list[str]


def get_query(q: str, south: float, north: float, west: float, east: float) -> str:
    """Pass coordinates to bounding box query."""
    return render_template_string(q, south=south, north=north, west=west, east=east)


def query_map(prefix: str, **kwargs) -> dict[str, str]:
    if kwargs.get("want_isa"):
        queries = ("item_tag", "hq_item_tag")
    else:
        queries = ("enwiki", "hq_enwiki", "item_tag", "hq_item_tag")

    return {
        name: render_template(f"wikidata_query/{prefix}_{name}.sparql", **kwargs)
        for name in queries
    }


def bbox_query_map(
    south: float, north: float, west: float, east: float, **kwargs
) -> str:
    return query_map("bbox", south=south, north=north, west=west, east=east, **kwargs)


def point_query_map(lat, lon, radius_m):
    return query_map("point", lat=lat, lon=lon, radius=radius_m / 1_000)


def get_enwiki_query(*args):
    return get_query(wikidata_enwiki_query, *args)


def get_enwiki_hq_query(*args):
    return get_query(wikidata_enwiki_hq_query, *args)


def get_item_tag_query(*args):
    return get_query(wikidata_item_tags, *args)


def get_hq_item_tag_query(*args):
    return get_query(wikidata_hq_item_tags, *args)


def get_point_query(lat, lon, radius):
    return render_template_string(
        wikidata_point_query, lat=lat, lon=lon, radius=float(radius) / 1000.0
    )


def run_query(
    query: str,
    name: str | None = None,
    return_json: bool = True,
    timeout: int = None,
    send_error_mail: bool = False,
) -> requests.models.Response | list[typing.Any]:
    attempts = 5

    def error_mail(subject, r):
        if send_error_mail:
            mail.error_mail("wikidata query error", query, r)

    if name:
        filename = cache_filename(name + ".json")
        if os.path.exists(filename):
            return json.load(open(filename))["results"]["bindings"]

    for attempt in range(attempts):
        try:  # retry if we get a ChunkedEncodingError
            r = requests.post(
                wikidata_query_api_url,
                data={"query": query, "format": "json"},
                timeout=timeout,
                headers=user_agent_headers(),
            )
            if r.status_code != 200:
                break
            if name:
                open(filename, "wb").write(r.content)
            if return_json:
                return r.json()["results"]["bindings"]
            else:
                return r
        except requests.exceptions.ChunkedEncodingError:
            if attempt == attempts - 1:
                error_mail("wikidata query error", r)
                raise QueryError(query, r)

    # query timeout generates two different exceptions
    # java.lang.RuntimeException: java.util.concurrent.ExecutionException: com.bigdata.bop.engine.QueryTimeoutException: Query deadline is expired.
    # java.util.concurrent.TimeoutException
    if (
        "Query deadline is expired." in r.text
        or "java.util.concurrent.TimeoutException" in r.text
    ):
        error_mail("wikidata query timeout", r)
        raise QueryTimeout(query, r)

    error_mail("wikidata query error", r)
    raise QueryError(query, r)


def flatten_criteria(items):
    start = {"Tag:" + i[4:] + "=" for i in items if i.startswith("Key:")}
    return {i for i in items if not any(i.startswith(s) for s in start)}


def wd_uri_to_id(value: str) -> int:
    """Given a Wikidata URI return the item ID."""
    return int(drop_start(value, wd_entity))


def wd_to_qid(wd: dict[str, str]) -> str | None:
    """Extract QID from dict return by Wikidata API."""
    # expecting {'type': 'url', 'value': 'https://www.wikidata.org/wiki/Q30'}
    return wd_uri_to_qid(wd["value"]) if wd["type"] == "uri" else None


def wd_uri_to_qid(value: str) -> str:
    """Given a Wikidata URI return the item QID."""
    assert value.startswith(wd_entity)
    return value[len(wd_entity) - 1 :]


def enwiki_url_to_title(url: str) -> str:
    """Convert from English Wikipedia URL to article title."""
    return unquote(drop_start(url, enwiki)).replace("_", " ")


def parse_enwiki_query(rows):
    return {
        wd_to_qid(row["place"]): {
            "query_label": row["placeLabel"]["value"],
            "enwiki": enwiki_url_to_title(row["article"]["value"]),
            "location": row["location"]["value"],
            "tags": set(),
        }
        for row in rows
    }


def drop_tag_prefix(v: str) -> str | None:
    """Remove 'Tag:' or 'Key:' from the start of a string."""
    if v.startswith("Key:") and "=" not in v:
        return v[4:]
    if v.startswith("Tag:") and "=" in v:
        return v[4:]


def parse_item_tag_query(rows, items):
    for row in rows:
        tag_or_key = drop_tag_prefix(row["tag"]["value"])
        if not tag_or_key or tag_or_key in skip_tags:
            continue
        qid = wd_to_qid(row["place"])
        if not qid:
            continue

        if qid not in items:
            items[qid] = {
                "query_label": row["placeLabel"]["value"],
                "location": row["location"]["value"],
                "tags": set(),
            }
            for k in "address", "street":
                if k in row:
                    items[qid][k] = row[k]["value"]
        items[qid]["tags"].add(tag_or_key)


def page_banner_from_entity(entity: Entity, **kwargs):
    property_key = "P948"
    if property_key not in entity["claims"]:
        return

    filename = entity["claims"][property_key][0]["mainsnak"]["datavalue"]["value"]

    try:
        images = commons.image_detail([filename], **kwargs)
        return images[filename]
    except Exception:
        return


def entity_label(entity: Entity, language=None) -> str:
    """Get a label from the entity."""
    if language and language in entity["labels"]:
        return entity["labels"][language]["value"]
    if "en" in entity["labels"]:
        return entity["labels"]["en"]["value"]

    # pick a label at random
    return list(entity["labels"].values())[0]["value"]


def entity_description(entity: Entity, language: str | None = None) -> str | None:
    """Get description from entity with given language with fallback to English."""
    if language and language in entity["descriptions"]:
        return entity["descriptions"][language]["value"]
    if "en" in entity["descriptions"]:
        return entity["descriptions"]["en"]["value"]


def names_from_entity(entity: Entity, skip_lang=None):
    if not entity or "labels" not in entity:
        return
    if skip_lang is None:
        skip_lang = set()
    if not entity:
        return

    ret = defaultdict(list)
    cat_start = "Category:"

    for k, v in entity["labels"].items():
        if k in skip_lang:
            continue
        ret[v["value"]].append(("label", k))

    for k, v in entity["sitelinks"].items():
        if k + "wiki" in skip_lang:
            continue
        title = v["title"]
        if title.startswith(cat_start):
            title = title[len(cat_start) :]

        first_letter = title[0]
        if first_letter.isupper():
            lc_first_title = first_letter.lower() + title[1:]
            if lc_first_title in ret:
                title = lc_first_title

        ret[title].append(("sitelink", k))

    for lang, value_list in entity.get("aliases", {}).items():
        if lang in skip_lang or len(value_list) > 3:
            continue
        for name in value_list:
            ret[name["value"]].append(("alias", lang))

    commonscats = entity.get("claims", {}).get("P373", [])
    for i in commonscats:
        if "datavalue" not in i["mainsnak"]:
            if report_missing_values:
                mail.datavalue_missing("commons category", entity)
            continue
        value = i["mainsnak"]["datavalue"]["value"]
        ret[value].append(("commonscat", None))

    officialname = entity.get("claims", {}).get("P1448", [])
    for i in officialname:
        if "datavalue" not in i["mainsnak"]:
            if report_missing_values:
                mail.datavalue_missing("official name", entity)
            continue
        value = i["mainsnak"]["datavalue"]["value"]
        ret[value["text"]].append(("officialname", value["language"]))

    nativelabel = entity.get("claims", {}).get("P1705", [])
    for i in nativelabel:
        if "datavalue" not in i["mainsnak"]:
            if report_missing_values:
                mail.datavalue_missing("native label", entity)
            continue
        value = i["mainsnak"]["datavalue"]["value"]
        ret[value["text"]].append(("nativelabel", value["language"]))

    image = entity.get("claims", {}).get("P18", [])
    for i in image:
        if "datavalue" not in i["mainsnak"]:
            if report_missing_values:
                mail.datavalue_missing("image", entity)
            continue
        value = i["mainsnak"]["datavalue"]["value"]
        m = re.search(r"\.[a-z]{3,4}$", value)
        if m:
            value = value[: m.start()]
        for pattern in r" - geograph\.org\.uk - \d+$", r"[, -]*0\d{2,}$":
            m = re.search(pattern, value)
            if m:
                value = value[: m.start()]
                break
        ret[value].append(("image", None))

    return ret


def parse_osm_keys(rows):
    start = "http://www.wikidata.org/entity/"
    items = {}
    for row in rows:
        uri = row["item"]["value"]
        qid = drop_start(uri, start)
        tag = row["tag"]["value"]
        for i in "Key:", "Tag:":
            if tag.startswith(i):
                tag = tag[4:]

        # Ignore some overly generic tags from Wikidata objects:
        # facility (Q13226383)            - osm tag: amenity
        # geographic location (Q2221906)  - osm tag: location
        # artificial entity (Q16686448)   - osm tag: man_made

        if tag in {"amenity", "location", "man_made"}:
            continue
        if qid not in items:
            items[qid] = {
                "uri": uri,
                "label": row["itemLabel"]["value"],
                "tags": set(),
            }
        items[qid]["tags"].add(tag)
    return items


def get_location_hierarchy(qid, name=None):
    # not currently in use
    query = located_in_query.replace("QID", qid)
    return [
        {
            "qid": wd_to_qid(row["item"]),
            "label": row["itemLabel"]["value"],
            "country": row["countryLabel"]["value"],
        }
        for row in run_query(query, name=name)
    ]


def up_one_level(qid: str, name: str | None = None) -> dict[str, str | None] | None:
    query = up_one_level_query.replace("QID", qid)
    try:
        rows = run_query(query, name=name, timeout=2)
    except requests.Timeout:
        return None

    if not rows:
        return None

    skip = {
        "Q180673",  # ceremonial county of England
        "Q1138494",  # historic county of England
    }

    ignore_up = any(wd_to_qid(row["isa"]) in skip for row in rows)

    row = rows[0]

    c1 = "country1" in row
    c2 = "country2" in row

    return {
        "name": row["startLabel"]["value"],
        "up": row["itemLabel"]["value"] if not ignore_up else None,
        "country_qid": wd_to_qid(row["country1"]) if c1 else None,
        "country_name": row["country1Label"]["value"] if c1 else None,
        "up_country_qid": wd_to_qid(row["country2"]) if c2 else None,
        "up_country_name": row["country2Label"]["value"] if c2 else None,
    }


def next_level_types(types: list[str]) -> str:
    """Wikidata Query SPARQL fragment to filter on item type."""
    types = list(types)
    if len(types) == 1:
        return "?item wdt:P31/wdt:P279* wd:{} .".format(types[0])
    return " union ".join("{ ?item wdt:P31/wdt:P279* wd:" + t + " }" for t in types)


def isa_list(types):
    types = list(types)
    if len(types) == 1:
        return "?item wdt:P31 wd:{} .".format(types[0])
    return " union ".join("{ ?item wdt:P31 wd:" + t + " }" for t in types)


def get_next_level_query(
    qid: str, entity: Entity, language: str = "en", name: str | None = None
) -> str:
    """Return text of SPARQL query for next admin level."""
    claims = entity.get("claims", {})

    isa = {i["mainsnak"]["datavalue"]["value"]["id"] for i in claims.get("P31", [])}

    isa_continent = {
        "Q5107",  # continent
        "Q855697",  # subcontinent
    }

    types_from_isa = isa & next_level_type_map.keys()

    if types_from_isa:
        # use first match in type map
        type_list = next_level_type_map[list(types_from_isa)[0]]
        type_values = " ".join(f"wd:{type_qid}" for type_qid in type_list)
        types = "VALUES ?type {" + type_values + "} .\n?item wdt:P31 ?type .\n"
        query = next_level_by_type.replace("TYPES", types)
    elif isa & isa_continent:
        query = countries_in_continent_query
    elif qid in small_island_nations:
        query = small_island_nations_query
    elif qid in admin_area_map:
        types = next_level_types(admin_area_map[qid])
        query = next_level_query2.replace("TYPES", types)
    elif "P150" in claims:  # P150 = contains administrative territorial entity
        places = [i["mainsnak"]["datavalue"]["value"]["id"] for i in claims["P150"]]
        print(places)
        query_places = " ".join(f"(wd:{qid})" for qid in places)
        query = next_level_query3.replace("PLACES", query_places)
    elif "Q82794" in isa and "P527" in claims:
        places = [i["mainsnak"]["datavalue"]["value"]["id"] for i in claims["P527"]]
        query_places = " ".join(f"(wd:{qid})" for qid in places)
        query = next_level_query3.replace("PLACES", query_places)
    else:
        query = next_level_query

    return query.replace("QID", qid).replace("LANGUAGE", language)


def run_next_level_query(query: str, name: str | None) -> requests.Response:
    """Call the Wikidata Query Service and mail admin for queries that take too long."""
    t0 = time()
    r = run_query(query, name=name, return_json=False, send_error_mail=True)
    query_time = time() - t0
    if query_time > 2:
        subject = f"next level places query took {query_time:.1f}"
        body = f"{request.url}\n\n{query}"
        mail.send_mail(subject, body)

    return r


def get_isa_list_from_row(row: dict[str, dict[str, str]]) -> list[str]:
    """WDQS rows that contain IsA URIs in the isa_list field."""
    isa_list = []
    for url in row["isa_list"]["value"].split(" "):
        if not url:
            continue
        isa_qid = wd_uri_to_qid(url)
        if isa_qid not in isa_list:
            isa_list.append(isa_qid)
    return isa_list


def get_population_from_row(row: dict[str, dict[str, str]]) -> int | None:
    """Read population from WDQS row."""
    pop = row.get("pop")
    # https://www.wikidata.org/wiki/Q896427 has 'unknown value' for population
    if pop:
        try:
            pop_value = int(pop["value"])
        except ValueError:
            pop_value = None
    else:
        pop_value = None

    return pop_value


def process_next_level_places(
    query_rows: list[dict[str, dict[str, typing.Any]]]
) -> list[Row]:
    """Process a list of rows from the WDQS into our own format."""
    rows = []
    for row in query_rows:
        item_id = wd_uri_to_id(row["item"]["value"])
        i: Row = {
            "population": get_population_from_row(row),
            "area": (
                int(float(row["area"]["value"]) / 1e6) if row.get("area") else None
            ),
            "label": row["itemLabel"]["value"],
            "description": (
                row["itemDescription"]["value"] if "itemDescription" in row else None
            ),
            "start": row["startLabel"]["value"],
            "item_id": item_id,
            "qid": "Q{:d}".format(item_id),
            "isa": get_isa_list_from_row(row),
        }
        rows.append(i)

    return rows


def next_level_places(
    qid: str,
    entity: Entity,
    language: str | None = None,
    query: str | None = None,
    name: str | None = None,
) -> list[Row]:
    """Look up the next level places for the given QID."""
    if not query:
        query = get_next_level_query(qid, entity, language=language)

    r = run_next_level_query(query, name)

    query_rows = r.json()["results"]["bindings"]
    if any("isa_list" not in row for row in query_rows):
        mail.error_mail("wikidata browse query error", query, r)
        raise QueryError(query, r)

    if not query_rows and "P527" in entity["claims"]:
        query = next_level_has_part_query.replace("QID", qid).replace(
            "LANGUAGE", language
        )
        r = run_query(query, name=name, return_json=False, send_error_mail=True)
        query_rows = r.json()["results"]["bindings"]

    if not query_rows:
        claims = entity.get("claims", {})
        # Taiwan Province, People's Republic of China (Q57251)
        # [no value] for located in the administrative territorial entity (P131)
        located_in = {
            i["mainsnak"]["datavalue"]["value"]["id"]
            for i in claims.get("P131", [])
            if "datavalue" in i["mainsnak"]
        }

        for located_in_qid in located_in:
            located_in_entity = get_entity(located_in_qid)
            query = get_next_level_query(
                located_in_qid, located_in_entity, language=language
            )
            r = run_query(query, return_json=False, send_error_mail=True)
            located_in_rows = r.json()["results"]["bindings"]
            query_rows += located_in_rows

    return process_next_level_places(query_rows)


def query_for_items(query: str, items: list[str]) -> str:
    """Fill in template with QIDs."""
    assert items
    query_items = " ".join(f"wd:{qid}" for qid in items)
    return query.replace("ITEMS", query_items)


def get_item_labels(items: list[str]) -> list[dict[str, typing.Any]]:
    """Run query to get labels for given items."""
    query = query_for_items(item_labels_query, items)
    rows = []
    for row in run_query(query):
        item_id = wd_uri_to_id(row["item"]["value"])
        qid = "Q{:d}".format(item_id)

        i = {
            "label": row["itemLabel"]["value"],
            "item_id": item_id,
            "qid": qid,
        }
        rows.append(i)
    return rows


def row_qid_and_label(row: dict[str, typing.Any], name: str) -> str | None:
    qid = wd_to_qid(row[name])
    return {"qid": qid, "label": row[name + "Label"]["value"]} if qid else None


def get_isa(items, name=None):
    graph = item_types_graph(items, name=name)

    ret = {}
    for qid in items:
        if qid not in graph:
            continue
        visited, queue = set(), [qid]
        result = []
        while queue:
            vertex = queue.pop(0)
            if vertex in visited:
                continue
            if vertex != qid:
                result.append(graph[vertex])
            visited.add(vertex)
            if (vertex == qid or "country" in graph[vertex]) and "children" in graph[
                vertex
            ]:
                queue.extend(graph[vertex]["children"] - visited)

        drop = set()
        for i in result[:]:
            if not (
                len(i.get("children", [])) == 1
                and "country" in i
                and any(c.isupper() for c in i["label"])
            ):
                continue
            child = graph[list(i["children"])[0]]["label"]
            if i["label"].startswith(child):
                drop.add(i["qid"])
            else:
                i["label"] += f" ({child})"

        result = [i for i in result if i["qid"] not in drop]

        all_children = set()
        for i in result:
            if "children" in i:
                all_children.update(i.pop("children"))
            if "country" in i:
                del i["country"]
        ret[qid] = [i for i in result if i["qid"] not in all_children]
    return ret


def item_types_graph(items, name=None, rows=None):
    if rows is None:
        query = query_for_items(item_types_tree, items)
        rows = run_query(query, name=name, send_error_mail=False)
    graph = {}
    for row in rows:
        item_qid = wd_to_qid(row["item"])
        type_qid = wd_to_qid(row["type"])
        if not item_qid or not type_qid:
            continue
        if type_qid not in graph:
            graph[type_qid] = {
                "qid": type_qid,
                "label": row["typeLabel"]["value"],
                "children": set(),
            }
        if item_qid not in graph:
            graph[item_qid] = {
                "qid": item_qid,
                "label": row["itemLabel"]["value"],
                "children": set(),
            }
        if "country" in row and "country" not in graph[item_qid]:
            country = row_qid_and_label(row, "country")
            if country:
                graph[item_qid]["country"] = country

        graph[item_qid]["children"].add(type_qid)

    return graph


def find_superclasses(items, name=None):
    query = query_for_items(subclasses, items)
    return {
        (wd_to_qid(row["item"]), wd_to_qid(row["type"]))
        for row in run_query(query, name=name)
    }


def claim_value(claim):
    try:
        return claim["mainsnak"]["datavalue"]["value"]
    except KeyError:
        pass


def country_iso_codes_from_qid(qid):
    item = WikidataItem.retrieve_item(qid)
    extra = {"Q159583": "VA"}  # Holy See
    no_iso_3166_code = {
        "Q23427",  # South Ossetia
        "Q3315371",  # Global Affairs Canada
        "Q170355",  # Indigenous Australians
        "Q6605",  # Sakha Republic
        "Q53492009",  # Embassy of the United States, Jerusalem
    }

    # Embassy of Canada, Washington, D.C. (Q137245) has two values in the
    # operator (P137) property: Canada (Q16) and Global Affairs Canada (Q3315371)
    # We ignore the second one

    # Aboriginal Tent Embassy (Q189212) has the operator (P137) property as
    # Indigenous Australians (Q170355)

    # Tel Aviv Branch Office of the Embassy of the United States (Q53444085)
    # operator (P137): Embassy of the United States, Jerusalem (Q53492009)

    if qid in no_iso_3166_code:
        return

    for wikidata_property in ("P297", "P298"):
        if qid in extra or item.claims.get(wikidata_property):
            continue
        body = "https://www.wikidata.org/wiki/" + qid
        mail.send_mail(f"{qid}: {wikidata_property} is missing", body)

    codes = [claim_value(c) for c in item.claims.get("P297") or []]
    codes += [claim_value(c) for c in item.claims.get("P298") or []]
    if qid in extra:
        codes.append(extra[qid])
    return [i for i in codes if i is not None]


class WikidataItem:
    def __init__(self, qid, entity):
        assert entity
        self.qid = qid
        self.entity = entity

    @classmethod
    def retrieve_item(cls, qid):
        entity = get_entity(qid)
        if not entity:
            return
        item = cls(qid, entity)
        return item

    @property
    def claims(self):
        return self.entity["claims"]

    @property
    def labels(self):
        return self.entity.get("labels", {})

    @property
    def aliases(self):
        return self.entity.get("aliases", {})

    @property
    def sitelinks(self):
        return self.entity.get("sitelinks", {})

    def get_sitelinks(self):
        """List of sitelinks with language names in English."""
        sitelinks = []
        for key, value in self.sitelinks.items():
            if len(key) != 6 or not key.endswith("wiki"):
                continue
            lang = key[:2]
            url = "https://{}.wikipedia.org/wiki/{}".format(
                lang, value["title"].replace(" ", "_")
            )
            sitelinks.append(
                {
                    "code": lang,
                    "lang": get_language_label(lang),
                    "url": url,
                    "title": value["title"],
                }
            )

        sitelinks.sort(key=lambda i: i["lang"])
        return sitelinks

    def remove_badges(self):
        if "sitelinks" not in self.entity:
            return
        for v in self.entity["sitelinks"].values():
            if "badges" in v:
                del v["badges"]

    def first_claim_value(self, key):
        return claim_value(self.claims[key][0])

    @property
    def has_coords(self):
        try:
            self.first_claim_value("P625")
        except (IndexError, KeyError):
            return False
        return True

    @property
    def has_earth_coords(self):
        earth = "http://www.wikidata.org/entity/Q2"
        return self.has_coords and self.first_claim_value("P625")["globe"] == earth

    @property
    def coords(self):
        if not self.has_coords:
            return None, None
        c = self.first_claim_value("P625")
        return c["latitude"], c["longitude"]

    @property
    def nrhp(self):
        try:
            nrhp = self.first_claim_value("P649")
        except (IndexError, KeyError):
            return
        if nrhp.isdigit():
            return nrhp

    def get_oql(self, criteria, radius):
        nrhp = self.nrhp
        if not criteria:
            return
        lat, lon = self.coords
        if lat is None or lon is None:
            return

        osm_filter = "around:{},{:.5f},{:.5f}".format(radius, lat, lon)

        union = []
        for tag_or_key in sorted(criteria):
            union += overpass.oql_from_wikidata_tag_or_key(tag_or_key, osm_filter)

        if nrhp:
            union += [
                '\n    {}({})["ref:nrhp"={}];'.format(t, osm_filter, nrhp)
                for t in ("node", "way", "rel")
            ]

        # FIXME extend oql to also check is_in
        # like this:
        #
        # is_in(48.856089,2.29789);
        # area._[admin_level];
        # out tags;

        oql = ("[timeout:300][out:json];\n" + "({}\n);\n" + "out center tags;").format(
            "".join(union)
        )

        return oql

    def trim_location_from_names(self, wikidata_names):
        if "P131" not in self.entity["claims"]:
            return

        location_names = set()
        located_in = [
            i["mainsnak"]["datavalue"]["value"]["id"]
            for i in self.entity["claims"]["P131"]
            if "datavalue" in i["mainsnak"]
        ]

        # Parc naturel régional des marais du Cotentin et du Bessin (Q2138341)
        # is in more than 50 locations. The maximum entities in one request is 50.
        if len(located_in) > 50:
            return
        for location in get_entities(located_in):
            if "labels" not in location:
                continue
            location_names |= {
                v["value"]
                for v in location["labels"].values()
                if v["value"] not in wikidata_names
            }

        for name_key, name_values in list(wikidata_names.items()):
            for n in location_names:
                new = None
                if name_key.startswith(n + " "):
                    new = name_key[len(n) + 1 :]
                elif name_key.endswith(", " + n):
                    new = name_key[: -(len(n) + 2)]
                if new and new not in wikidata_names:
                    wikidata_names[new] = name_values

    def osm_key_query(self):
        return render_template_string(wikidata_subclass_osm_tags, qid=self.qid)

    @property
    def osm_keys(self):
        if hasattr(self, "_osm_keys"):
            return self._osm_keys
        self._osm_keys = run_query(self.osm_key_query())
        return self._osm_keys

    def languages_from_country(self):
        langs = []
        for country in self.claims.get("P17", []):
            c = claim_value(country)
            if not c:
                continue
            for l in language.get_country_lanaguage(c["numeric-id"]):
                if l not in langs:
                    langs.append(l)
        return langs

    def query_language_from_country(self):
        if hasattr(self, "_language_codes"):
            return self._language_codes
        query = """
SELECT DISTINCT ?code WHERE {
  wd:QID wdt:P17 ?country .
  ?country wdt:P37 ?lang .
  ?lang wdt:P424 ?code .
}""".replace(
            "QID", self.qid
        )
        rows = run_query(query)
        self._language_codes = [row["code"]["value"] for row in rows]
        return self._language_codes

    def label(self, lang=None):
        labels = self.labels
        sitelinks = [i[:-4] for i in self.sitelinks.keys() if i.endswith("wiki")]
        if not labels:
            return
        if lang and lang in labels:  # requested language
            return labels[lang]["value"]

        language_codes = self.languages_from_country()
        for code in language_codes:
            if code in labels and code in sitelinks:
                return labels[code]["value"]

        for code in language_codes:
            if code in labels:
                return labels[code]["value"]

        if "en" in labels:
            return labels["en"]["value"]

        for code in sitelinks:
            if code in labels:
                return labels[code]["value"]

        return list(labels.values())[0]["value"]

    @property
    def names(self):
        return dict(names_from_entity(self.entity))

    @property
    def is_a(self):
        return [
            isa["mainsnak"]["datavalue"]["value"]["id"]
            for isa in self.entity.get("claims", {}).get("P31", [])
        ]

    @property
    def is_a_detail(self):
        return [WikidataItem.retrieve_item(qid) for qid in self.is_a]

    def is_proposed(self):
        """is this a proposed building or structure (Q811683)?"""
        return "Q811683" in self.is_a

    def criteria(self):
        items = {row["tag"]["value"] for row in self.osm_keys}
        for is_a in self.is_a:
            items |= set(extra_keys.get(is_a, []))

        # Ignore some overly generic tags from Wikidata objects:
        # facility (Q13226383)            - osm key: amenity
        # geographic location (Q2221906)  - osm key: location
        # artificial entity (Q16686448)   - osm key: man_made
        # room (Q180516)                  - osm key: room

        items.discard("Key:amenity")
        items.discard("Key:location")
        items.discard("Key:man_made")
        items.discard("Key:room")
        return items

    def report_broken_wikidata_osm_tags(self):
        start_allowed = ("Key", "Tag", "Role", "Relation")
        for row in self.osm_keys:
            value = row["tag"]["value"]
            if any(value.startswith(f"{start}:") for start in start_allowed):
                continue
            isa_item_id = wd_uri_to_id(row["item"]["value"])
            isa_qid = "Q{:d}".format(isa_item_id)
            body = f"""
qid: {self.qid}\n
IsA: https://osm.wikidata.link/reports/isa/{isa_qid}
row: {repr(row)}\n"""
            mail.send_mail("broken OSM tag in Wikidata", body)

    def find_nrhp_match(self, overpass_reply):
        nrhp = self.nrhp
        if not nrhp:
            return
        osm = [e for e in overpass_reply if e["tags"].get("ref:nrhp") == nrhp]
        if len(osm) == 1:
            return osm[0]

    def parse_item_query(self, criteria, overpass_reply):
        nrhp_match = self.find_nrhp_match(overpass_reply)
        if nrhp_match:
            return [(nrhp_match, None)]

        wikidata_names = self.names
        self.trim_location_from_names(wikidata_names)
        endings = matcher.get_ending_from_criteria(
            {i.partition(":")[2] for i in criteria}
        )

        found = []
        for element in overpass_reply:
            m = match.check_for_match(element["tags"], wikidata_names, endings=endings)
            if m:
                element["key"] = "{0[type]:s}_{0[id]:d}".format(element)
                found.append((element, m))
        return found
