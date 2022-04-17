from flask import current_app
from collections import Counter, defaultdict
from . import match, database, wikidata, embassy, model, mail

import os.path
import json
import re

cat_to_ending = {}
patterns = {}
entity_types = {}
default_max_dist = 4
extract_name_good_enough = True

re_farmhouse = re.compile("^(.*) farm ?house$", re.I)


def get_pattern(key):
    if key in patterns:
        return patterns[key]
    return patterns.setdefault(key, re.compile(r"\b" + re.escape(key) + r"\b", re.I))


def categories_to_tags(categories, cat_to_entity=None):
    if cat_to_entity is None:
        cat_to_entity = build_cat_map()
    tags = set()
    for cat in categories:
        lc_cat = cat.lower()
        for key, value in cat_to_entity.items():
            if not get_pattern(key).search(lc_cat):
                continue
            exclude = value.get("exclude_cats")
            if exclude:
                pattern = re.compile(
                    r"\b(" + "|".join(re.escape(e) for e in exclude) + r")\b", re.I
                )
                if pattern.search(lc_cat):
                    continue
            tags |= set(value["tags"])
    return sorted(tags)


def categories_to_tags_map(categories):
    cat_to_entity = build_cat_map()
    ret = defaultdict(set)
    for cat in categories:
        lc_cat = cat.lower()
        for key, value in cat_to_entity.items():
            if not get_pattern(key).search(lc_cat):
                continue
            exclude = value.get("exclude_cats")
            if exclude:
                pattern = re.compile(
                    r"\b(" + "|".join(re.escape(e) for e in exclude) + r")\b", re.I
                )
                if pattern.search(lc_cat):
                    continue
            ret[cat] |= set(value["tags"])
    return ret


def load_entity_types():
    data_dir = current_app.config["DATA_DIR"]
    filename = os.path.join(data_dir, "entity_types.json")
    return json.load(open(filename))


def simplify_tags(tags):
    """remove foo=bar if dict contains foo"""
    key_only = sorted(t for t in tags if "=" not in t)
    for k in key_only:
        for t in set(tags):
            if t.startswith(k + "="):
                tags.remove(t)
    return tags


def tag_and_key_if_possible(tags):
    """remove foo if dict contains foo=bar"""
    key_only = sorted(t for t in tags if "=" not in t)
    for k in key_only:
        for t in set(tags):
            if t.startswith(k + "="):
                if k in tags:
                    tags.remove(k)
                continue
    return tags


def build_cat_map():
    cat_to_entity = {}
    for i in load_entity_types():
        for c in i["cats"]:
            lc_cat = c.lower()
            if " by " in lc_cat:
                lc_cat = lc_cat[: lc_cat.find(" by ")]
            cat_to_entity[lc_cat] = i
    return cat_to_entity


def get_ending_from_criteria(tags):
    global entity_types

    if not entity_types:
        entity_types = load_entity_types()
    tags = set(tags)
    tags.discard("type=site")  # too generic

    endings = set()
    for t in entity_types:
        if tags & set(t["tags"]):
            endings.update(t.get("trim", []))

    return endings


def could_be_building(tags, instanceof):
    place_tags = {
        "place",
        "place=neighbourhood",
        "landuse=residential",
        "boundary=administrative",
        "admin_level",
    }
    if tags.issubset(place_tags):
        return False  # human settlement

    if any(tag.startswith("building") for tag in tags):
        return True

    global entity_types

    if not entity_types:
        entity_types = load_entity_types()

    check_housename = False
    found_instanceof = False
    if instanceof:
        for t in entity_types:
            if t.get("wikidata") not in instanceof:
                continue
            found_instanceof = True
            if t.get("check_housename"):
                check_housename = True
    if found_instanceof:
        return check_housename

    tags = set(tags)
    return any(t.get("check_housename") and tags & set(t["tags"]) for t in entity_types)


def get_max_dist_from_criteria(tags):
    global entity_types

    if not entity_types:
        entity_types = load_entity_types()
    tags = set(tags)

    max_dists = []
    for t in entity_types:
        type_max_dist = t.get("dist")
        if type_max_dist and tags & set(t["tags"]):
            max_dists.append(type_max_dist)

    return max(max_dists) if max_dists else None


def hstore_query(tags):
    """hstore query for use with osm2pgsql database"""
    cond = []
    for tag in tags:
        if "=" not in tag:
            cond.append(f"(tags ? '{tag}')")
            continue
        k, v = tag.split("=")
        cond.append(f"('{v}' = any(string_to_array((tags->'{k}'), ';')))")
        if "_" not in v:
            continue
        space = v.replace("_", " ")
        cond.append(f"('{space}' = any(string_to_array((tags->'{k}'), ';')))")

    return " or\n ".join(cond)


def nearby_nodes_sql(item, prefix, max_dist=10, limit=50):
    point = f"ST_TRANSFORM(ST_GeomFromEWKT('{item.ewkt}'), 3857)"
    sql = (
        f"select 'point', osm_id, name, tags, "
        f"ST_Distance({point}, way) as dist "
        f"from {prefix}_point "
        f"where ST_DWithin({point}, way, {max_dist})"
    )
    return sql


def existing_sql(prefix):
    sql_list = []
    for obj_type in "point", "line", "polygon":
        obj_sql = f"select '{obj_type}', osm_id, tags from {prefix}_{obj_type} "
        sql_list.append(obj_sql)
    return "select * from (" + " union ".join(sql_list) + f") a where tags ? 'wikidata'"


def get_existing(cur, prefix):
    sql = existing_sql(prefix)
    cur.execute(sql)
    rows = cur.fetchall()
    existing = defaultdict(list)
    for src_type, src_id, osm_tags in rows:
        (osm_type, osm_id) = get_osm_id_and_type(src_type, src_id)
        wikidata_tag = osm_tags.get("wikidata")
        if not wikidata_tag:
            continue
        wikidata_tag = wikidata_tag.strip()
        if wikidata_tag[0] != "Q" or not wikidata_tag[1:].isdigit():
            continue
        existing[wikidata_tag].append((osm_type, osm_id))

    return dict(existing)


def item_match_sql(item, prefix, ignore_tags=None, limit=50):
    point = "ST_TRANSFORM(ST_GeomFromEWKT('{}'), 3857)".format(item.ewkt)
    item_max_dist = get_max_dist_from_criteria(item.tags) or default_max_dist

    tags = item.calculate_tags(ignore_tags=ignore_tags)
    if not tags:
        return

    hstore = hstore_query(tags)
    assert hstore

    sql_list = []
    for obj_type in "point", "line", "polygon":
        obj_sql = (
            f"select '{obj_type}', osm_id, name, tags, "
            f"ST_Distance({point}, way) as dist "
            f"from {prefix}_{obj_type} "
            f"where ST_DWithin({point}, way, {item_max_dist} * 1000)"
        )
        sql_list.append(obj_sql)
    sql = (
        "select * from ("
        + " union ".join(sql_list)
        + f") a where ({hstore}) order by dist limit {limit}"
    )
    return sql


def run_sql(cur, sql, debug=False):
    if debug:
        print(sql)

    cur.execute(sql)
    return cur.fetchall()


def find_nrhp_match(nrhp_numbers, rows):
    nrhp_numbers = set(nrhp_numbers)
    nrhp_match = []
    for src_type, src_id, osm_name, osm_tags, dist in rows:
        (osm_type, osm_id) = get_osm_id_and_type(src_type, src_id)

        if osm_tags.get("ref:nrhp") not in nrhp_numbers:
            continue

        candidate = {
            "osm_type": osm_type,
            "osm_id": osm_id,
            "name": osm_name,
            "tags": osm_tags,
            "dist": dist,
            "planet_table": src_type,
            "src_id": src_id,
        }
        nrhp_match.append(candidate)

    if len(nrhp_match) == 1:
        return nrhp_match


def find_matching_tags(osm, wikidata):
    matching = set()
    for wikidata_tag in wikidata:
        if "=" in wikidata_tag:
            k, _, v = wikidata_tag.partition("=")
            if k in osm and v in set(osm[k].split(";")):
                matching.add(wikidata_tag)
        elif wikidata_tag in osm:
            matching.add(wikidata_tag)
    return tag_and_key_if_possible(matching)


def bad_building_match(osm_tags, name_match, item):
    if "amenity" in osm_tags:
        amenity = set(osm_tags["amenity"].split(";"))
        if "parking" in amenity:
            return True

    if not name_match:
        return False

    wd_station = item.is_a_station()
    osm_station = any(
        k.endswith("railway") and v in {"station", "halt"} for k, v in osm_tags.items()
    )
    is_station = wd_station or osm_station

    for osm, detail_list in name_match.items():
        for match_type, _, source in detail_list:
            if match_type == "both_trimmed":
                continue
            if (
                not extract_name_good_enough
                and len(source) == 1
                and source[0][0] == "extract"
            ):
                continue
            if osm == "operator" and match_type == "wikidata_trimmed":
                continue
            if match_type == "wikidata_trimmed" and is_station:
                continue
            return False

    return True


def is_osm_bus_stop(tags):
    return tags.get("highway") == "bus_stop" or (
        tags.get("bus") == "yes" and tags.get("public_transport") == "stop_position"
    )


def is_diplomatic_mission(matching_tags, osm_tags):
    if "amenity=embassy" in matching_tags:
        return True
    if osm_tags.get("office") == "diplomatic":
        return True
    terms = ["embassy", "diplomatic", "consulate", "ambassador"]
    for key, value in osm_tags.items():
        if "name" not in key or "old" in key:
            continue
        lc_name = value.lower()
        if any(term in lc_name for term in terms):
            return True
    return False


def diplomatic_mission_different_country(item, tags):
    name = tags.get("name:en") or tags.get("name")
    osm_country = tags.get("diplomatic:sending_country") or tags.get("country")
    item_countries = {country["id"] for country in item.get_claim("P137")}

    if name:
        name_country = embassy.from_name(name)
        if name_country and name_country["qid"] not in item_countries:
            return True

    if not osm_country or len(osm_country) not in (2, 3):
        return False

    codes = set()
    for qid in item_countries:
        # no ISO code for 'Embassy of South Ossetia, Moscow'
        # https://www.wikidata.org/wiki/Q4374094

        iso_codes = wikidata.country_iso_codes_from_qid(qid)
        if iso_codes:
            codes.update(iso_codes)

    return not any(iso_code.upper() == osm_country.upper() for iso_code in codes)


def is_building_only_match(matching_tags):
    building_tags = {"building", "building=yes", "historic:building"}
    return matching_tags.issubset(building_tags)


def is_bad_match(item, osm_tags):
    for bad_match_filter in model.BadMatchFilter.query:
        if bad_match_filter.check(item.tags, osm_tags):
            return True

    amenity = set(osm_tags["amenity"].split(";") if "amenity" in osm_tags else [])

    building = set(osm_tags["building"].split(";") if "building" in osm_tags else [])

    if "car_rental" in amenity or "car_sharing" in amenity:
        return True  # building only not good enough

    for building_type in ("stable", "barn", "farm_auxiliary"):
        if (
            "building=" + building_type in item.tags
            and "building=house" not in item.tags
            and "house" in building
            and building_type not in building
        ):
            return True  # Wikidata stable shouldn't match OSM house

    if (
        "building=tower" in item.tags
        and "amenity=pub" not in item.tags
        and "pub" in amenity
        and "man_made" not in osm_tags
    ):
        return True  # Wikidata tower shouldn't match OSM pub

    if (
        "historic=castle" in item.tags
        and "railway=station" not in item.tags
        and (
            osm_tags.get("railway") == "station"
            or osm_tags.get("building") == "train_station"
        )
        and osm_tags.get("historic") != "castle"
    ):
        return True  # castle shouldn't railway station

    if (
        "railway=station" in item.tags
        and "amenity=cafe" not in item.tags
        and "cafe" in amenity
        and osm_tags.get("railway") != "station"
        and osm_tags.get("building") != "train_station"
    ):
        return True  # station shouldn't match cafe

    if (
        "railway=station" in item.tags
        and "shop=supermarket" not in item.tags
        and "supermarket" == osm_tags.get("shop")
        and osm_tags.get("railway") != "station"
        and osm_tags.get("building") != "train_station"
    ):
        return True  # station shouldn't match supermarket

    if (
        "building=train_station" not in item.tags
        and osm_tags.get("building") == "train_station"
    ):
        return True  # non-station shouldn't match station

    if "amenity=fuel" not in item.tags and "fuel" in amenity:
        return True  # petrol station

    if (
        "place" in item.tags
        and not any(t.startswith("railway") for t in item.tags)
        and "place" not in osm_tags
        and "railway" in osm_tags
    ):
        return True  # place shouldn't match railway

    return False


def is_address_node(osm_type, osm_tags):
    if osm_type != "node" or "addr:housename" in osm_tags:
        return False

    return all(
        key.startswith("addr:")
        or key.startswith("source:")
        or key in {"entrance", "source"}
        for key in osm_tags.keys()
    )


def get_within_names(cur, prefix, src_type, src_id):
    sql = f"""select a.tags
from {prefix}_polygon as a, {prefix}_{src_type} as b
where b.osm_id={src_id} and a.osm_id != b.osm_id and st_contains(a.way, b.way);
"""
    names = set()
    for (osm_tags,) in run_sql(cur, sql):
        if osm_tags.keys() & {"place", "tourism"}:
            names.update(match.get_names(osm_tags).values())
    return names


def find_item_matches(cur, item, prefix, debug=False):
    if not item or not item.entity:
        return []
    wikidata_names = item.names()
    if not wikidata_names:
        return []

    cats = item.categories or []

    # point = "ST_GeomFromEWKT('{}')".format(item.ewkt)

    # item_max_dist = max(max_dist[cat] for cat in item['cats'])

    item_is_a_historic_district = item.is_a_historic_district()
    ignore_tags = {"building"} if item_is_a_historic_district else set()
    sql = item_match_sql(item, prefix, ignore_tags=ignore_tags)
    rows = run_sql(cur, sql, debug) if sql else []

    sql = nearby_nodes_sql(item, prefix)
    rows += run_sql(cur, sql, debug)
    if not rows:
        return []

    if debug:
        print("row count:", len(rows))
        print()
    seen = set()

    nrhp_numbers = item.ref_nrhp()
    if nrhp_numbers:
        found = find_nrhp_match(nrhp_numbers, rows)
        if found:
            return found

    item_identifiers = item.get_item_identifiers()

    endings = get_ending_from_criteria(item.tags)
    endings |= item.more_endings_from_isa()

    if item.is_farmhouse():
        endings = set()

    wikidata_tags = item.calculate_tags()

    place_names = item.place_names()
    instanceof = set(item.instanceof())
    is_hamlet = item.is_hamlet()
    if is_hamlet:
        endings.discard("house")

    endings.discard("street")

    check_within = current_app.config.get("HUNT_FOR_MORE_PLACE_NAMES") or False
    match_address_nodes = current_app.config.get("MATCH_ADDRESS_NODES") or False

    candidates = []
    for src_type, src_id, osm_name, osm_tags, dist in rows:
        (osm_type, osm_id) = get_osm_id_and_type(src_type, src_id)
        if (osm_type, osm_id) in seen:
            continue
        if debug:
            print((osm_type, osm_id, osm_name, osm_tags, dist))
        seen.add((osm_type, osm_id))

        if (
            osm_tags.get("locality") == "townland"
            and "locality=townland" not in item.tags
        ):
            continue  # only match townlands when specifically searching for one

        if item_is_a_historic_district and "building" in osm_tags:
            continue  # historic district shouldn't match building

        if not match_address_nodes and is_address_node(osm_type, osm_tags):
            continue  # Don't match address nodes. There are lots of these in New York.

        try:
            admin_level = (
                int(osm_tags["admin_level"]) if "admin_level" in osm_tags else None
            )
        except Exception:
            admin_level = None

        identifier_match = match.check_identifier(osm_tags, item_identifiers)

        if not identifier_match:
            if any(c.startswith("Cities ") for c in cats) and admin_level == 10:
                continue

        address_match = match.check_name_matches_address(osm_tags, wikidata_names)

        if address_match is False:  # OSM and Wikidata addresses differ
            continue

        if not address_match and match.check_for_address_in_extract(
            osm_tags, item.extract
        ):
            address_match = True

        if check_within:
            within = get_within_names(cur, prefix, src_type, src_id)
        else:
            within = set()

        name_match = match.check_for_match(
            osm_tags,
            wikidata_names,
            endings,
            place_names=place_names | within,
            trim_house=not is_hamlet,
        )

        if "seamark:name" in name_match and "man_made=lighthouse" not in item.tags:
            del name_match["seamark:name"]  # not a lighthouse

        if not (identifier_match or address_match or name_match):
            continue

        matching_tags = find_matching_tags(osm_tags, wikidata_tags)

        if is_diplomatic_mission(
            matching_tags, osm_tags
        ) and diplomatic_mission_different_country(item, osm_tags):
            continue

        building_only_match = is_building_only_match(matching_tags)

        amenity = set(osm_tags["amenity"].split(";") if "amenity" in osm_tags else [])

        if (
            "building" in item.tags
            and "amenity=car_sharing" not in item.tags
            and "building" not in osm_tags
            and "car_sharing" in amenity
        ):
            continue  # Wikidata building shouldn't match car sharing

        if (
            building_only_match
            and address_match
            and not name_match
            and not identifier_match
        ):
            if (
                "amenity=school" in item.tags
                and "amenity=restaurant" not in item.tags
                and "restaurant" in amenity
                and "school" not in amenity
            ):
                continue  # Wikidata school shouldn't match OSM restaurant

        if (
            building_only_match
            and not address_match
            and name_match
            and not identifier_match
            and is_bad_match(item, osm_tags)
        ):
            continue

        if (not matching_tags or building_only_match) and instanceof == {"Q34442"}:
            continue  # nearby road match

        if osm_tags.get("amenity") == "parking" and "amenity=parking" not in item.tags:
            continue  # parking garage in OSM should only match parking Wikidata item

        if (
            not matching_tags
            and is_osm_bus_stop(osm_tags)
            and "Q953806" not in instanceof
        ):
            continue  # nearby match OSM bus stop matching non-bus stop

        if (
            "leisure=park" in matching_tags
            and item.is_cricket_ground()
            and (
                osm_tags.get("designation") == "common" or "common" in osm_name.lower()
            )
        ):
            continue

        if (
            name_match
            and not identifier_match
            and not address_match
            and building_only_match
        ):
            if bad_building_match(osm_tags, name_match, item):
                continue
            wd_stadium = item.is_a_stadium()
            if (
                wd_stadium
                and "amenity=restaurant" not in item.tags
                and "restaurant" in amenity
            ):
                continue
            if wd_stadium and osm_tags.get("shop") == "supermarket":
                continue

        if (
            matching_tags == {"natural=peak"}
            and item.is_mountain_range()
            and dist > 100
        ):
            continue

        if item.is_nhle and dist > 500:
            continue  # NHLE items normally have quite precise coordinates

        if (
            not identifier_match
            and "railway=station" in item.tags
            and "amenity=ferry_terminal" not in item.tags
            and "ferry_terminal" in amenity
            and osm_tags.get("railway") != "station"
            and osm_tags.get("building") != "train_station"
        ):
            continue  # station shouldn't match ferry terminal

        if (
            "amenity=place_of_worship" in item.tags
            and "man_made=bridge" not in item.tags
            and osm_tags.get("man_made") == "bridge"
            and "place_of_worship" not in amenity
        ):
            continue  # place of worship shouldn't match bridge

        if (
            not name_match
            and address_match
            and (
                "building=apartments" in item.tags
                or "building=residential" in item.tags
            )
            and not any(tag.startswith("shop") for tag in item.tags)
            and "shop" in osm_tags
            and osm_tags.get("building") not in ("apartments", "residential")
        ):
            continue  # apartment building shouldn't match shop

        if (
            not name_match
            and address_match
            and "studio=audio" in item.tags
            and not any(tag.startswith("shop") for tag in item.tags)
            and "shop" in osm_tags
            and osm_tags.get("studio") != "audio"
        ):
            continue  # recording studio shouldn't match shop

        if (
            "artwork_type=statue" in item.tags
            and "tourism=museum" not in item.tags
            and osm_tags.get("tourism") == "museum"
            and osm_tags.get("artwork_type") != "statue"
        ):
            continue  # statue shouldn't match museum

        if (
            "historic=memorial" not in item.tags
            and osm_tags.get("historic") == "memorial"
        ):
            continue  # only memorial should match memorial

        sql = (
            f"select ST_AsText(ST_Transform(way, 4326)) "
            f"from {prefix}_{src_type} "
            f"where osm_id={src_id}"
        )
        cur.execute(sql)
        row = cur.fetchone()
        geom = row and row[0]

        candidate = {
            "osm_type": osm_type,
            "osm_id": osm_id,
            "name": osm_name,
            "tags": osm_tags,
            "dist": dist,
            # 'match': match.match_type.name,
            "planet_table": src_type,
            "src_id": src_id,
            "geom": geom,
            "identifier_match": identifier_match,
            "address_match": address_match,
            "name_match": name_match,
            "matching_tags": matching_tags,
        }
        candidates.append(candidate)
    candidates = filter_distant(candidates)
    candidates = prefer_proper_name_match(candidates)
    candidates = prefer_key_over_building(candidates, "amenity")
    candidates = prefer_tag_match_over_building_only_match(candidates)
    candidates = prefer_railway_station(candidates)
    if candidates and item.is_farmhouse():
        candidates = prefer_farmhouse(candidates)
    if "man_made=bridge" in item.tags:
        candidates = filter_bridge(candidates)
    return candidates


def prefer_tag_match_over_building_only_match(candidates):
    if len(candidates) == 1:
        return candidates
    more_good = []
    less_good = []
    for c in candidates:
        good = c["name_match"] and not is_building_only_match(c["matching_tags"])
        (more_good if good else less_good).append(c)

    if not more_good or not less_good:
        return candidates

    # consider distance
    # Q18160107 has a nearby building only match and a name+tag match 300m away
    # the nearby match is correct

    if all(c["dist"] > 100 for c in more_good) and any(
        c["dist"] < 10 for c in less_good
    ):
        return candidates

    return more_good


def prefer_farmhouse(candidates):
    if len(candidates) != 2:
        return candidates

    if any(not c.get("name") for c in candidates):
        return candidates

    house, farmhouse = sorted(candidates, key=lambda c: len(c["name"]))

    m = re_farmhouse.match(farmhouse["name"])
    if not m:
        return candidates
    farmhouse_start = m.group(1).lower()
    house_name = house["name"].lower()

    if house_name == farmhouse_start or house_name == farmhouse_start + " house":
        return [farmhouse]
    else:
        return candidates


def filter_bridge(candidates):
    if not any(c["tags"].get("man_made") == "bridge" for c in candidates):
        return candidates

    return [
        c for c in candidates if "man_made" in c["tags"] or "bridge" not in c["tags"]
    ]


def check_item_candidate(candidate):
    item, osm_tags = candidate.item, candidate.tags
    cats = item.categories or []
    item_identifiers = item.get_item_identifiers()
    wikidata_names = item.names()

    wikidata_tags = item.calculate_tags()

    endings = get_ending_from_criteria(item.tags)
    endings |= item.more_endings_from_isa()

    place_names = item.place_names()
    instanceof = set(item.instanceof())

    try:
        admin_level = (
            int(osm_tags["admin_level"]) if "admin_level" in osm_tags else None
        )
    except Exception:
        admin_level = None

    if item.is_a_historic_district() and "building" in osm_tags:
        return {"reject": "historic district shouldn't match building"}
    identifier_match = match.check_identifier(osm_tags, item_identifiers)
    if not identifier_match:
        if any(c.startswith("Cities ") for c in cats) and admin_level == 10:
            return {"reject": "bad city match"}

    address_match = match.check_name_matches_address(osm_tags, wikidata_names)

    if address_match is False:  # OSM and Wikidata addresses differ
        return {"reject": "OSM and Wikidata addresses differ"}

    if not address_match and match.check_for_address_in_extract(osm_tags, item.extract):
        address_match = True

    is_hamlet = item.is_hamlet()

    if is_hamlet:
        endings.discard("house")

    name_match = match.check_for_match(
        osm_tags,
        wikidata_names,
        endings,
        place_names=place_names,
        trim_house=not is_hamlet,
    )

    if not (identifier_match or address_match or name_match):
        return {"reject": "no match", "place_names": place_names}

    matching_tags = find_matching_tags(osm_tags, wikidata_tags)

    building_only_match = is_building_only_match(matching_tags)

    amenity = set(osm_tags["amenity"].split(";") if "amenity" in osm_tags else [])

    if (
        building_only_match
        and address_match
        and not name_match
        and not identifier_match
        and "amenity=school" in item.tags
        and "amenity=restaurant" not in item.tags
        and "restaurant" in amenity
        and "school" not in amenity
    ):
        return {"reject": "Wikidata school shouldn't match OSM restaurant"}

    if (
        building_only_match
        and not address_match
        and name_match
        and not identifier_match
        and is_bad_match(item, osm_tags)
    ):
        return {"reject": "bad match"}

    if (not matching_tags or building_only_match) and instanceof == {"Q34442"}:
        return {"reject": "nearby road match"}

    if not matching_tags and is_osm_bus_stop(osm_tags) and "Q953806" not in instanceof:
        return {"reject": "nearby match OSM bus stop matching non-bus stop"}

    if (
        name_match
        and not identifier_match
        and not address_match
        and building_only_match
    ):
        if bad_building_match(osm_tags, name_match, item):
            return {
                "identifier_match": identifier_match,
                "address_match": address_match,
                "name_match": name_match,
                "matching_tags": matching_tags,
                "place_names": place_names,
                "osm_tags": osm_tags,
                "reject": "bad building match",
            }

        wd_stadium = item.is_a_stadium()
        if (
            wd_stadium
            and "amenity=restaurant" not in item.tags
            and "restaurant" in amenity
        ):
            return {"reject": "stadium shouldn't match restaurant"}
        if wd_stadium and osm_tags.get("shop") == "supermarket":
            return {"reject": "stadium shouldn't match supermarket"}

    if (
        matching_tags == {"natural=peak"}
        and item.is_mountain_range()
        and candidate.dist > 100
    ):
        return {"reject": "mountain range shouldn't match peak"}

    return {
        "identifier_match": identifier_match,
        "address_match": address_match,
        "name_match": name_match,
        "matching_tags": matching_tags,
        "place_names": place_names,
        "osm_tags": osm_tags,
    }


def run_individual_match(place, item):
    conn = database.session.bind.raw_connection()
    cur = conn.cursor()

    candidates = find_item_matches(cur, item, place.prefix, debug=False)
    conn.close()

    return candidates


def get_osm_id_and_type(source_type, source_id):
    if source_type == "point":
        return ("node", source_id)
    if source_id > 0:
        return ("way", source_id)
    return ("relation", -source_id)


def planet_table_id(osm):
    osm_id = int(osm["id"])
    if osm["type"] == "node":
        return ("point", osm_id)
    table = "polygon" if "way_area" in osm["tags"] else "line"
    return (table, osm_id if osm["type"] == "way" else -osm_id)


def get_biggest_polygon(item):
    biggest = None
    biggest_size = None
    for osm in item["candidates"]:
        if osm["type"] not in {"way", "relation"}:
            continue
        if "way_area" not in osm["tags"]:
            continue
        area = float(osm["tags"]["way_area"])
        if biggest is None or area > biggest_size:
            biggest_size = area
            biggest = osm

    return -osm["id"] if osm["type"] == "relation" else osm["id"]


def all_in_one(item, conn, prefix):
    cur = conn.cursor()
    biggest = get_biggest_polygon(item)
    if not biggest:
        return
    sql_list = []

    for table in "point", "line", "polygon":
        id_list = ",".join(
            str(osm["src_id"])
            for osm in item["candidates"]
            if osm["table"] == table and (table == "point" or osm["src_id"] != biggest)
        )

        if not id_list:
            continue
        obj_sql = (
            "select '{}' as t, osm_id, way " "from {}_{} " "where osm_id in ({})"
        ).format(table, table, prefix, id_list)
        sql_list.append(obj_sql)

    if not sql_list:
        return
    sql = (
        "select ST_Within(a.way, b.way) from ("
        + " union ".join(sql_list)
        + ") a, {}_polygon b where b.osm_id={}".format(prefix, biggest)
    )
    cur.execute(sql)
    if all(row[0] for row in cur.fetchall()):
        return biggest


def filter_candidates(items, conn):  # unused?
    assert isinstance(items, list)
    for item in items[:]:
        candidates = item["candidates"]
        for osm in candidates:
            osm["table"], osm["src_id"] = planet_table_id(osm)

        for line in candidates[:]:
            if line["table"] == "line":
                if any(
                    poly["table"] == "polygon" and poly["src_id"] == line["src_id"]
                    for poly in candidates
                ):
                    candidates.remove(line)

        if not candidates:
            items.remove(item)
            continue

        item["candidates"] = candidates
        if len(candidates) == 1:
            continue

        re_place_cat = re.compile(
            r"\b(Districts|Areas|Cities|Towns|Villages|Airports)\b", re.I
        )
        if any(re_place_cat.search(cat) for cat in item["cats"]):
            nodes = [osm for osm in candidates if osm["type"] == "node"]
            if len(nodes) == 1:
                candidates = nodes

        if len(candidates) == 1:
            item["candidates"] = candidates
            continue

        big = all_in_one(item, conn)
        if big:
            for osm in candidates:
                if osm["table"] == "polygon" and osm["src_id"] == big:
                    candidates = [osm]
                    break
        if len(candidates) == 1:
            item["candidates"] = candidates
            continue
    return items


def filter_place(candidates):
    # FIXME: some places are more complex, Cambridge for example
    types = {c.osm_type for c in candidates}
    place_node = None
    other = False
    if len(candidates) < 2 or "node" not in types:
        return
    if len(types) < 2:
        return

    for c in candidates:
        if c.osm_type == "node":
            if "place" in c.tags:
                place_node = c
        else:
            if "admin_level" in c.tags or c.tags.get("landuse") == "residential":
                other = True

    if place_node and other:
        return place_node


def filter_schools(candidates):
    if len(candidates) < 2:
        return
    if all("amenity=school" not in c.matching_tags() for c in candidates):
        return

    # use the one thing tagged amenity=school
    # check everything else is tagged building=school

    match = None
    for c in candidates:
        tags = c.matching_tags()
        if "amenity=school" in tags:
            if match:
                return
            match = c
        elif tags != ["building=school"]:
            return
    return match


def filter_churches(candidates):
    if len(candidates) < 2:
        return
    if all("amenity=place_of_worship" not in c.matching_tags() for c in candidates):
        return

    # use the one thing tagged amenity=place_of_worship
    # check everything else is tagged religion=christian

    match = None
    for c in candidates:
        tags = c.matching_tags()
        if "amenity=place_of_worship" in tags:
            if match:
                return
            match = c
        elif tags != ["religion=christian"]:
            return
    return match


def filter_station(candidates):
    if len(candidates) < 2:
        return

    station = [c for c in candidates if "railway=station" in c.matching_tags()]
    tram_stop = [c for c in candidates if "railway=tram_stop" in c.matching_tags()]

    if len(station) == 1 and len(tram_stop) + 1 == len(candidates):
        return station[0]

    if all("public_transport=station" not in c.matching_tags() for c in candidates):
        return

    # use the one thing tagged public_transport=station
    # check everything else is tagged public_transport=platform

    match = None
    for c in candidates:
        tags = c.matching_tags()
        if "public_transport=station" in tags:
            if match:  # multiple stations
                return
            match = c
        elif "railway=tram_stop" not in tags:
            return
    return match


def filter_building(candidates):
    """
    If the item is primarily a building then pick the building way over a node
    that represents one of the current uses of the building
    """

    if len(candidates) < 2:
        return

    way = [c for c in candidates if c.osm_type == "way"]
    node = [c for c in candidates if c.osm_type == "node"]

    if not (len(way) == 1 and len(node) + 1 == len(candidates)):
        return
    building = way[0]

    if "building" in building.tags:
        return building


def filter_reservoir(candidates):
    if len(candidates) < 2:
        return

    way = [c for c in candidates if c.osm_type == "way"]
    node = [c for c in candidates if c.osm_type == "node"]

    if len(way) == 1 and len(node) + 1 == len(candidates):
        return way[0]


def reduce_candidates(item, candidates):
    # place = filter_place(candidates)
    # if place:
    #     candidates = [place]

    school = filter_schools(candidates)
    if school:
        candidates = [school]

    station = filter_station(candidates)
    if station:
        candidates = [station]

    church = filter_churches(candidates)
    if church:
        candidates = [church]

    if item.is_primarily_building():
        building = filter_building(candidates)
        if building:
            candidates = [building]

    if item.is_reservoir():
        reservoir = filter_reservoir(candidates)
        if reservoir:
            candidates = [reservoir]

    return candidates


def filter_candidates_more(items, bad=None, ignore_existing=False):
    osm_count = Counter()
    by_osm = defaultdict(list)

    if bad is None:
        bad = {}

    for item in items:
        for c in item.candidates:
            by_osm[(c.osm_type, c.osm_id)].append(item)

    for item in items:
        for c in item.candidates:
            osm_count[(c.osm_type, c.osm_id)] += 1
            by_osm[(c.osm_type, c.osm_id)].append(item)

    for item in items:
        if item.item_id in bad:
            yield (item, {"note": "has bad match"})
            continue
        candidates = item.candidates.all()

        done = False
        for candidate in candidates:
            housename = candidate.tags.get("addr:housename")
            if housename and housename.isdigit():
                yield (item, {"note": "number as house name"})
                done = True
                break
            name = candidate.tags.get("name")
            if name and name.isdigit():
                yield (item, {"note": "number as name"})
                done = True
                break
        if done:
            continue

        candidates = reduce_candidates(item, candidates)
        if len(candidates) != 1:
            yield (item, {"note": "more than one candidate found"})
            continue

        candidate = candidates[0]

        if osm_count[(candidate.osm_type, candidate.osm_id)] > 1:
            yield (item, {"note": "OSM candidate matches multiple Wikidata items"})
            continue

        if not ignore_existing and "wikidata" in candidate.tags:
            yield (item, {"note": "candidate already tagged"})
            continue

        yield (item, {"candidate": candidate})


def prefer_key_over_building(candidates, key):
    if len(candidates) == 1:
        return candidates

    best_match = None
    for c in candidates:
        mt = c["matching_tags"]
        if any(tag.startswith(key + "=") for tag in mt):
            if best_match:
                return candidates
            best_match = c
            continue

        if len(mt) != 1 or not list(mt)[0].startswith("building"):
            return candidates

    return [best_match] if best_match else candidates


def image_only_match(m):
    return len(m) == 1 and len(m[0][2]) == 1 and m[0][2][0][0] == "image"


def prefer_proper_name_match(candidates):
    if len(candidates) == 1:
        return candidates

    def candidate_name_match(c):
        """
        does this candidate match contain a name match, as opposed to matching on
        operator or addr:housename.
        """
        nm = c.get("name_match")
        return nm and (
            ("name" in nm and not image_only_match(nm["name"]))
            or any(
                k.startswith("name:") and not image_only_match(v) for k, v in nm.items()
            )
        )

    best_match = None
    for c in candidates:
        if candidate_name_match(c):
            if best_match:
                return candidates
            best_match = c
            continue
        if (
            "addr:housename" not in c["name_match"]
            and "operator" not in c["name_match"]
        ):
            return candidates

    return [best_match] if best_match else candidates


def prefer_railway_station(candidates):
    if len(candidates) == 1:
        return candidates
    station = [c for c in candidates if "railway=station" in c["matching_tags"]]

    if len(station) != 1:
        return candidates

    other = sum(
        1
        for c in candidates
        if "railway=station" not in c["matching_tags"]
        and (
            "railway=tram_stop" in c["matching_tags"]
            or "railway=depot" in c["matching_tags"]
            or "landuse=railway" in c["matching_tags"]
            or "building=train_station" in c["matching_tags"]
            or "public_transport=stop_position" in c["matching_tags"]
        )
    )

    return station if other + 1 == len(candidates) else candidates


def filter_distant(candidates):
    if any(c["tags"].keys() & {"place", "admin_level"} for c in candidates):
        return candidates
    if len(candidates) < 2:
        return candidates

    chosen = None
    for c in candidates:
        if c["dist"] < 50:
            if chosen:
                return candidates
            chosen = c
            continue
        if c["dist"] < 1000:
            return candidates
    return [chosen] if chosen else candidates
