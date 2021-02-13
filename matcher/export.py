import json
from .model import IsA, ItemIsA


def place_for_export(place):
    place_fields = [
        "place_id",
        "osm_type",
        "osm_id",
        "display_name",
        "category",
        "type",
        "place_rank",
        "icon",
        "south",
        "west",
        "north",
        "east",
        "extratags",
        "address",
        "namedetails",
        "item_count",
        "candidate_count",
        "state",
        "override_name",
        "lat",
        "lon",
        "wikidata_query_timeout",
        "wikidata",
        "item_types_retrieved",
        "index_hide",
        "overpass_is_in",
        "existing_wikidata",
        "osm_url",
        "type_label",
        "area",
        "name_for_changeset",
    ]

    item_fields = [
        "item_id",
        "enwiki",
        "entity",
        "categories",
        "qid",
        "query_label",
        "ewkt",
        "extract_names",
        "wikidata_uri",
    ]

    candidate_fields = [
        "osm_id",
        "osm_type",
        "name",
        "dist",
        "tags",
        "planet_table",
        "src_id",
        "identifier_match",
        "address_match",
        "name_match",
    ]

    isa_fields = ["item_id", "entity", "qid", "label"]

    place_data = {key: getattr(place, key) for key in place_fields}

    place_data["added"] = str(place.added)
    place_data["geom"] = json.loads(place.geojson)

    isa_list = []

    item_list = []
    item_map = {}
    item_ids = []
    for item in place.get_candidate_items():
        item_ids.append(item.item_id)

        item_data = {key: getattr(item, key) for key in item_fields}
        item_data["lat"], item_data["lon"] = item.get_lat_lon()
        item_data["tags"] = list(item.tags)
        item_data["extracts"] = dict(item.extracts)
        item_data["candidates"] = []
        item_data["isa"] = []
        for candidate in item.candidates:
            candidate_data = {key: getattr(candidate, key) for key in candidate_fields}
            candidate_data["geom"] = json.loads(place.geojson)
            item_data["candidates"].append(candidate_data)

        item_map[item.item_id] = item_data
        item_list.append(item_data)

    q = IsA.query.join(ItemIsA).filter(ItemIsA.item_id.in_(item_ids))
    for isa in q:
        isa_data = {key: getattr(isa, key) for key in isa_fields}
        isa_list.append(isa_data)

    q = ItemIsA.query.filter(ItemIsA.item_id.in_(item_ids))
    for item_isa in q:
        item_map[item_isa.item_id]["isa"].append(item_isa.isa_id)

    return dict(place=place_data, items=item_list, isa=isa_list)
