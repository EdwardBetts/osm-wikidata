"""Functions for querying taginfo."""

from collections import defaultdict
from typing import Any, TypedDict, cast

import requests

from . import CallParams

api_root_url = "https://taginfo.openstreetmap.org/api/4/"


class EntityType(TypedDict):
    """Type for entity type dict."""

    cats: list[str]
    tags: list[str]
    trim: list[str]
    check_housename: bool
    wikidata: str
    dist: int


class TagInfoTagReply(TypedDict):
    key: str
    value: str
    count_all: int
    wiki: dict[str, dict[str, str | None]]


class TagInfoKeyReply(TypedDict):
    key: str
    count_all: int


TagInfoDict = dict[str, int | str]


EntityTypeList = list[EntityType]


def build_all_tags(entity_types: EntityTypeList) -> dict[str, set[str]]:
    """Build tag key to value set mapping for a list of entity types."""
    all_tags = defaultdict(set)
    for i in entity_types:
        for t in i["tags"]:
            if "=" not in t:
                continue
            k, v = t.split("=")
            all_tags[k].add(v)
    return dict(all_tags)


def api_call(method: str, params: CallParams) -> Any:
    """Call taginfo API."""
    params["format"] = "json_pretty"
    r = requests.get(api_root_url + method, params=params)
    return r.json()["data"]


def get_tags(entity_types: EntityTypeList) -> list[TagInfoTagReply]:
    all_tags = build_all_tags(entity_types)
    params: CallParams = {
        "tags": ",".join("{}={}".format(k, ",".join(v)) for k, v in all_tags.items()),
    }

    return cast(list[TagInfoTagReply], api_call("tags/list", params))


def get_keys() -> list[TagInfoKeyReply]:
    """Get the top 200 taginfo keys that are in the wiki."""
    params: CallParams = {
        "page": 1,
        "rp": 200,
        "filter": "in_wiki",
        "sortname": "count_all",
        "sortorder": "desc",
    }

    return cast(list[TagInfoKeyReply], api_call("keys/all", params))


def get_taginfo(entity_types: EntityTypeList) -> dict[str, TagInfoDict]:
    tags = get_tags(entity_types)
    keys = get_keys()

    taginfo: dict[str, TagInfoDict] = {
        i["key"]: {"count_all": i["count_all"]} for i in keys
    }

    for i in tags:
        tag = i["key"] + "=" + i["value"]
        taginfo[tag] = {"count_all": i["count_all"]}
        if not i.get("wiki"):
            continue
        image = cast(dict[str, Any], i["wiki"]).get("en", {}).get("image")
        if not image:
            image = next(
                (lang["image"] for lang in i["wiki"].values() if "image" in lang), None
            )
        if image:
            taginfo[tag]["image"] = image

    return taginfo
