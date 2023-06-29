"""Nominatim geocode."""

import json
import typing
from collections import OrderedDict

import requests
from flask import current_app

from . import user_agent_headers


class SearchError(Exception):
    """Search error."""


Hit = dict[str, typing.Any]


def lookup_with_params(**kwargs: str | int) -> list[Hit]:
    """Nominatim geocode with parameters."""
    url = "http://nominatim.openstreetmap.org/search"

    params = {
        "format": "jsonv2",
        "addressdetails": 1,
        "email": current_app.config["ADMIN_EMAIL"],
        "extratags": 1,
        "limit": 20,
        "namedetails": 1,
        "accept-language": "en",
        "polygon_text": 1,
    }
    params.update(kwargs)
    r = requests.get(url, params=params, headers=user_agent_headers())
    if r.status_code == 500:
        raise SearchError

    try:
        hits: list[Hit] = json.loads(r.text, object_pairs_hook=OrderedDict)
    except json.decoder.JSONDecodeError:
        raise SearchError(r)

    return hits


def lookup(q: str) -> list[Hit]:
    """Do nominatim lookup with given query."""
    return lookup_with_params(q=q)


def get_us_county(county: str, state: str) -> Hit | None:
    """Look for US county in nominatim."""
    if " " not in county and "county" not in county:
        county += " county"
    results: list[Hit] = lookup(q="{}, {}".format(county, state))

    def pred(hit: Hit) -> typing.TypeGuard[Hit]:
        return (
            "osm_type" in hit
            and hit["osm_type"] != "node"
            and county in hit["display_name"].lower()
        )

    return next(filter(pred, results), None)


def get_us_city(name: str, state: str) -> Hit | None:
    """Lookup US city via Nominatim."""
    results = lookup_with_params(city=name, state=state)
    if len(results) != 1:
        results = [
            hit for hit in results if hit["type"] == "city" or hit["osm_type"] == "node"
        ]
        if len(results) != 1:
            print("more than one")
            return None
    hit = results[0]
    if hit["type"] not in ("administrative", "city"):
        print("not a city")
        return None
    if hit["osm_type"] == "node":
        print("node")
        return None
    if not hit["display_name"].startswith(name):
        print("wrong name")
        return None
    assert "osm_type" in hit and "osm_id" in hit and "geotext" in hit
    return hit


def reverse(osm_type: str, osm_id: int, polygon_text: int = 1) -> dict[str, typing.Any]:
    """Reverse geocode using nominatim."""
    url = "https://nominatim.openstreetmap.org/reverse"

    params = {
        "osm_type": osm_type[0].upper(),
        "osm_id": osm_id,
        "format": "jsonv2",
        "addressdetails": 1,
        "email": current_app.config["ADMIN_EMAIL"],
        "extratags": 1,
        "namedetails": 1,
        "accept-language": "en",
        "polygon_text": polygon_text,
    }
    r = requests.get(url, params=params, headers=user_agent_headers())
    if r.status_code == 500:
        raise SearchError

    try:
        hit: dict[str, typing.Any] = json.loads(r.text, object_pairs_hook=OrderedDict)
    except json.decoder.JSONDecodeError:
        raise SearchError(r)

    if "error" in hit:
        raise SearchError(r)

    return hit
