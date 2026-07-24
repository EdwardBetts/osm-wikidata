"""Authenticated edits to Wikidata."""

import json
import typing

import requests

from . import wikidata_oauth
from .wikimedia_api_logging import logged_request

osm_type_property = {
    "node": "P11693",
    "way": "P10689",
    "relation": "P402",
}


class WikidataEditError(Exception):
    """Unexpected Wikidata edit API response."""


def api_post_request(
    params: dict[str, str | int],
    timeout: int = 10,
) -> dict[str, typing.Any]:
    """Send an authenticated POST request to the Wikidata API."""
    oauth = wikidata_oauth.get_session()
    r = logged_request(
        oauth,
        "POST",
        wikidata_oauth.api_url,
        data=params,
        timeout=timeout,
    )
    try:
        data = r.json()
    except requests.exceptions.JSONDecodeError:
        raise WikidataEditError(f"HTTP {r.status_code}: {r.text[:200]!r}")

    if "error" in data:
        error = data["error"]
        message = error.get("info") or error.get("code") or data
        raise WikidataEditError(str(message))

    return typing.cast(dict[str, typing.Any], data)


def get_csrf_token() -> str:
    """Get a CSRF token for Wikidata edits."""
    data = api_post_request(
        {
            "action": "query",
            "meta": "tokens",
            "format": "json",
            "formatversion": 2,
        }
    )
    return typing.cast(str, data["query"]["tokens"]["csrftoken"])


def claim_exists(
    entity: dict[str, typing.Any],
    property_id: str,
    value: str,
) -> bool:
    """Return true if the entity already has the given external-id claim."""
    for claim in entity.get("claims", {}).get(property_id, []):
        mainsnak = claim.get("mainsnak", {})
        datavalue = mainsnak.get("datavalue", {})
        if datavalue.get("value") == value:
            return True
    return False


def add_osm_link(
    qid: str,
    osm_type: str,
    osm_id: int | str,
    entity: dict[str, typing.Any] | None = None,
    summary: str | None = None,
) -> bool:
    """Add the reciprocal OpenStreetMap ID statement to Wikidata.

    Returns true when a statement was created, or false when it already existed.
    """
    property_id = osm_type_property[osm_type]
    value = str(osm_id)
    if entity is not None and claim_exists(entity, property_id, value):
        return False

    token = get_csrf_token()
    data = api_post_request(
        {
            "action": "wbcreateclaim",
            "format": "json",
            "formatversion": 2,
            "entity": qid,
            "property": property_id,
            "snaktype": "value",
            "value": json.dumps(value),
            "token": token,
            "summary": summary or f"add OpenStreetMap {osm_type} ID from OWL Places",
        },
        timeout=20,
    )
    if "claim" not in data:
        raise WikidataEditError(f"Unexpected Wikidata API response: {data!r}")
    return True
