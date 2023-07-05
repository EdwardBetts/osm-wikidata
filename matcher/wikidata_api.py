import time
import typing
from typing import Any

import requests
import requests.exceptions
import simplejson.errors

from . import mail, user_agent_headers
from .utils import chunk

wikidata_url = "https://www.wikidata.org/w/api.php"
page_size = 50

EntityType = dict[str, Any]


class TooManyEntities(Exception):
    """Too many entities."""

    pass


CallParams = typing.Mapping[str, str | int]


class QueryError(Exception):
    """Query error."""

    def __init__(self, query: CallParams, r: requests.Response) -> None:
        """Init."""
        self.query = query
        self.r = r


class QueryTimeout(QueryError):
    """Query timeout error."""


def api_call(params: CallParams) -> requests.Response:
    """Call the wikidata API."""
    call_params: CallParams = {
        "format": "json",
        "formatversion": 2,
        **params,
    }

    r = requests.get(wikidata_url, params=call_params, headers=user_agent_headers())
    return r


def entity_iter(
    ids: set[str], debug: bool = False, attempts: int = 5
) -> typing.Iterator[tuple[str, dict[str, typing.Any]]]:
    for num, cur in enumerate(chunk(ids, page_size)):
        if debug:
            print(f"entity_iter: {num * page_size}/{len(ids)}")
        str_ids = "|".join(cur)
        for attempt in range(attempts):
            try:
                r = api_call({"action": "wbgetentities", "ids": str_ids})
                break
            except requests.exceptions.ChunkedEncodingError:
                if attempt == attempts - 1:
                    raise
                time.sleep(1)
        r.raise_for_status()
        json_data = r.json()
        if "entities" not in json_data:
            mail.send_mail("error fetching wikidata entities", r.text)

        for qid, entity in json_data["entities"].items():
            yield qid, entity


def get_entity(qid: str) -> EntityType | None:
    json_data = api_call({"action": "wbgetentities", "ids": qid}).json()

    try:
        entity: EntityType = list(json_data["entities"].values())[0]
    except KeyError:
        return None
    if "missing" not in entity:
        return entity

    return None


def get_lastrevid(qid: str) -> int:
    """Get the lastrevid for the given QID."""
    params: CallParams = {"action": "query", "prop": "info", "titles": qid}
    lastrevid: int = api_call(params).json()["query"]["pages"][0]["lastrevid"]
    return lastrevid


def get_lastrevids(qid_list: list[str]) -> dict[str, int]:
    if not qid_list:
        return {}
    params: typing.Mapping[str, str] = {
        "action": "query",
        "prop": "info",
        "titles": "|".join(qid_list),
    }
    r = api_call(params)
    json_data = r.json()
    if "query" not in json_data:
        print(r.text)
    return {page["title"]: page["lastrevid"] for page in json_data["query"]["pages"]}


def get_entities(ids: list[str], attempts: int = 5) -> list[EntityType]:
    if not ids:
        return []
    if len(ids) > 50:
        raise TooManyEntities
    params: CallParams = {"action": "wbgetentities", "ids": "|".join(ids)}
    for attempt in range(attempts):
        try:  # retry if we get a ChunkedEncodingError
            r = api_call(params)
            try:
                json_data = r.json()
            except simplejson.errors.JSONDecodeError:
                raise QueryError(params, r)
            return list(json_data["entities"].values())
        except requests.exceptions.ChunkedEncodingError:
            if attempt == attempts - 1:
                raise QueryError(params, r)

    return []
