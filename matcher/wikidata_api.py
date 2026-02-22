import json
import os
import time
import typing

import flask
import requests
import requests.exceptions
import simplejson.errors

from . import Entity, mail, user_agent_headers
from .utils import chunk

wikidata_url = "https://www.wikidata.org/w/api.php"
page_size = 50


class TooManyEntities(Exception):
    """Too many entities."""

    pass


CallParams = typing.Mapping[str, str | int]


class QueryError(Exception):
    """Query error."""

    def __init__(self, query: str, r: requests.Response) -> None:
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


def get_entity(qid: str) -> Entity | None:
    json_data = api_call({"action": "wbgetentities", "ids": qid}).json()

    try:
        entity: Entity = list(json_data["entities"].values())[0]
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


def get_entities(ids: list[str], attempts: int = 5) -> list[Entity]:
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


def get_entity_with_cache(qid: str) -> Entity:
    """Get an item from Wikidata."""
    cache_dir = flask.current_app.config["CACHE_DIR"]
    cache_filename = os.path.join(cache_dir, qid + ".json")
    entity: Entity
    if os.path.exists(cache_filename):
        entity = json.load(open(cache_filename))
        return entity

    r = api_call({"action": "wbgetentities", "ids": qid})
    entity = r.json()["entities"][qid]
    with open(cache_filename, "w") as f:
        json.dump(entity, f, indent=2)
    return entity


def get_entities_with_cache(qids: list[str]) -> list[Entity]:
    """Get an item from Wikidata."""
    items: list[Entity] = []
    missing: list[str] = []
    cache_dir = flask.current_app.config["CACHE_DIR"]
    for qid in qids:
        cache_filename = os.path.join(cache_dir, qid + ".json")
        if os.path.exists(cache_filename):
            try:
                entity = json.load(open(cache_filename))
            except json.decoder.JSONDecodeError:
                missing.append(qid)
            else:
                items.append(entity)
        else:
            missing.append(qid)

    for cur in chunk(missing, 50):
        r = api_call({"action": "wbgetentities", "ids": "|".join(cur)})
        reply = r.json()
        if "entities" not in reply:
            print(json.dumps(reply, indent=2))
        assert "entities" in reply
        for qid, entity in reply["entities"].items():
            cache_filename = os.path.join(cache_dir, qid + ".json")
            with open(cache_filename, "w") as f:
                json.dump(entity, f, indent=2)
            items.append(entity)
    return items
