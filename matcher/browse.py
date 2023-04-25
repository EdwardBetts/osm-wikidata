"""Support functions for browse interface."""

import json
import os
from datetime import datetime, timedelta
from time import time
from typing import Any, TypedDict

from . import (
    commons,
    database,
    nominatim,
    utils,
    wikidata,
    wikidata_api,
    wikidata_language,
)
from .model import IsA, WikidataItem
from .place import Place

cache_ttl = timedelta(days=1)


class Entity(TypedDict):
    """Wikidata Entity."""

    claims: dict[str, Any]


def place_from_qid(
    qid: str, q: str | None = None, entity: Entity | None = None
) -> Place | None:
    """Look up via QID and return place."""
    hit = hit_from_qid(qid, q=None, entity=None)
    return place_from_nominatim(hit) if hit else None


def hit_from_qid(
    qid: str, q: str | None = None, entity: Entity | None = None
) -> Place | None:
    """Run nominatim search and get hit with matching QID."""
    if q is None:
        if entity is None:
            entity = wikidata_api.get_entity(qid)
        q = qid_to_search_string(qid, entity)

    hits = nominatim.lookup(q=q)
    for hit in hits:
        hit_qid = hit["extratags"].get("wikidata")
        if hit_qid != qid:
            continue
        return hit

    return None


def get_isa_from_entity(entity: Entity) -> set[str]:
    """Find IsA QIDs from a Wikidata and entity."""
    isa = {
        i["mainsnak"]["datavalue"]["value"]["id"]
        for i in entity.get("claims", {}).get("P31", [])
    }
    return isa


def qid_to_search_string(qid: str, entity: Entity) -> str:
    """Build a search string."""
    isa = get_isa_from_entity(entity)

    label = wikidata.entity_label(entity)

    country_or_bigger = {
        "Q5107",  # continent
        "Q6256",  # country
        "Q484652",  # international organization
        "Q855697",  # subcontinent
        "Q3624078",  # sovereign state
        "Q1335818",  # supranational organisation
        "Q4120211",  # regional organization
    }

    if isa & country_or_bigger:
        return label

    names = wikidata.up_one_level(qid)
    if not names:
        return label
    country = names["country_name"] or names["up_country_name"]

    q = names["name"]
    if names["up"]:
        q += ", " + names["up"]
    if country and country != names["up"]:
        q += ", " + country
    return q


def place_from_nominatim(hit) -> Place | None:
    """Get place object from nominatim hit."""
    if not ("osm_type" in hit and "osm_id" in hit):
        return
    p = Place.query.filter_by(
        osm_type=hit["osm_type"], osm_id=hit["osm_id"]
    ).one_or_none()
    if p:
        p.update_from_nominatim(hit)
    else:
        p = Place.from_nominatim(hit)
        database.session.add(p)
    database.session.commit()
    return p


class BrowseDetail:
    """Details for browse page."""

    def __init__(
        self,
        item_id: int,
        timing: list[tuple[str, int]],
        lang: str | None = None,
        sort: str | None = None,
    ):
        """Object constructor."""
        self.item_id = item_id
        self.timing = timing
        self.lang = lang
        self.sort = sort
        # self.item = WikidataItem.query.get(item_id)
        self.extra_rows = []
        self.extra_type_label = None

    @property
    def qid(self) -> str:
        """Wikidata QID of top-level item."""
        return f"Q{self.item_id}"

    @property
    def entity(self) -> Entity:
        """Wikidata item entity dict."""
        return self.item.entity

    @property
    def place(self) -> Place:
        """Place for top-level item."""
        place = Place.get_by_wikidata(self.qid)
        if place:
            return place
        return place_from_qid(self.qid, entity=self.entity)

    @property
    def name(self) -> str:
        """Top-level item name."""
        return wikidata.entity_label(self.entity, language=self.lang)

    @property
    def description(self) -> str:
        """Top-level item description."""
        return wikidata.entity_description(self.entity, language=self.lang)

    def get_extra_rows(self):
        """Is there a second type of subregion we want to show on the browse page."""
        if self.qid == "Q21":  # England
            # Q48091 = region of England
            types = wikidata.next_level_types(["Q48091"])
            query = (
                wikidata.next_level_query2.replace("TYPES", types)
                .replace("QID", self.qid)
                .replace("LANGUAGE", self.lang)
            )
            self.extra_rows = wikidata.next_level_places(
                self.qid, self.entity, language=self.lang, query=query
            )
            self.extra_type_label = "Regions of England"
            return

        has_geographic_region = any("Q82794" in row["isa"] for row in self.rows)
        if has_geographic_region:
            self.extra_type_label = "Geographic regions"
            rows = []

            for row in self.rows:
                if "Q82794" not in row["isa"]:
                    rows.append(row)
                else:
                    self.extra_rows.append(row)

            self.rows = rows

    def download_missing_isa(self, download_isa):
        """Download any IsA object that aren't in the database already."""
        for isa_qid, entity in wikidata_api.entity_iter(download_isa):
            if self.isa_map[isa_qid]:
                self.isa_map[isa_qid].entity = entity
                continue
            isa_obj = IsA(item_id=isa_qid[1:], entity=entity)
            self.isa_map[isa_qid] = isa_obj
            database.session.add(isa_obj)
        database.session.commit()

    def build_isa_map(self, rows):
        """Build a map of IsA item QIDs to Wikidata objects."""
        self.isa_map: dict[str, IsA] = {}
        download_isa: set[str] = set()
        for row in rows:
            for isa_qid in row["isa"]:
                if isa_qid in self.isa_map:
                    continue
                isa_obj = IsA.query.get(isa_qid[1:])
                if isa_obj and isa_obj.entity:
                    self.isa_map[isa_qid] = isa_obj
                else:
                    download_isa.add(isa_qid)

        if download_isa:
            self.download_missing_isa(download_isa)

    def get_lang_qids_from_country(self):
        # P17 = country
        if self.lang_qids or "P17" not in self.item.entity["claims"]:
            return

        for c in self.item.entity["claims"]["P17"]:
            if "datavalue" not in c["mainsnak"]:
                continue

            if "qualifiers" in c and "P582" in c["qualifiers"]:
                continue  # end time qualifier

            country_qid = c["mainsnak"]["datavalue"]["value"]["id"]
            country_item_id = c["mainsnak"]["datavalue"]["value"]["numeric-id"]
            country = WikidataItem.query.get(country_item_id)
            if country:
                self.check_lastrevid.append((country_qid, country.rev_id))
            else:
                country = WikidataItem.download(country_item_id)
            self.items[country_qid] = country

            for lang_qid in wikidata_language.get_lang_qids(country.entity):
                if lang_qid not in self.lang_qids:
                    self.lang_qids.append(lang_qid)

    def add_langs_to_items(self):
        for lang_qid in self.lang_qids:
            lang_item_id = int(lang_qid[1:])
            lang_item = WikidataItem.query.get(lang_item_id)
            if lang_item:
                self.check_lastrevid.append((lang_qid, lang_item.rev_id))
            else:
                lang_item = WikidataItem.download(lang_item_id)
            self.items[lang_qid] = lang_item

    def update_items(self):
        if not self.check_lastrevid:
            return
        check_qids = [check_qid for check_qid, rev_id in self.check_lastrevid]
        cur_rev_ids = wikidata_api.get_lastrevids(check_qids)
        for check_qid, rev_id in self.check_lastrevid:
            if cur_rev_ids[check_qid] > rev_id:
                self.items[check_qid].update()

    def sort_rows(self) -> None:
        """Sort rows in the specified order."""
        if self.sort and self.sort in {"area", "population", "qid", "label"}:
            self.rows.sort(key=lambda i: i[self.sort] if i[self.sort] else 0)

    def add_english(self) -> None:
        """We always want to include English as a language option."""
        if self.languages and not any(
            lang.get("code") == "en" for lang in self.languages
        ):
            self.languages.append({"code": "en", "local": "English", "en": "English"})

    def get_former_type(self, isa_map) -> set[str]:
        """Which types represent historical entities."""
        return {
            isa_qid
            for isa_qid, isa in isa_map.items()
            if any(
                term in isa.entity_label().lower() for term in ("historical", "former")
            )
        }

    def add_code_to_lang(self):
        if not self.lang and self.languages:
            for lang_dict in self.languages:
                if "code" not in lang_dict:
                    continue
                self.lang = lang_dict["code"]
                break

    def get_languages(self):
        lang_items = (self.items[lang_qid].entity for lang_qid in self.lang_qids)
        self.languages = wikidata_language.process_language_entities(lang_items)

    def get_rows_with_cache(self):
        """Call Wikidata Query service to get next-level rows, cache the results."""
        cache_path = utils.cache_dir()

        now = datetime.utcnow()

        filename = os.path.join(cache_path, f"{self.qid}_{self.lang}")
        self.rows = []
        if os.path.exists(filename):
            with open(filename) as f:
                json_data = json.load(f)
                timestamp = datetime.fromisoformat(json_data["timestamp"])
                if now - timestamp < cache_ttl:
                    self.rows = json_data["rows"]
        if not self.rows:
            self.rows = wikidata.next_level_places(
                self.qid, self.entity, language=self.lang
            )
            with open(filename, "w") as f:
                json.dump(
                    {"timestamp": now.isoformat(), "rows": self.rows}, f, indent=2
                )

    def details(self):
        """Return details for browse page."""
        self.check_lastrevid = []

        # top level item
        self.item = WikidataItem.query.get(self.item_id)
        self.items = {}
        if self.item:
            self.check_lastrevid.append((self.qid, self.item.rev_id))
        else:
            self.item = WikidataItem.download(self.item_id)
        self.items[self.qid] = self.item

        # try to guess languages from given item
        self.lang_qids = wikidata_language.get_lang_qids(self.item.entity)

        self.get_lang_qids_from_country()
        self.add_langs_to_items()
        self.update_items()
        self.get_languages()
        self.add_english()

        self.timing.append(("get entity done", time()))

        self.add_code_to_lang()

        if not self.lang:
            self.lang = "en"

        self.get_rows_with_cache()

        self.timing.append(("next level places done", time()))

        self.get_extra_rows()

        self.timing.append(("start isa map", time()))
        self.build_isa_map(self.rows + self.extra_rows)
        self.timing.append(("isa map done", time()))

        self.sort_rows()

        former_type = self.get_former_type(self.isa_map)

        self.current_places = [
            row for row in self.rows if not (set(row["isa"]) & former_type)
        ]
        self.former_places = [row for row in self.rows if set(row["isa"]) & former_type]


class Continent(TypedDict):
    """Dict to represent a continent."""

    label: str
    description: str
    country_count: int
    qid: str
    banner: str | None
    banner_url: str | None


def row_to_continent_dict(row: dict[str, Any]) -> Continent:
    """Convert a WDQS row into a contient item."""
    item = {
        "label": row["continentLabel"]["value"],
        "description": row["continentDescription"]["value"],
        "country_count": row["count"]["value"],
        "qid": wikidata.wd_to_qid(row["continent"]),
    }
    return item


def get_banner_images(items: list[Continent]) -> None:
    """Add banner URL to items."""
    banner_filenames = [item["banner"] for item in items if item.get("banner")]
    images = commons.image_detail(banner_filenames)
    for item in items:
        banner = item.get("banner")
        item["banner_url"] = images[banner]["url"] if banner else None


def rows_to_item_list(rows: list[dict[str, Any]]) -> list[Continent]:
    """List of WDQS rows to item list."""
    items = []
    for row in rows:
        item = row_to_continent_dict(row)
        item["banner"] = None
        try:
            filename = commons.commons_uri_to_filename(row["banner"]["value"])
            item["banner"] = filename
        except KeyError:
            pass
        items.append(item)
        row["item"] = item

    return items


def get_continents() -> list[Continent]:
    """Return details of the continents."""
    query = wikidata.continents_with_country_count_query
    rows = wikidata.run_query(query)
    items = rows_to_item_list(rows)

    get_banner_images(items)

    return items
