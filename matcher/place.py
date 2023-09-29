"""Place model."""

import json
import os.path
import re
import subprocess
from collections import Counter
from time import time

import user_agents
from flask import abort, current_app, g, redirect, url_for
from geoalchemy2 import Geography, Geometry
from sqlalchemy import cast, func, select
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import (
    backref,
    column_property,
    deferred,
    load_only,
    object_session,
    relationship,
)
from sqlalchemy.orm.exc import MultipleResultsFound
from sqlalchemy.schema import Column, ForeignKey, ForeignKeyConstraint, UniqueConstraint
from sqlalchemy.sql.expression import false, or_, true
from sqlalchemy.types import JSON, BigInteger, Boolean, DateTime, Float, Integer, String

from . import (
    default_change_comments,
    matcher,
    nominatim,
    overpass,
    utils,
    wikidata,
    wikidata_api,
    wikipedia,
)
from .database import get_tables, now_utc, session
from .model import (
    Base,
    Changeset,
    IsA,
    Item,
    ItemCandidate,
    ItemTag,
    PlaceItem,
    get_bad,
    osm_type_enum,
)
from .overpass import oql_from_tag

radius_default = 1_000  # in metres, only for nodes

place_chunk_size = 32
degrees = "(-?[0-9.]+)"
re_box = re.compile(rf"^BOX\({degrees} {degrees},{degrees} {degrees}\)$")
re_geonames_spring = re.compile(r"^\d[0-9A-Z_]{13} Spring$")

base_osm_url = "https://api.openstreetmap.org"

overpass_types = {"way": "way", "relation": "rel", "node": "node"}

skip_tags = {
    "route:road",
    "highway=primary",
    "highway=road",
    "highway=service",
    "highway=motorway",
    "highway=trunk",
    "highway=unclassified",
    "highway",
    "name",
    "website",
    "type=waterway",
    "waterway=river" "addr:street",
    "type=associatedStreet",
    "amenity",
}


def drop_building_tag(tags: list[str]) -> None:
    """Drop builing tags.

    Building is a very generic tag so remove it if we have more specific search criteria
    """
    if "building" in tags or "building=yes" in tags:
        without_buildings = [t for t in tags if t not in ("building", "building=yes")]
        if without_buildings:
            tags.discard("building")
            tags.discard("building=yes")


BBox = tuple[float, float, float, float]


def bbox_chunk(bbox: BBox, n: int) -> list[BBox]:
    """Split bounding box into chunks."""
    n = max(1, n)
    (south, north, west, east) = bbox
    ns = (north - south) / n
    ew = (east - west) / n

    chunks = []
    for row in range(n):
        for col in range(n):
            chunk = (
                south + ns * row,
                south + ns * (row + 1),
                west + ew * col,
                west + ew * (col + 1),
            )
            chunks.append(chunk)
    return chunks


def envelope(bbox):
    # note: different order for coordinates, xmin first, not ymin
    ymin, ymax, xmin, xmax = bbox
    return func.ST_MakeEnvelope(xmin, ymin, xmax, ymax, 4326)


class Place(Base):
    """Place model."""

    __tablename__ = "place"
    place_id = Column(BigInteger, primary_key=True, autoincrement=False)
    osm_type = Column(osm_type_enum, nullable=False)
    osm_id = Column(BigInteger, nullable=False)
    radius = Column(Integer)  # in metres, only for nodes
    display_name = Column(String, nullable=False)
    category = Column(String, nullable=False)
    type = Column(String, nullable=False)
    place_rank = Column(Integer, nullable=False)
    icon = Column(String)
    geom = Column(Geography(spatial_index=True))
    south = Column(Float, nullable=False)
    west = Column(Float, nullable=False)
    north = Column(Float, nullable=False)
    east = Column(Float, nullable=False)
    extratags = deferred(Column(JSON))
    address = deferred(Column(JSON))
    namedetails = deferred(Column(JSON))
    item_count = Column(Integer)
    candidate_count = Column(Integer)
    state = Column(String, index=True)
    override_name = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    added = Column(DateTime, default=now_utc())
    wikidata_query_timeout = Column(Boolean, default=False)
    wikidata = Column(String)
    item_types_retrieved = Column(Boolean, default=False)
    index_hide = Column(Boolean, default=False)
    overpass_is_in = deferred(Column(JSON))
    existing_wikidata = deferred(Column(JSON))
    language_count = Column(JSON)
    match_cache = deferred(Column(JSON))

    area = column_property(func.ST_Area(geom))
    geometry_type = column_property(func.GeometryType(geom))
    geojson = column_property(func.ST_AsGeoJSON(geom, 4), deferred=True)
    srid = column_property(func.ST_SRID(geom))
    npoints = column_property(func.ST_NPoints(cast(geom, Geometry)), deferred=True)
    # match_ratio = column_property(candidate_count / item_count)
    num_geom = column_property(
        func.ST_NumGeometries(cast(geom, Geometry)), deferred=True
    )

    items = relationship(
        "Item",
        secondary="place_item",
        lazy="dynamic",
        back_populates="places",
    )

    __table_args__ = (UniqueConstraint("osm_type", "osm_id"),)

    @property
    def osm_url(self) -> str:
        """OSM URL."""
        return f"{base_osm_url}/{self.osm_type}/{self.osm_id}"

    @classmethod
    def get_by_osm(cls, osm_type: str, osm_id: int):
        """Get place by OSM type and ID."""
        return cls.query.filter_by(osm_type=osm_type, osm_id=osm_id).one_or_none()

    @classmethod
    def from_osm(cls, osm_type: str, osm_id: int):
        place = cls.get_by_osm(osm_type, osm_id)
        if place:
            return place

        try:
            hit = nominatim.reverse(osm_type, osm_id)
        except nominatim.SearchError:
            return
        place = Place.from_nominatim(hit)
        if place:
            session.add(place)
            session.commit()
        return place

    @property
    def type_label(self):
        t = self.type.replace("_", " ")
        cat = self.category.replace("_", " ")
        if cat == "place":
            return t
        if t == "yes":
            return cat
        return t + " " + cat

    @classmethod
    def get_by_wikidata(cls, qid):
        q = cls.query.filter_by(wikidata=qid)
        try:
            return q.one_or_none()
        except MultipleResultsFound:
            return None

    def get_address_key(self, key):
        if isinstance(self.address, dict):
            return self.address.get(key)
        for line in self.address or []:
            if line["type"] == key:
                return line["name"]

    @property
    def country_code(self):
        return self.get_address_key("country_code")

    @property
    def country(self):
        return self.get_address_key("country")

    @classmethod
    def get_or_abort(cls, osm_type, osm_id):
        place = cls.get_by_osm(osm_type, osm_id)
        if place:
            return place
        abort(404)

    @hybrid_property
    def area_in_sq_km(self):
        return self.area / (1000 * 1000)

    @property
    def type_and_id(self):
        return (self.osm_type, self.osm_id)

    @property
    def too_big(self):
        max_area = current_app.config["PLACE_MAX_AREA"]
        return self.area_in_sq_km > max_area

    @property
    def too_complex(self):
        return self.npoints > current_app.config["PLACE_MAX_NPOINTS"]

    @property
    def bad_geom_type(self):
        return self.geometry_type in {"LINESTRING", "MULTILINESTRING"}

    @property
    def area_in_range(self):
        min_area = current_app.config["PLACE_MIN_AREA"]
        if g.user.is_authenticated:
            max_area = current_app.config["PLACE_MAX_AREA"]
        else:
            max_area = current_app.config["PLACE_MAX_AREA_ANON"]

        return min_area < self.area_in_sq_km < max_area

    @property
    def allowed_cat(self):
        cats = {"place", "boundary", "natural", "leisure", "amenity", "landuse"}
        return self.category in cats

    @property
    def matcher_allowed(self):
        """Are we allowed to run the matcher for this place?"""

        allow_node = bool(current_app.config.get("ALLOW_NODE_MATCH"))
        if self.osm_type == "node":
            return allow_node
        return (
            not self.bad_geom_type
            and self.allowed_cat
            and self.area_in_range
            and not self.too_complex
        )

    def update_from_nominatim(self, hit):
        assert "error" not in hit
        if self.place_id != int(hit["place_id"]):
            print((self.place_id, hit["place_id"]))
            self.place_id = hit["place_id"]

        keys = (
            "lat",
            "lon",
            "display_name",
            "place_rank",
            "category",
            "type",
            "icon",
            "extratags",
            "namedetails",
        )
        assert all(hit[n] is not None for n in ("lat", "lon"))
        for n in keys:
            setattr(self, n, hit.get(n))
        bbox = hit["boundingbox"]
        assert all(i is not None for i in bbox)
        (self.south, self.north, self.west, self.east) = bbox
        self.address = [{"name": n, "type": t} for t, n in hit["address"].items()]
        self.wikidata = (
            hit["extratags"].get("wikidata") if hit.get("extratags") else None
        )
        self.geom = hit["geotext"]

    def change_comment(self, item_count, isa_labels=None):
        if item_count == 1:
            return g.user.single or default_change_comments["single"]

        if isa_labels:
            isa = ", ".join(isa_labels[:-2] + [" and ".join(isa_labels[-2:])])
            comment = f"Add wikidata tags to {isa} in PLACE."
            # maximum length of changeset comment is 255 characters
            if len(comment) < 255:
                return comment.replace("PLACE", self.name_for_change_comment)

        comment = getattr(g.user, "multi", None) or default_change_comments["multi"]
        return comment.replace("PLACE", self.name_for_change_comment)

    @property
    def name_for_changeset(self):
        address = self.address
        n = self.name
        if not address:
            return self.name
        if isinstance(address, list):
            d = {a["type"]: a["name"] for a in address}
        elif isinstance(address, dict):
            d = address

        if d.get("country_code") == "us":
            state = d.get("state")
            if state and n != state:
                return n + ", " + state

        country = d.get("country")
        if country and self.name != country:
            return "{} ({})".format(self.name, country)

        return self.name

    def update_address(self):
        hit = nominatim.reverse(self.osm_type, self.osm_id, polygon_text=0)
        self.address = [dict(name=n, type=t) for t, n in hit["address"].items()]
        session.commit()

    @property
    def name_for_change_comment(self):
        n = self.name
        first_part = n.lower()

        if self.address:
            if isinstance(self.address, dict):
                self.update_address()

            address = {a["type"]: a["name"] for a in self.address}

            parts = []
            country_code = address.get("country_code")
            skip = {"country_code", "postcode"}
            if country_code in {"us"}:
                skip.add("county")
            if country_code in {"gb", "us"} and "state" in address:
                skip.add("country")
            if self.type in {"university", "hospital", "administrative"}:
                skip |= {"path", "footway", "road", "neighbourhood"}
            if (
                country_code == "gb"
                and self.category == "boundary"
                and self.type in {"traditional", "ceremonial", "historic"}
            ):
                parts = [
                    a for a in self.address if a["type"] in {"state_district", "state"}
                ]
            else:
                parts = [a for a in self.address if a["type"] not in skip]

            name_parts = [n]
            prev_part = n
            for part in parts:
                if part["name"] == prev_part or (
                    part["type"] != "city"
                    and (part["name"] in prev_part or prev_part in part["name"])
                ):
                    continue
                name_parts.append(part["name"])
                prev_part = part["name"]

            n = ", ".join(name_parts)
            first_part = name_parts[0].lower()
        if " of " in first_part or "national park" in first_part:
            return "the " + n
        else:
            return n

    @classmethod
    def from_nominatim(cls, hit):
        assert "error" not in hit
        keys = (
            "place_id",
            "osm_type",
            "osm_id",
            "lat",
            "lon",
            "display_name",
            "place_rank",
            "category",
            "type",
            "icon",
            "extratags",
            "namedetails",
        )
        n = {k: hit[k] for k in keys if k in hit}
        bbox = hit["boundingbox"]
        (n["south"], n["north"], n["west"], n["east"]) = bbox
        n["geom"] = hit["geotext"]
        n["address"] = [dict(name=n, type=t) for t, n in hit["address"].items()]
        if hit.get("extratags"):
            n["wikidata"] = hit["extratags"].get("wikidata")
        return cls(**n)

    @classmethod
    def get_or_add_place(cls, hit):
        place = cls.query.filter_by(
            osm_type=hit["osm_type"], osm_id=hit["osm_id"]
        ).one_or_none()

        if place and place.place_id != hit["place_id"]:
            place.update_from_nominatim(hit)
        elif not place:
            place = Place.query.get(hit["place_id"])
            if place:
                place.update_from_nominatim(hit)
            else:
                place = cls.from_nominatim(hit)
                session.add(place)
        session.commit()
        return place

    @property
    def match_ratio(self):
        if self.item_count:
            return self.candidate_count / self.item_count

    @property
    def bbox(self):
        return (self.south, self.north, self.west, self.east)

    @property
    def is_point(self):
        return self.osm_type == "node"

    @property
    def display_area(self):
        return "{:,.1f} km²".format(self.area_in_sq_km)

    def get_wikidata_query(self):
        # this is an old function, it isn't used by the matcher
        if self.osm_type == "node":
            radius = self.radius or radius_default
            query = wikidata.get_point_query(self.lat, self.lon, radius)
        else:
            query = wikidata.get_enwiki_query(*self.bbox)
        return query

    def point_wikidata_items(self):
        radius = self.radius or radius_default
        query_map = wikidata.point_query_map(self.lat, self.lon, radius)
        return self.items_from_wikidata(query_map)

    def bbox_wikidata_items(self, bbox=None, want_isa=None):
        if bbox is None:
            bbox = self.bbox

        query_map = wikidata.bbox_query_map(*bbox, want_isa=want_isa)
        items = self.items_from_wikidata(query_map, want_isa=want_isa)

        # Would be nice to include OSM chunk information with each
        # item. Not doing it at this point because it means lots
        # of queries. Easier once the items are loaded into the database.
        return {
            k: v
            for k, v in items.items()
            if self.covers(v) and not re_geonames_spring.match(v["query_label"])
        }

    def items_from_wikidata(self, query_map, want_isa=None):
        if not want_isa:
            rows = wikidata.run_query(query_map["enwiki"])
            items = wikidata.parse_enwiki_query(rows)

            try:  # add items with the coordinates in the HQ field
                rows = wikidata.run_query(query_map["hq_enwiki"])
                items.update(wikidata.parse_enwiki_query(rows))
            except wikidata_api.QueryError:
                pass  # HQ query timeout isn't fatal
        else:
            items = {}

        rows = wikidata.run_query(query_map["item_tag"])
        wikidata.parse_item_tag_query(rows, items)

        try:  # add items with the coordinates in the HQ field
            rows = wikidata.run_query(query_map["hq_item_tag"])
            wikidata.parse_item_tag_query(rows, items)
        except wikidata_api.QueryError:
            pass  # HQ query timeout isn't fatal

        return items

    def covers(self, item):
        """Is the given item within the geometry of this place."""
        q = select([func.ST_Covers(Place.geom, item["location"])]).where(
            Place.place_id == self.place_id
        )
        return object_session(self).scalar(q)

    def add_tags_to_items(self):
        for item in self.items.filter(Item.categories != "{}"):
            # if wikidata says this is a place then adding tags
            # from wikipedia can just confuse things
            if any(t.startswith("place") for t in item.tags):
                continue
            for t in matcher.categories_to_tags(item.categories):
                item.tags.add(t)

    @property
    def prefix(self):
        return f"osm_{self.place_id}"

    @property
    def gis_tables(self):
        return {f"{self.prefix}_{t}" for t in ("line", "point", "polygon")}

    @property
    def identifier(self):
        return f"{self.osm_type}/{self.osm_id}"

    @property
    def overpass_filename(self):
        overpass_dir = current_app.config["OVERPASS_DIR"]
        return os.path.join(overpass_dir, "{}.xml".format(self.place_id))

    def is_overpass_filename(self, f):
        """Does the overpass filename belongs to this place."""
        place_id = str(self.place_id)
        return f == place_id + ".xml" or f.startswith(place_id + "_")

    def delete_overpass(self):
        for f in os.scandir(current_app.config["OVERPASS_DIR"]):
            if self.is_overpass_filename(f.name):
                os.remove(f.path)

    def clean_up(self):
        if current_app.config.get("DO_CLEAN_UP") is False:
            return
        place_id = self.place_id

        engine = session.bind
        for t in get_tables():
            if not t.startswith(self.prefix):
                continue
            engine.execute(f"drop table if exists {t}")
        engine.execute("commit")

        overpass_dir = current_app.config["OVERPASS_DIR"]
        for f in os.listdir(overpass_dir):
            if not any(f.startswith(str(place_id) + end) for end in ("_", ".")):
                continue
            os.remove(os.path.join(overpass_dir, f))

    @property
    def overpass_done(self):
        return os.path.exists(self.overpass_filename)

    def items_with_candidates(self):
        return self.items.join(ItemCandidate)

    def items_with_candidates_count(self):
        if self.state != "ready":
            return
        return (
            session.query(Item.item_id)
            .join(PlaceItem)
            .join(Place)
            .join(ItemCandidate)
            .filter(Place.place_id == self.place_id)
            .group_by(Item.item_id)
            .count()
        )

    def items_without_candidates(self):
        return self.items.outerjoin(ItemCandidate).filter(
            ItemCandidate.item_id.is_(None)
        )

    def items_with_multiple_candidates(self):
        # select count(*) from (select 1 from item, item_candidate where item.item_id=item_candidate.item_id) x;
        q = (
            self.items.join(ItemCandidate)
            .group_by(Item.item_id)
            .having(func.count(Item.item_id) > 1)
            .with_entities(Item.item_id)
        )
        return q

    @property
    def name(self):
        if self.override_name:
            return self.override_name

        name = self.namedetails.get("name:en") or self.namedetails.get("name")
        display = self.display_name
        if not name:
            return display

        for short in ("City", "1st district"):
            start = len(short) + 2
            if (
                name == short
                and display.startswith(short + ", ")
                and ", " in display[start:]
            ):
                name = display[: display.find(", ", start)]
                break

        return name

    @property
    def name_extra_detail(self):
        for n in "name:en", "name":
            if n not in self.namedetails:
                continue
            start = self.namedetails[n] + ", "
            if self.display_name.startswith(start):
                return self.display_name[len(start) :]

    @property
    def export_name(self):
        return self.name.replace(":", "").replace(" ", "_")

    def items_with_instanceof(self):
        return [item for item in self.items if item.instanceof()]

    def osm2pgsql_cmd(self, filename=None):
        if filename is None:
            filename = self.overpass_filename
        style = os.path.join(current_app.config["DATA_DIR"], "matcher.style")
        return [
            "osm2pgsql",
            "--create",
            "--slim",
            "--drop",
            "--hstore-all",
            "--hstore-add-index",
            "--prefix",
            self.prefix,
            "--cache",
            "500",
            "--style",
            style,
            "--multi-geometry",
            "--host",
            current_app.config["DB_HOST"],
            "--username",
            current_app.config["DB_USER"],
            "--database",
            current_app.config["DB_NAME"],
            filename,
        ]

    def load_into_pgsql(self, filename=None, capture_stderr=True):
        if filename is None:
            filename = self.overpass_filename

        if not os.path.exists(filename):
            return "no data from overpass to load with osm2pgsql"

        if os.stat(filename).st_size == 0:
            return "no data from overpass to load with osm2pgsql"

        cmd = self.osm2pgsql_cmd(filename)

        if not capture_stderr:
            p = subprocess.run(cmd, env={"PGPASSWORD": current_app.config["DB_PASS"]})
            return
        p = subprocess.run(
            cmd,
            stderr=subprocess.PIPE,
            env={"PGPASSWORD": current_app.config["DB_PASS"]},
        )
        if p.returncode != 0:
            if b"Out of memory" in p.stderr:
                return "out of memory"
            else:
                return p.stderr.decode("utf-8")

    def save_overpass(self, content):
        with open(self.overpass_filename, "wb") as out:
            out.write(content)

    @property
    def all_tags(self):
        tags = set()
        for item in self.items:
            tags |= set(item.tags)
            tags |= item.get_extra_tags()
            tags |= item.disused_tags()
        tags.difference_update(skip_tags)
        return matcher.simplify_tags(tags)

    @property
    def overpass_type(self):
        return overpass_types[self.osm_type]

    @property
    def overpass_filter(self):
        return "around:{0.radius},{0.lat},{0.lon}".format(self)

    @property
    def wikidata_item_id(self):
        if self.wikidata:
            return int(self.wikidata[1:])

    def building_names(self):
        re_paren = re.compile(r"\(.+\)")
        re_drop = re.compile(r"\b(the|and|at|of|de|le|la|les|von)\b")
        names = set()
        for building in (item for item in self.items if "building" in item.tags):
            for n in building.names():
                if n[0].isdigit() and "," in n:
                    continue
                n = n.lower()
                comma = n.rfind(", ")
                if comma != -1 and not n[0].isdigit():
                    n = n[:comma]

                n = re_paren.sub("", n).replace("'s", "('s)?")
                n = n.replace("(", "").replace(")", "").replace(".", r"\.")
                names.add(n)
                names.add(re_drop.sub("", n))

        names = sorted(n.replace(" ", r"\W*") for n in names)
        if names:
            return "({})".format("|".join(names))

    def get_point_oql(self, buildings_special=False):
        tags = self.all_tags

        if buildings_special and "building" in tags:
            buildings = self.building_names()
            tags.remove("building")
        else:
            buildings = None

        radius = self.radius or radius_default
        return overpass.oql_for_point(self.lat, self.lon, radius, tags, buildings)

    def get_bbox_oql(self, buildings_special=False):
        bbox = f"{self.south:f},{self.west:f},{self.north:f},{self.east:f}"

        tags = self.all_tags

        if buildings_special and "building" in tags:
            buildings = self.building_names()
            tags.remove("building")
        else:
            buildings = None

        return overpass.oql_for_area(
            self.overpass_type, self.osm_id, tags, bbox, buildings
        )

        union = ["{}({});".format(self.overpass_type, self.osm_id)]

        for tag in self.all_tags:
            u = (
                oql_from_tag(tag, filters=self.overpass_filter)
                if self.osm_type == "node"
                else oql_from_tag(tag)
            )
            if u:
                union += u

        if self.osm_type == "node":
            oql = (
                "[timeout:300][out:xml];\n" + "({});\n" + "(._;>;);\n" + "out qt;"
            ).format("".join(union))
            return oql

        bbox = "{:f},{:f},{:f},{:f}".format(
            self.south, self.west, self.north, self.east
        )
        offset = {"way": 2400000000, "relation": 3600000000}
        area_id = offset[self.osm_type] + int(self.osm_id)

        oql = (
            "[timeout:300][out:xml][bbox:{}];\n"
            + "area({})->.a;\n"
            + "({});\n"
            + "(._;>;);\n"
            + "out qt;"
        ).format(bbox, area_id, "".join(union))
        return oql

    def get_oql(self, buildings_special=False):
        if self.is_point:
            return self.get_point_oql(buildings_special=False)
        else:
            return self.get_bbox_oql(buildings_special=False)

    def candidates_url(self, **kwargs):
        return self.place_url("candidates", **kwargs)

    def place_url(self, endpoint, **kwargs):
        return url_for(endpoint, osm_type=self.osm_type, osm_id=self.osm_id, **kwargs)

    def browse_url(self):
        if self.wikidata:
            return url_for("browse_page", item_id=self.wikidata_item_id)

    def next_state_url(self):
        return (
            self.candidates_url()
            if self.state == "ready"
            else self.matcher_progress_url()
        )

    def matcher_progress_url(self):
        return self.place_url("matcher.matcher_progress")

    def matcher_done_url(self, start):
        return self.place_url("matcher.matcher_done", start=start)

    def redirect_to_matcher(self):
        return redirect(self.matcher_progress_url())

    def item_list(self):
        lang = self.most_common_language() or "en"
        q = self.items.filter(Item.entity.isnot(None)).order_by(Item.item_id)
        return [{"id": i.item_id, "name": i.label(lang=lang)} for i in q]

    def save_items(self, items, debug=None):
        if debug is None:

            def debug(msg):
                pass

        debug("save items")
        seen = {}
        for qid, v in items.items():
            wikidata_id = int(qid[1:])
            item = Item.query.get(wikidata_id)

            debug(f"saving: {qid}")

            if item:
                item.location = v["location"]
            else:
                item = Item(item_id=wikidata_id, location=v["location"])
                session.add(item)
            for k in "enwiki", "categories", "query_label":
                if k in v:
                    setattr(item, k, v[k])

            tags = set(v["tags"])
            # if wikidata says this is a place then adding tags
            # from wikipedia can just confuse things
            # Wikipedia articles sometimes combine a village and a windmill
            # or a neighbourhood and a light rail station.
            # Exception for place tags, we always add place tags from
            # Wikipedia categories.
            if "categories" in v:
                is_place = any(t.startswith("place") for t in tags)
                for t in matcher.categories_to_tags(v["categories"]):
                    if t.startswith("place") or not is_place:
                        tags.add(t)

            # drop_building_tag(tags)

            tags -= skip_tags

            item.tags = tags
            if qid in seen:
                continue

            seen[qid] = item

            existing = PlaceItem.query.filter_by(item=item, place=self).one_or_none()
            if not existing:
                place_item = PlaceItem(item=item, place=self)
                session.add(place_item)
            debug(f"saved: {qid}")

        for item in self.items:
            if item.qid in seen:
                continue
            link = PlaceItem.query.filter_by(item=item, place=self).one()
            session.delete(link)
        debug("done")

        return seen

    def load_items(self, bbox=None, debug=False):
        if bbox is None:
            bbox = self.bbox

        items = self.bbox_wikidata_items(bbox)
        if debug:
            print("{:d} items".format(len(items)))

        wikipedia.add_enwiki_categories(items)

        self.save_items(items)

        session.commit()

    def load_extracts(self, debug=False, progress=None):
        for code, _ in self.languages_wikidata():
            self.load_extracts_wiki(debug=debug, progress=progress, code=code)

    def load_extracts_wiki(self, debug=False, progress=None, code="en"):
        wiki = code + "wiki"
        by_title = {
            item.sitelinks()[wiki]["title"]: item
            for item in self.items
            if wiki in (item.sitelinks() or {})
        }

        query_iter = wikipedia.get_extracts(by_title.keys(), code=code)
        for title, extract in query_iter:
            item = by_title[title]
            if debug:
                print(title)
            item.extracts[wiki] = extract
            if wiki == "enwiki":
                item.extract_names = wikipedia.html_names(extract)
            if progress:
                progress(item)

    def wbgetentities(self, debug=False):
        sub = (
            session.query(Item.item_id).join(ItemTag).group_by(Item.item_id).subquery()
        )
        q = self.items.filter(Item.item_id == sub.c.item_id).options(
            load_only(Item.qid)
        )

        if debug:
            print("running wbgetentities query")
            print(q)
            print(q.count())
        items = {i.qid: i for i in q}
        if debug:
            print("{} items".format(len(items)))

        for qid, entity in wikidata_api.entity_iter(items.keys(), debug=debug):
            if debug:
                print(qid)
            items[qid].entity = entity

    def languages_osm(self):
        lang_count = Counter()

        candidate_count = 0
        candidate_has_language_count = 0
        for c in self.items_with_candidates().with_entities(ItemCandidate):
            candidate_count += 1
            candidate_has_language = False
            for lang in c.languages():
                lang_count[lang] += 1
                candidate_has_language = True
            if candidate_has_language:
                candidate_has_language_count += 1

        return sorted(lang_count.items(), key=lambda i: i[1], reverse=True)

    def languages_wikidata(self):
        lang_count = Counter()
        item_count = self.items.count()
        count_sv = self.country_code in {"se", "fi"}

        for item in self.items:
            if item.entity and "labels" in item.entity:
                keys = item.entity["labels"].keys()
                if not count_sv and keys == {"ceb", "sv"}:
                    continue
                for lang in keys:
                    if "-" in lang or lang == "ceb":
                        continue
                    lang_count[lang] += 1

        if item_count > 10:
            # truncate the long tail of languages
            lang_count = {
                key: count
                for key, count in lang_count.items()
                if key == "en" or count / item_count > 0.1
            }

        if self.country_code == "us":
            lang_count = {
                key: count for key, count in lang_count.items() if key in {"en", "es"}
            }

        if self.country_code == "gb":
            lang_count = {
                key: count
                for key, count in lang_count.items()
                if key in {"en", "fr", "de", "cy"}
            }

        return sorted(lang_count.items(), key=lambda i: i[1], reverse=True)[:10]

    def languages(self):
        if self.language_count:
            return self.language_count

        wikidata = self.languages_wikidata()
        osm = dict(self.languages_osm())

        count = [
            {"code": code, "wikidata": count, "osm": osm.get(code)}
            for code, count in wikidata
        ]
        self.language_count = count
        session.commit()
        return count

    def most_common_language(self):
        lang_count = Counter()
        for item in self.items:
            if item.entity and "labels" in item.entity:
                for lang in item.entity["labels"].keys():
                    lang_count[lang] += 1
        try:
            return lang_count.most_common(1)[0][0]
        except IndexError:
            return None

    def reset_all_items_to_not_done(self):
        place_items = (
            PlaceItem.query.join(Item)
            .filter(
                Item.entity.isnot(None),
                PlaceItem.place == self,
                PlaceItem.done == true(),
            )
            .order_by(PlaceItem.item_id)
        )

        for place_item in place_items:
            place_item.done = False
        session.commit()

    def matcher_query(self):
        return (
            PlaceItem.query.join(Item)
            .filter(
                Item.entity.isnot(None),
                PlaceItem.place == self,
                or_(PlaceItem.done.is_(None), PlaceItem.done != true()),
            )
            .order_by(PlaceItem.item_id)
        )

    def run_matcher(self, debug=False, progress=None, want_isa=None):
        if want_isa is None:
            want_isa = set()
        if progress is None:

            def progress(candidates, item):
                pass

        conn = session.bind.raw_connection()
        cur = conn.cursor()

        self.existing_wikidata = matcher.get_existing(cur, self.prefix)

        place_items = self.matcher_query()
        total = place_items.count()
        # too many items means something has gone wrong
        assert total < 200_000
        for num, place_item in enumerate(place_items):
            item = place_item.item

            if debug:
                print("searching for", item.label())
                print(item.tags)

            item_isa_set = set(item.instanceof())
            skip_item = want_isa and not (item_isa_set & want_isa)

            if skip_item and item.skip_item_during_match():
                candidates = []
            else:
                t0 = time()
                candidates = matcher.find_item_matches(
                    cur, item, self.prefix, debug=debug
                )
                seconds = time() - t0
                if debug:
                    print("find_item_matches took {:.1f}".format(seconds))
                    print("{}: {}".format(len(candidates), item.label()))

            progress(candidates, item)

            # if this is a refresh we remove candidates that no longer match
            as_set = {(i["osm_type"], i["osm_id"]) for i in candidates}
            for c in item.candidates[:]:
                if c.edits.count():
                    continue  # foreign keys mean we can't remove saved candidates
                if (c.osm_type, c.osm_id) not in as_set:
                    c.bad_matches.delete()
                    session.delete(c)

            if not candidates:
                continue

            for i in candidates:
                c = ItemCandidate.query.get((item.item_id, i["osm_id"], i["osm_type"]))
                if c:
                    c.update(i)
                else:
                    c = ItemCandidate(**i, item=item)
                    session.add(c)

            place_item.done = True

            if num % 100 == 0:
                session.commit()

        self.item_count = self.items.count()
        self.candidate_count = self.items_with_candidates_count()
        session.commit()

        conn.close()

    def load_isa(self, progress=None):
        if progress is None:

            def progress(msg):
                pass

        isa_map = {
            item.qid: [isa_qid for isa_qid in item.instanceof()] for item in self.items
        }
        isa_map = {qid: l for qid, l in isa_map.items() if l}

        if not isa_map:
            return

        download_isa = set()
        isa_obj_map = {}
        for qid, isa_list in isa_map.items():
            isa_objects = []
            # some Wikidata items feature two 'instance of' statements that point to
            # the same item.
            # Example: Cambridge University Museum of Zoology (Q5025605)
            # https://www.wikidata.org/wiki/Q5025605
            seen_isa_qid = set()
            for isa_qid in isa_list:
                if isa_qid in seen_isa_qid:
                    continue
                seen_isa_qid.add(isa_qid)
                item_id = int(isa_qid[1:])
                isa = IsA.query.get(item_id)
                if not isa or not isa.entity:
                    download_isa.add(isa_qid)
                if not isa:
                    isa = IsA(item_id=item_id)
                    session.add(isa)
                isa_obj_map[isa_qid] = isa
                isa_objects.append(isa)
            item = Item.query.get(qid[1:])
            item.isa = isa_objects

        for qid, entity in wikidata_api.entity_iter(download_isa):
            isa_obj_map[qid].entity = entity

        session.commit()

    def do_match(self, debug=True):
        if self.state == "ready":  # already done
            return

        if not self.state or self.state == "refresh":
            print("load items")
            self.load_items()  # includes categories
            self.state = "tags"
            session.commit()

        if self.state == "tags":
            print("wbgetentities")
            self.wbgetentities(debug=debug)
            print("load extracts")
            self.load_extracts(debug=debug)
            self.state = "wbgetentities"
            session.commit()

        if self.state in ("wbgetentities", "overpass_error", "overpass_timeout"):
            print("loading_overpass")
            self.get_overpass()
            self.state = "postgis"
            session.commit()

        if self.state == "postgis":
            print("running osm2pgsql")
            self.load_into_pgsql(capture_stderr=False)
            self.state = "osm2pgsql"
            session.commit()

        if self.state == "osm2pgsql":
            print("run matcher")
            self.run_matcher(debug=debug)
            self.state = "load_isa"
            session.commit()

        if self.state == "load_isa":
            print("load isa")
            self.load_isa()
            print("ready")
            self.state = "ready"
            session.commit()

    def get_overpass(self):
        oql = self.get_oql()
        if self.area_in_sq_km < 800:
            r = overpass.run_query_persistent(oql)
            assert r
            self.save_overpass(r.content)
        else:
            self.chunk()

    def get_items(self):
        items = [
            item
            for item in self.items_with_candidates()
            if all("wikidata" not in c.tags for c in item.candidates)
        ]

        filter_list = matcher.filter_candidates_more(items, bad=get_bad(items))
        add_tags = []
        for item, match in filter_list:
            picked = match.get("candidate")
            if not picked:
                continue
            dist = picked.dist
            intersection = set()
            for k, v in picked.tags.items():
                tag = k + "=" + v
                if k in item.tags or tag in item.tags:
                    intersection.add(tag)
            if dist < 400:
                symbol = "+"
            elif dist < 4000 and intersection == {"place=island"}:
                symbol = "+"
            elif dist < 3000 and intersection == {"natural=wetland"}:
                symbol = "+"
            elif dist < 2000 and intersection == {"natural=beach"}:
                symbol = "+"
            elif dist < 2000 and intersection == {"natural=bay"}:
                symbol = "+"
            elif dist < 2000 and intersection == {"aeroway=aerodrome"}:
                symbol = "+"
            elif dist < 1000 and intersection == {"amenity=school"}:
                symbol = "+"
            elif dist < 800 and intersection == {"leisure=park"}:
                symbol = "+"
            elif dist < 2000 and intersection == {"landuse=reservoir"}:
                symbol = "+"
            elif dist < 3000 and item.tags == {"place", "admin_level"}:
                symbol = "+"
            elif dist < 3000 and item.tags == {"place", "place=town", "admin_level"}:
                symbol = "+"
            elif (
                dist < 3000
                and item.tags == {"admin_level", "place", "place=neighbourhood"}
                and "place" in picked.tags
            ):
                symbol = "+"
            else:
                symbol = "?"

            print(
                "{:1s}  {:9s}  {:5.0f}  {!r}  {!r}".format(
                    symbol, item.qid, picked.dist, item.tags, intersection
                )
            )
            if symbol == "+":
                add_tags.append((item, picked))
        return add_tags

    def chunk_n(self, n):
        n = max(1, n)
        (south, north, west, east) = self.bbox
        ns = (north - south) / n
        ew = (east - west) / n

        chunks = []
        for row in range(n):
            for col in range(n):
                chunk = (
                    south + ns * row,
                    south + ns * (row + 1),
                    west + ew * col,
                    west + ew * (col + 1),
                )
                want_chunk = func.ST_Intersects(Place.geom, envelope(chunk))
                want = (
                    session.query(want_chunk)
                    .filter(Place.place_id == self.place_id)
                    .scalar()
                )
                if want:
                    chunks.append(chunk)

        return chunks

    def get_chunks(self, chunk_size=None, skip=None):
        if chunk_size is None:
            chunk_size = place_chunk_size
        if skip is None:
            skip = set()
        bbox_chunks = list(self.polygon_chunk(size=chunk_size))

        chunks = []
        need_self = True  # include self in first non-empty chunk
        for num, chunk in enumerate(bbox_chunks):
            filename = self.chunk_filename(num, bbox_chunks)
            oql = self.oql_for_chunk(chunk, include_self=need_self, skip=skip)
            chunks.append(
                {
                    "num": num,
                    "oql": oql,
                    "filename": filename,
                }
            )
            if need_self and oql:
                need_self = False
        return chunks

    def chunk_filename(self, num, chunks):
        if len(chunks) == 1:
            return "{}.xml".format(self.place_id)
        return "{}_{:03d}_{:03d}.xml".format(self.place_id, num, len(chunks))

    def chunk(self):
        chunk_size = utils.calc_chunk_size(self.area_in_sq_km)
        chunks = self.chunk_n(chunk_size)

        print("chunk size:", chunk_size)

        files = []
        for num, chunk in enumerate(chunks):
            filename = self.chunk_filename(num, len(chunks))
            # print(num, q.count(), len(tags), filename, list(tags))
            full = os.path.join("overpass", filename)
            files.append(full)
            if os.path.exists(full):
                continue
            oql = self.oql_for_chunk(chunk, include_self=(num == 0))

            r = overpass.run_query_persistent(oql)
            if not r:
                print(oql)
            assert r
            open(full, "wb").write(r.content)

        cmd = ["osmium", "merge"] + files + ["-o", self.overpass_filename]
        print(" ".join(cmd))
        subprocess.run(cmd)

    def oql_for_chunk(self, chunk, include_self=False, skip=None):
        skip = set(skip or [])
        q = self.items.filter(cast(Item.location, Geometry).contained(envelope(chunk)))

        tags = set()
        for item in q:
            tags |= set(item.tags)
            tags |= item.get_extra_tags()
            tags |= item.disused_tags()
        tags.difference_update(skip_tags)
        tags.difference_update(skip)
        tags = matcher.simplify_tags(tags)
        if not (tags):
            return

        ymin, ymax, xmin, xmax = chunk
        bbox = "{:f},{:f},{:f},{:f}".format(ymin, xmin, ymax, xmax)

        oql = overpass.oql_for_area(
            self.overpass_type, self.osm_id, tags, bbox, None, include_self=include_self
        )
        return oql

    def chunk_count(self):
        return sum(1 for _ in self.polygon_chunk(size=place_chunk_size))

    def geojson_chunks(self):
        chunks = []
        for chunk in self.polygon_chunk(size=place_chunk_size):
            clip = func.ST_Intersection(Place.geom, envelope(chunk))

            geojson = (
                session.query(func.ST_AsGeoJSON(clip, 4))
                .filter(Place.place_id == self.place_id)
                .scalar()
            )

            chunks.append(geojson)
        return chunks

    def wikidata_chunk_size(self, size=22):
        if self.osm_type == "node":
            return 1

        area = self.area_in_sq_km
        if area < 3000 and not self.wikidata_query_timeout:
            return 1
        return utils.calc_chunk_size(area, size=size)

    def polygon_chunk(self, size=64):
        stmt = (
            session.query(func.ST_Dump(Place.geom.cast(Geometry())).label("x"))
            .filter_by(place_id=self.place_id)
            .subquery()
        )

        q = session.query(
            stmt.c.x.path[1],
            func.ST_Area(stmt.c.x.geom.cast(Geography)) / (1000 * 1000),
            func.Box2D(stmt.c.x.geom),
        )

        for num, area, box2d in q:
            chunk_size = utils.calc_chunk_size(area, size=size)
            west, south, east, north = map(float, re_box.match(box2d).groups())
            for chunk in bbox_chunk((south, north, west, east), chunk_size):
                yield chunk

    def latest_matcher_run(self):
        return self.matcher_runs.order_by(PlaceMatcher.start.desc()).first()

    def obj_for_json(self, include_geom=False):
        keys = [
            "osm_type",
            "osm_id",
            "display_name",
            "name",
            "extratags",
            "address",
            "namedetails",
            "state",
            "lat",
            "lon",
            "area_in_sq_km",
            "name_for_changeset",
            "name_for_change_comment",
            "bbox",
        ]
        out = {key: getattr(self, key) for key in keys}
        out["added"] = str(self.added)
        if include_geom:
            out["geom"] = json.loads(self.geojson)

        items = []
        for item in self.items:
            if not item.sitelinks():
                continue
            cur = {
                "labels": item.labels,
                "qid": item.qid,
                "url": item.wikidata_uri,
                "item_identifiers": item.get_item_identifiers(),
                "names": item.names(),
                "sitelinks": item.sitelinks(),
                "location": item.get_lat_lon(),
            }
            if item.categories:
                cur["categories"] = item.categories

            matches = [
                {
                    "osm_type": m.osm_type,
                    "osm_id": m.osm_id,
                    "dist": m.dist,
                    "label": m.label,
                }
                for m in item.candidates
            ]

            if matches:
                cur["matches"] = matches

            items.append(cur)

        out["items"] = items
        return out

    def refresh_nominatim(self):
        try:
            hit = nominatim.reverse(self.osm_type, self.osm_id)
        except nominatim.SearchError:
            return  # FIXME: mail admin
        self.update_from_nominatim(hit)
        session.commit()

    def is_in(self):
        if self.overpass_is_in:
            return self.overpass_is_in

        # self.overpass_is_in = overpass.is_in(self.overpass_type, self.osm_id)
        self.overpass_is_in = overpass.is_in_lat_lon(self.lat, self.lon)
        if self.overpass_is_in:
            session.commit()
        return self.overpass_is_in

    def suggest_larger_areas(self):
        ret = []
        for e in reversed(self.is_in() or []):
            osm_type, osm_id, bounds = e["type"], e["id"], e["bounds"]
            if osm_type == self.osm_type and osm_id == self.osm_id:
                continue

            box = func.ST_MakeEnvelope(
                bounds["minlon"],
                bounds["minlat"],
                bounds["maxlon"],
                bounds["maxlat"],
                4326,
            )

            q = func.ST_Area(box.cast(Geography))
            bbox_area = session.query(q).scalar()
            area_in_sq_km = bbox_area / (1000 * 1000)

            if area_in_sq_km < 10 or area_in_sq_km > 40_000:
                continue
            place = Place.from_osm(osm_type, osm_id)
            if not place:
                continue
            place.admin_level = (
                e["tags"].get("admin_level") or None if "tags" in e else None
            )
            ret.append(place)

        ret.sort(key=lambda place: place.area_in_sq_km)
        return ret

    def get_candidate_items(self):
        items = self.items_with_candidates()

        if self.existing_wikidata:
            existing = {
                qid: set(tuple(i) for i in osm_list)
                for qid, osm_list in self.existing_wikidata.items()
            }
        else:
            existing = {}

        items = [
            item
            for item in items
            if item.qid not in existing
            and all("wikidata" not in c.tags for c in item.candidates)
        ]

        need_commit = False
        for item in items:
            for c in item.candidates:
                if c.set_match_detail():
                    need_commit = True
        if need_commit:
            session.commit()

        return items


class PlaceMatcher(Base):
    __tablename__ = "place_matcher"
    start = Column(DateTime, default=now_utc(), primary_key=True)
    end = Column(DateTime)
    osm_type = Column(osm_type_enum, primary_key=True)
    osm_id = Column(BigInteger, primary_key=True)
    remote_addr = Column(String)
    user_id = Column(Integer, ForeignKey("user.id"))
    user_agent = Column(String)
    is_refresh = Column(Boolean, nullable=False)

    place = relationship(
        "Place",
        uselist=False,
        backref=backref(
            "matcher_runs", lazy="dynamic", order_by="PlaceMatcher.start.desc()"
        ),
    )

    user = relationship(
        "User",
        uselist=False,
        backref=backref(
            "matcher_runs", lazy="dynamic", order_by="PlaceMatcher.start.desc()"
        ),
    )

    __table_args__ = (
        ForeignKeyConstraint(
            ["osm_type", "osm_id"],
            ["place.osm_type", "place.osm_id"],
        ),
    )

    def duration(self):
        if self.end:
            return self.end - self.start

    def display_duration(self):
        if not self.end:
            return

        total_seconds = int((self.end - self.start).total_seconds())
        mins, secs = divmod(total_seconds, 60)
        hours, mins = divmod(mins, 60)
        if hours:
            return f"{hours}h {mins}m {secs}s"
        if mins:
            return f"{mins}m {secs}s"
        return f"{secs}s"

    def complete(self):
        self.end = now_utc()

    def is_bot(self):
        ua = self.user_agent
        return ua and user_agents.parse(ua).is_bot

    @property
    def log_filename(self):
        start = self.start.strftime("%Y-%m-%d_%H:%M:%S")
        return f"{self.osm_type}_{self.osm_id}_{start}.log"

    @property
    def log_full_filename(self):
        return os.path.join(utils.log_location(), self.log_filename)

    def log_exists(self):
        return os.path.exists(self.log_full_filename)

    def read_log(self):
        filename = self.log_full_filename
        if os.path.exists(filename):
            return open(filename).read()

    def open_log_for_writes(self):
        if not current_app.config.get("LOG_MATCHER_REQUESTS"):
            return

        filename = self.log_full_filename
        assert not os.path.exists(filename)
        return open(filename, "w")

    def log_url(self, endpoint="admin.view_log"):
        return url_for(
            endpoint,
            osm_type=self.osm_type,
            osm_id=self.osm_id,
            start=str(self.start).replace(" ", "_"),
        )


def get_top_existing(limit=39):
    cols = [
        Place.place_id,
        Place.display_name,
        Place.area,
        Place.state,
        Place.candidate_count,
        Place.item_count,
    ]
    c = func.count(Changeset.place_id)

    q = (
        Place.query.filter(
            Place.state.in_(["ready", "load_isa", "refresh"]),
            Place.area > 0,
            Place.index_hide == false(),
            Place.candidate_count > 4,
        )
        .options(load_only(*cols))
        .outerjoin(Changeset)
        .group_by(*cols)
        .having(c == 0)
        .order_by((Place.item_count / Place.area).desc())
    )
    return q[:limit]
