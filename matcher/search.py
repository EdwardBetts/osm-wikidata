import re
import typing

from flask import current_app, g, request, session, url_for

from . import database, nominatim
from .model import PageBanner
from .place import Place

re_place_identifier = re.compile(r"^(node|way|relation)/(\d+)$")
re_qid = re.compile(r"^(Q\d+)$")

NominatimResults = list[dict[str, typing.Any]]


class SearchError(Exception):
    """Search error."""


class Hit:
    """Search hit."""

    osm_type: str | None
    osm_id: int | None
    display_name: str
    place: Place | None
    category: str
    type: str

    def __init__(self, d: dict[str, typing.Any]):
        """Init."""
        self.osm_type = d.get("osm_type")
        self.osm_id = d.get("osm_id")
        self.display_name = d["display_name"]
        self.place = d.get("place")
        self.category = d["category"].replace("_", " ")
        self.type = d["type"].replace("_", " ")
        self.area = d.get("area")
        self.matcher_allowed = self.place and self.place.matcher_allowed

    @property
    def name(self) -> str:
        """Name of object."""
        return self.display_name

    @property
    def osm_url(self) -> str:
        """OSM api URL."""
        return f"https://api.openstreetmap.org/{self.osm_type}/{self.osm_id}"

    @property
    def wikidata(self) -> str | None:
        """Wikidata QID for place."""
        return self.place.wikidata if self.place else None

    def banner(self) -> str | None:
        """Banner URL."""
        if not self.place or not self.place.wikidata:
            return None
        b = PageBanner.get_by_qid(self.place.wikidata)
        return b.url if b else None

    def next_level_name_search(self) -> str:
        """Next level search."""
        assert request.endpoint
        return url_for(request.endpoint, q=self.next_level_name())

    def banner_link(self) -> str:
        """Link from banner."""
        if self.ready:
            return self.url
        if self.show_browse_link():
            return self.browse_url()
        if self.matcher_allowed:
            return self.next_state_url()

        return self.next_level_name_search()

    @property
    def ready(self) -> bool:
        """Place is ready."""
        return bool(self.place and self.place.state == "ready")

    @property
    def url(self) -> str:
        """Place candidates page URL."""
        assert self.place
        return self.place.candidates_url()

    def next_state_url(self) -> str:
        assert self.place
        return self.place.next_state_url()

    def browse_url(self) -> str | None:
        """Browse URL."""
        assert self.place
        return self.place.browse_url()

    def next_level_name(self) -> str:
        terms = self.display_name.split(", ")
        return ", ".join(terms[1:])

    def show_browse_link(self) -> bool:
        """We show the browse link along with the matcher link."""
        min_area = current_app.config.get("BROWSE_LINK_MIN_AREA", 0)
        return bool(self.wikidata and self.area and self.area > min_area)

    def show_browse_link_instead(self) -> bool:
        """Show the browse link instead of the matcher link."""
        config = current_app.config

        place_max_area = (
            config["PLACE_MAX_AREA"]
            if g.user.is_authenticated
            else config["PLACE_MAX_AREA_ANON"]
        )

        return bool(
            self.wikidata
            and (
                (self.area and self.area >= place_max_area)
                or (self.place and self.place.too_complex)
            )
        )

    @property
    def disallowed_cat(self) -> bool:
        """Place is in a disallowed category."""
        return bool(self.place and not self.place.allowed_cat)

    def reason_matcher_not_allowed(self) -> str | None:
        if self.osm_type == "node":
            return "matcher only works with relations and ways, not with nodes"
        config = current_app.config

        if self.matcher_allowed:
            return None

        if self.place and not self.place.allowed_cat:
            return "matcher only works with place or boundary"

        if self.osm_type not in ("way", "relation") or not self.area:
            return None

        if self.area >= config["PLACE_MAX_AREA"]:
            return "area too large for matcher"
        elif self.area < config["PLACE_MIN_AREA"]:
            return "area too small for matcher"

        if self.place and self.place.too_complex:
            return "place boundary is too complex for matcher"

        return None


def convert_hits_to_objects(results: NominatimResults) -> list[Hit]:
    """Convert results into list of hit objects."""
    return [Hit(hit) for hit in results if "osm_type" in hit]


def update_search_results(results: NominatimResults) -> None:
    need_commit = False
    for hit in results:
        if not ("osm_type" in hit and "osm_id" in hit and "geotext" in hit):
            continue

        p = Place.query.get(hit["place_id"])
        if p and (p.osm_type != hit["osm_type"] or p.osm_id != hit["osm_id"]):
            need_commit = True
            db_place_hit = nominatim.reverse(p.osm_type, p.osm_id)
            if "error" in db_place_hit or "place_id" not in db_place_hit:
                # place deleted from OSM
                if p.osm_type == "node":
                    database.session.delete(p)
                # FIXME: mail admin if place isn't a node on OSM
            else:
                p.place_id = db_place_hit["place_id"]

        p = Place.query.filter_by(
            osm_type=hit["osm_type"], osm_id=hit["osm_id"]
        ).one_or_none()
        if p and p.place_id != hit["place_id"]:
            p.update_from_nominatim(hit)
            need_commit = True
        elif not p:
            p = Place.query.get(hit["place_id"])
            if p:
                p.update_from_nominatim(hit)
            else:
                p = Place.from_nominatim(hit)
                database.session.add(p)
            need_commit = True
    if need_commit:
        database.session.commit()


def check_for_place_identifier(q: str):
    q = q.strip()
    m = re_place_identifier.match(q)
    if not m:
        return
    osm_type, osm_id = m.groups()
    p = Place.from_osm(osm_type, int(osm_id))
    if not p:
        return

    return p.candidates_url() if p.state == "ready" else p.matcher_progress_url()


def check_for_search_identifier(q: str):
    q = q.strip()
    # if searching for a Wikidata QID then redirect to the item page for that QID
    m = re_qid.match(q)
    if m:
        return url_for("item_page", wikidata_id=m.group(1)[1:])

    return check_for_place_identifier(q)


def handle_redirect_on_single(results: NominatimResults):
    if not session.get("redirect_on_single", False):
        return

    session["redirect_on_single"] = False
    hits = [hit for hit in results if hit["osm_type"] != "node"]
    if len(hits) != 1:
        return

    hit = hits[0]
    place = Place.get_or_abort(hit["osm_type"], hit["osm_id"])
    if place:
        return place.redirect_to_matcher()


def check_for_city_node_in_results(q: str, results: NominatimResults) -> None:
    """Check for city node in results."""
    for hit_num, hit in enumerate(results):
        if hit.get("osm_type") != "node":
            continue
        if "," not in hit["display_name"]:
            continue
        name_parts = hit["display_name"].split(", ")
        node, area = name_parts[:2]
        if area not in (f"{node} City", f"City of {node}"):
            continue
        city_q = ", ".join(name_parts[1:])
        city_results = nominatim.lookup(city_q)
        if len(city_results) == 1:
            results[hit_num] = city_results[0]


def run(q: str) -> NominatimResults:
    """Run nominatim search."""
    try:
        results = nominatim.lookup(q)
        city_of = "City of "
        if q.startswith(city_of) and not results:
            q_trim = q[len(city_of) :]
            results = nominatim.lookup(q_trim)
            if results:
                return results

        check_for_city_node_in_results(q, results)
    except nominatim.SearchError:
        raise SearchError

    return results
