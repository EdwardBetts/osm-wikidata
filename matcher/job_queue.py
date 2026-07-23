"""Job queue."""

import html
import json
import os.path
import re
import subprocess
import traceback
import typing
from datetime import datetime, timedelta, timezone
from time import sleep, time

import lxml.etree
import psycopg2
import requests.exceptions
from sqlalchemy import text

from matcher import database, mail, model, overpass, space_alert, wikidata_api, wikipedia
from matcher.place import Place, PlaceMatcher, bbox_chunk
from matcher.view import app

re_point = re.compile(r"^Point\(([-E0-9.]+) ([-E0-9.]+)\)$")

NOTIFY_MAX_BYTES = 7900  # PostgreSQL NOTIFY payload limit is 8000 bytes
WIKIDATA_ITEMS_MAX_AGE = timedelta(hours=24)  # reuse cached items within this window
OVERPASS_RETRY_LIMIT = 5
OVERPASS_RETRY_BASE_SECONDS = 60
OVERPASS_RETRY_MAX_SECONDS = 300


class Chunk(typing.TypedDict):
    """Chunk."""

    filename: str
    num: int
    oql: str


def overpass_chunk_filename(chunk: Chunk) -> str:
    """Filename for overpass chunk."""
    return os.path.join(app.config["OVERPASS_DIR"], chunk["filename"])


def error_in_overpass_chunk(filename: str) -> bool:
    """Error present in overpass chunk."""
    if os.path.getsize(filename) >= 2000:
        return False
    content = open(filename).read()
    return "<remark> runtime error" in content or "<!DOCTYPE html" in content


def overpass_response_excerpt(text: str, max_length: int = 300) -> str:
    """Return a compact, readable excerpt from an Overpass error response."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) <= max_length:
        return text
    return text[: max_length - 3].rstrip() + "..."


def overpass_response_error_message(r: requests.models.Response) -> str:
    """Build a concise user-facing message for a failed Overpass response."""
    status = f"{r.status_code} {r.reason}".strip()
    parts = [
        "Can't access Overpass API.",
        f"Status URL: {r.url}.",
        f"HTTP status: {status}.",
    ]

    if r.headers.get("content-type"):
        parts.append(f"Content type: {r.headers['content-type']}.")

    excerpt = overpass_response_excerpt(r.text)
    if excerpt:
        parts.append(f"Response: {excerpt}")

    return " ".join(parts)


def build_item_list(items):
    item_list = []
    for qid, v in items.items():
        label = v["query_label"]
        enwiki = v.get("enwiki")
        if enwiki and not enwiki.startswith(label + ","):
            label = enwiki
        m = re_point.match(v["location"])
        if not m:
            print(qid, label, enwiki, v["location"])
        assert m
        lon, lat = map(float, m.groups())
        item = {"qid": qid, "label": label, "lat": lat, "lon": lon}
        if "tags" in v:
            item["tags"] = list(v["tags"])
        item_list.append(item)
    return item_list


class MatcherJobStopped(Exception):
    pass


class MatcherJobFailed(Exception):
    """A matcher job failed after reporting a useful message to the user."""


class MatcherJob:
    """Matcher job."""

    def __init__(
        self,
        osm_type: str,
        osm_id: int,
        user: model.User | None = None,
        remote_addr: str | None = None,
        user_agent: str | None = None,
        want_isa: set[str] | None = None,
        status_callback: typing.Callable | None = None,
    ) -> None:
        """Init."""
        self.osm_type = osm_type
        self.osm_id = osm_id
        self.t0 = time()
        self.user_id = user
        self.remote_addr = remote_addr
        self.user_agent = user_agent
        self.want_isa = set(want_isa) if want_isa else set()
        self.place: Place | None = None
        self.log_file = None
        self._notify_conn: psycopg2.extensions.connection | None = None
        self.status_callback = status_callback

    def _get_notify_conn(self) -> psycopg2.extensions.connection:
        """Get or create the psycopg2 connection used for NOTIFY."""
        if self._notify_conn is None or self._notify_conn.closed:
            db_url = app.config["DB_URL"]
            self._notify_conn = psycopg2.connect(db_url)
            self._notify_conn.autocommit = True
        return self._notify_conn

    def close(self) -> None:
        """Close the notification connection and log file."""
        if self._notify_conn and not self._notify_conn.closed:
            self._notify_conn.close()
        self._notify_conn = None
        if self.log_file:
            self.log_file.close()
            self.log_file = None

    def _pg_notify(self, channel: str, payload: str) -> None:
        """Send a PostgreSQL NOTIFY."""
        conn = self._get_notify_conn()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_notify(%s, %s)", [channel, payload])

    def _send_chunked_pins(
        self, channel: str, pins: list, time_val: float
    ) -> None:
        """Send a large pins list in multiple NOTIFY messages."""
        chunk_size = max(1, len(pins) // 20)
        i = 0
        while i < len(pins):
            chunk = pins[i : i + chunk_size]
            payload = json.dumps(
                {"type": "pins", "pins": chunk, "time": time_val}
            )
            # Shrink chunk_size until it fits
            while len(payload) > NOTIFY_MAX_BYTES and chunk_size > 1:
                chunk_size = max(1, chunk_size // 2)
                chunk = pins[i : i + chunk_size]
                payload = json.dumps(
                    {"type": "pins", "pins": chunk, "time": time_val}
                )
            self._pg_notify(channel, payload)
            i += len(chunk)

    def send(self, msg_type: str, **data: typing.Any) -> None:
        """Send a status message via PostgreSQL NOTIFY (and optional callback)."""
        data["time"] = time() - self.t0
        data["type"] = msg_type

        if self.log_file:
            print(json.dumps(data), file=self.log_file)
            self.log_file.flush()

        if self.status_callback:
            self.status_callback(data)

        channel = f"matcher_{self.osm_type}_{self.osm_id}"
        payload = json.dumps(data)

        if len(payload) <= NOTIFY_MAX_BYTES:
            self._pg_notify(channel, payload)
        elif msg_type == "pins" and "pins" in data:
            self._send_chunked_pins(channel, data["pins"], data["time"])
        else:
            print(f"WARNING: dropping oversized notify payload for type {msg_type!r}")

    def status(self, msg: str) -> None:
        """Send a status message."""
        if msg:
            self.send("msg", msg=msg)

    def error(self, msg: str, **data: typing.Any) -> None:
        """Send an error message."""
        print(f"ERROR: {msg}")
        self.send("error", msg=msg, **data)

    def failed(self, msg: str) -> None:
        """Send terminal failure message."""
        print(f"FAILED: {msg}")
        self.send("failed", msg=msg)

    def item_line(self, msg: str) -> None:
        """Send an item progress line."""
        if msg:
            self.send("item", msg=msg)

    def end_job(self) -> None:
        """Clean up after the job finishes."""
        self.close()

    def drop_database_tables(self) -> None:
        """Drop GIS tables for this place."""
        assert self.place
        gis_tables = self.place.gis_tables
        for t in gis_tables & set(database.get_tables()):
            database.session.execute(text(f"drop table if exists {t}"))
        database.session.commit()
        assert not self.place.gis_tables & set(database.get_tables())

    def prepare_for_refresh(self, is_refresh: bool = False) -> None:
        """Prepare for refresh."""
        assert self.place
        if is_refresh:
            # User explicitly requested a fresh run — discard the cached timestamp
            # so wikidata items are re-fetched from scratch.
            self.place.wikidata_items_retrieved_at = None
        self.place.delete_overpass()
        self.place.reset_all_items_to_not_done()
        self.drop_database_tables()
        self.place.refresh_nominatim()
        database.session.commit()

    def overpass_chunk_error(self, chunk: Chunk) -> bool | None:
        """Check if an overpass chunk contains an error."""
        if not chunk["oql"]:
            return None
        filename = overpass_chunk_filename(chunk)
        if not error_in_overpass_chunk(filename):
            return None
        content = open(filename).read()
        if "<!DOCTYPE html" in content:
            if "too busy" in content:
                msg = "Overpass server too busy to handle request"
            elif "runtime error" in content:
                msg = "Overpass runtime error"
            else:
                msg = "Overpass server returned an error"
            self.error("overpass: " + msg)
            mail.send_mail("Overpass error", content)
            return True
        root = lxml.etree.parse(filename).getroot()
        remark = root.find(".//remark")
        assert remark is not None and remark.text
        self.error("overpass: " + remark.text)
        mail.send_mail("Overpass error", remark.text)
        return True

    @staticmethod
    def overpass_response_error(r: requests.models.Response) -> str | None:
        """Classify an Overpass response body that contains an error page."""
        content = r.text
        if "<!DOCTYPE html" in content:
            if "too busy" in content:
                return "too_busy"
            if "runtime error" in content:
                return "runtime_error"
            return "server_error"
        if len(r.content) >= 2000:
            return None
        if "<remark> runtime error" in content:
            return "runtime_error"
        return None

    def retry_delay(self, attempt: int) -> int:
        """Return Overpass retry delay for a 1-based attempt number."""
        return min(
            OVERPASS_RETRY_BASE_SECONDS * 2 ** (attempt - 1),
            OVERPASS_RETRY_MAX_SECONDS,
        )

    def overpass_status_wait_seconds(self) -> int | None:
        """Return seconds until an Overpass slot opens, if status provides one."""
        try:
            status = overpass.get_status()
        except (overpass.OverpassError, requests.exceptions.RequestException):
            return None

        if not status["slots"]:
            return None
        secs = status["slots"][0]
        return secs if secs > 0 else None

    def retry_wait(
        self, service: str, reason: str, delay: int, attempt: int, max_attempts: int
    ) -> None:
        """Send structured retry wait status."""
        self.send(
            "retry_wait",
            service=service,
            reason=reason,
            delay=delay,
            attempt=attempt,
            max_attempts=max_attempts,
        )

    def fetch_overpass_chunk(self, oql: str) -> requests.models.Response | None:
        """Fetch an Overpass chunk, retrying transient busy/rate-limit responses."""
        attempt = 1
        while attempt <= OVERPASS_RETRY_LIMIT:
            try:
                r = overpass.run_query(oql)
            except overpass.RateLimited:
                if not self.wait_for_slot():
                    return None
                continue
            except requests.exceptions.RequestException as e:
                self.error(
                    f"Can't access Overpass API query endpoint: {e}",
                    stage="overpass",
                )
                return None

            error_kind = self.overpass_response_error(r)
            if error_kind != "too_busy":
                return r

            if attempt == OVERPASS_RETRY_LIMIT:
                return r

            delay = self.overpass_status_wait_seconds() or self.retry_delay(attempt)
            self.status(
                "Overpass server too busy, retrying in "
                f"{delay} seconds ({attempt}/{OVERPASS_RETRY_LIMIT})"
            )
            self.retry_wait(
                service="Overpass",
                reason="server too busy",
                delay=delay,
                attempt=attempt,
                max_attempts=OVERPASS_RETRY_LIMIT,
            )
            sleep(delay)
            attempt += 1

        return None

    def wait_for_slot(self) -> bool:
        """Wait for an Overpass API slot. Returns False if Overpass is unavailable."""
        try:
            status = overpass.get_status()
        except overpass.OverpassError as e:
            r = e.r
            body = f"URL: {r.url}\n\nresponse:\n{r.text}"
            mail.send_mail("Overpass API unavailable", body)
            self.error(overpass_response_error_message(r), stage="overpass")
            return False
        except requests.exceptions.Timeout:
            url = overpass.status_url()
            mail.send_mail("Overpass API timeout", f"Timeout talking to {url}")
            self.error(
                f"Can't access Overpass API. Status URL: {url}. Timed out after 10 seconds.",
                stage="overpass",
            )
            return False
        except requests.exceptions.RequestException as e:
            url = e.request.url if e.request is not None else overpass.status_url()
            mail.send_mail("Overpass API request failed", f"{type(e).__name__}: {e}")
            self.error(
                "Can't access Overpass API. "
                f"Status URL: {url}. Request failed: {type(e).__name__}: {e}",
                stage="overpass",
            )
            return False

        if not status["slots"]:
            return True
        secs = status["slots"][0]
        if secs <= 0:
            return True
        self.status(f"waiting {secs} seconds for overpass slot")
        sleep(secs)
        return True

    def overpass_request(self, chunks: list[Chunk]) -> bool:
        """Download overpass data for all chunks."""
        assert self.place

        for num, chunk in enumerate(chunks):
            oql = chunk.get("oql")
            if not oql:
                continue
            filename = overpass_chunk_filename(chunk)
            if not os.path.exists(filename):
                space_alert.check_free_space(app.config)
                if not self.wait_for_slot():
                    return False
                self.send("get_chunk", chunk_num=num)
                r = self.fetch_overpass_chunk(oql)
                if r is None:
                    return False
                with open(filename, "wb") as out:
                    out.write(r.content)
                space_alert.check_free_space(app.config)
            self.send("chunk_done", chunk_num=num)

        self.send("overpass_done")
        return True

    def matcher(self) -> None:
        """Run matcher."""
        assert self.place
        place = self.place

        if self._wikidata_items_are_fresh():
            self.status("using existing Wikidata items from recent run")
        else:
            self.get_items()
            db_items = {item.qid: item for item in self.place.items}
            self.get_item_detail(db_items)
            self.place.wikidata_items_retrieved_at = datetime.now(timezone.utc)
            database.session.commit()

        db_items = {item.qid: item for item in self.place.items}
        item_count = len(db_items)
        self.status("{:,d} Wikidata items found".format(item_count))

        chunk_size = 96 if self.want_isa else None
        skip = {"building", "building=yes"} if self.want_isa else set()

        if place.osm_type == "node":
            oql = place.get_oql()
            chunks = [{"filename": f"{place.place_id}.xml", "num": 0, "oql": oql}]
        else:
            chunks = place.get_chunks(chunk_size=chunk_size, skip=skip)
            self.report_empty_chunks(chunks)

        overpass_good = self.overpass_request(chunks)
        if not overpass_good:
            raise MatcherJobFailed("Overpass API unavailable")
        if any(self.overpass_chunk_error(chunk) for chunk in chunks):
            raise MatcherJobFailed("Overpass returned an error response")

        if len(chunks) > 1:
            self.merge_chunks(chunks)

        self.run_osm2pgsql()
        self.load_isa()
        self.run_matcher()
        self.place.clean_up()

    def run_in_app_context(self) -> None:
        """Run the full matcher pipeline."""
        self.place = Place.get_by_osm(self.osm_type, self.osm_id)
        if not self.place:
            self.send("not_found")
            self.send("done")
            return

        if self.place.state == "ready":
            self.send("already_done")
            self.send("done")
            return

        if self.place.bad_geom_type:
            self.send(
                "error",
                msg=f"geometry is not a polygon ({self.place.geometry_type}) — the boundary is not a closed ring",
            )
            self.failed("Matcher cannot run for this geometry")
            return

        is_refresh = self.place.state == "refresh"

        user = model.User.query.get(self.user_id) if self.user_id else None

        run_obj = PlaceMatcher(
            place=self.place,
            user=user,
            remote_addr=self.remote_addr,
            user_agent=self.user_agent,
            is_refresh=is_refresh,
        )
        database.session.add(run_obj)
        database.session.flush()

        self.log_file = run_obj.open_log_for_writes()

        self.prepare_for_refresh(is_refresh=is_refresh)
        self.matcher()

        run_obj.complete()
        self.place.state = "ready"
        database.session.commit()
        print(run_obj.start, run_obj.end)

        print("sending done")
        self.send("done")
        print("done sent")

    def _wikidata_items_are_fresh(self) -> bool:
        """Return True if wikidata items were fetched recently and exist in the DB."""
        assert self.place
        retrieved_at = self.place.wikidata_items_retrieved_at
        if retrieved_at is None:
            return False
        age = datetime.now(timezone.utc) - retrieved_at.replace(tzinfo=timezone.utc)
        if age > WIKIDATA_ITEMS_MAX_AGE:
            return False
        return self.place.items.count() > 0

    def handle_rate_limited(self, exc: wikidata_api.QueryRateLimited) -> None:
        """Handle a 429 rate-limit response from the Wikidata Query Service."""
        retry_after = exc.retry_after
        msg = f"Wikidata rate limited, waiting {retry_after} seconds before retrying"
        print(msg)
        self.status(msg)
        sleep(retry_after)

    def wikidata_chunked(self, chunks):
        assert self.place
        items = {}
        num = 0
        while chunks:
            bbox = chunks.pop()
            num += 1
            msg = f"requesting wikidata chunk {num}"
            print(msg)
            self.status(msg)
            try:
                items.update(
                    self.place.bbox_wikidata_items(bbox, want_isa=self.want_isa)
                )
            except wikidata_api.QueryTimeout:
                msg = f"wikidata timeout, splitting chunk {num} into four"
                print(msg)
                self.status(msg)
                chunks += bbox_chunk(bbox, 2)
            except wikidata_api.QueryRateLimited as e:
                chunks.append(bbox)  # put back to retry after waiting
                num -= 1
                self.handle_rate_limited(e)

        return items

    def get_items(self):
        assert self.place
        self.send("get_wikidata_items")

        if self.place.is_point:
            wikidata_items = self.get_items_point()
        else:
            wikidata_items = self.get_items_bbox()

        self.status("wikidata query complete")
        pins = build_item_list(wikidata_items)
        self.send("pins", pins=pins)

        self.send("load_cat")
        wikipedia.add_enwiki_categories(wikidata_items)
        self.send("load_cat_done")

        self.place.save_items(wikidata_items)
        self.send("items_saved")

    def get_items_point(self):
        assert self.place
        return self.place.point_wikidata_items()

    def get_items_bbox(self):
        assert self.place
        place = self.place
        if self.want_isa:
            size = 220
        else:
            size = 22
        chunk_size = place.wikidata_chunk_size(size=size)
        if chunk_size == 1:
            print("wikidata unchunked")
            while True:
                try:
                    wikidata_items = place.bbox_wikidata_items(want_isa=self.want_isa)
                    break
                except wikidata_api.QueryTimeout:
                    place.wikidata_query_timeout = True
                    database.session.commit()
                    chunk_size = 2
                    msg = "wikidata query timeout, retrying with smaller chunks."
                    self.status(msg)
                    break
                except wikidata_api.QueryRateLimited as e:
                    self.handle_rate_limited(e)

        if chunk_size != 1:
            chunks = list(place.polygon_chunk(size=size))
            msg = f"downloading wikidata in {len(chunks)} chunks"
            self.status(msg)
            wikidata_items = self.wikidata_chunked(chunks)

        return wikidata_items

    def get_item_detail(self, db_items):
        def extracts_progress(item):
            msg = "load extracts: " + item.label_and_qid()
            self.item_line(msg)

        print("getting wikidata item details")
        assert self.place
        self.status("getting wikidata item details")
        for qid, entity in wikidata_api.entity_iter(db_items.keys()):
            item = db_items[qid]
            item.entity = entity
            msg = "load entity: " + item.label_and_qid()
            print(msg)
            self.item_line(msg)
        self.item_line("wikidata entities loaded")

        self.status("loading wikipedia extracts")
        self.place.load_extracts(progress=extracts_progress)
        self.item_line("extracts loaded")

    def report_empty_chunks(self, chunks: list[Chunk]) -> None:
        """Report empty chunks to user."""
        empty = [chunk["num"] for chunk in chunks if not chunk["oql"]]
        if empty:
            self.send("empty", empty=empty)

    def merge_chunks(self, chunks: list[Chunk]) -> None:
        """Merge chunks using osmium."""
        assert self.place

        files = [
            os.path.join("overpass", chunk["filename"])
            for chunk in chunks
            if chunk.get("oql")
        ]

        cmd = ["osmium", "merge"] + files + ["-o", self.place.overpass_filename]
        p = subprocess.run(
            cmd,
            encoding="utf-8",
            universal_newlines=True,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        msg = p.stdout if p.returncode == 0 else p.stderr
        if msg:
            self.status(msg)

    def run_osm2pgsql(self) -> None:
        """Run osm2pgsql."""
        assert self.place
        self.status("running osm2pgsql")
        cmd = self.place.osm2pgsql_cmd()
        env = {"PGPASSWORD": app.config["DB_PASS"]}
        subprocess.run(cmd, env=env, check=True)
        print("osm2pgsql done")
        self.status("osm2pgsql done")

    def load_isa(self) -> None:
        """Load IsA data."""

        def progress(msg: str) -> None:
            self.status(msg)

        assert self.place
        self.status("downloading 'instance of' data for Wikidata items")
        self.place.load_isa(progress)
        self.status("Wikidata 'instance of' download complete")

    def run_matcher(self) -> None:
        """Run the matcher."""
        assert self.place
        total = self.place.items.count()
        self.send("matching_start", total=total)
        checked = 0

        def progress(candidates, item):
            nonlocal checked
            checked += 1
            num = len(candidates)
            noun = "candidate" if num == 1 else "candidates"
            count = f": {num} {noun} found"
            msg = item.label_and_qid() + count
            self.item_line(msg)
            self.send("matching_progress", num=checked, total=total)

        self.place.run_matcher(progress=progress, want_isa=self.want_isa)
