"""Job queue."""

import json
import os.path
import re
import subprocess
import traceback
import typing
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

    def error(self, msg: str) -> None:
        """Send an error message."""
        self.send("error", msg=msg)

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

    def prepare_for_refresh(self) -> None:
        """Prepare for refresh."""
        assert self.place
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

    def wait_for_slot(self) -> bool:
        """Wait for an Overpass API slot. Returns False if Overpass is unavailable."""
        try:
            status = overpass.get_status()
        except overpass.OverpassError as e:
            r = e.args[0]
            body = f"URL: {r.url}\n\nresponse:\n{r.text}"
            mail.send_mail("Overpass API unavailable", body)
            self.error("Can't access overpass API")
            return False
        except requests.exceptions.Timeout:
            mail.send_mail("Overpass API timeout", "Timeout talking to overpass API")
            self.error("Can't access overpass API")
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
                while True:
                    try:
                        r = overpass.run_query(oql)
                        break
                    except overpass.RateLimited:
                        self.wait_for_slot()
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

        self.get_items()
        db_items = {item.qid: item for item in self.place.items}
        item_count = len(db_items)
        self.status("{:,d} Wikidata items found".format(item_count))

        self.get_item_detail(db_items)

        chunk_size = 96 if self.want_isa else None
        skip = {"building", "building=yes"} if self.want_isa else set()

        if place.osm_type == "node":
            oql = place.get_oql()
            chunks = [{"filename": f"{place.place_id}.xml", "num": 0, "oql": oql}]
        else:
            chunks = place.get_chunks(chunk_size=chunk_size, skip=skip)
            self.report_empty_chunks(chunks)

        overpass_good = self.overpass_request(chunks)
        assert overpass_good
        if any(self.overpass_chunk_error(chunk) for chunk in chunks):
            return None

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

        self.prepare_for_refresh()
        self.matcher()

        run_obj.complete()
        self.place.state = "ready"
        database.session.commit()
        print(run_obj.start, run_obj.end)

        print("sending done")
        self.send("done")
        print("done sent")

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
        ctx = app.test_request_context()
        ctx.push()
        place = self.place
        if self.want_isa:
            size = 220
        else:
            size = 22
        chunk_size = place.wikidata_chunk_size(size=size)
        if chunk_size == 1:
            print("wikidata unchunked")
            try:
                wikidata_items = place.bbox_wikidata_items(want_isa=self.want_isa)
            except wikidata_api.QueryTimeout:
                place.wikidata_query_timeout = True
                database.session.commit()
                chunk_size = 2
                msg = "wikidata query timeout, retrying with smaller chunks."
                self.status(msg)

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

        def progress(candidates, item):
            num = len(candidates)
            noun = "candidate" if num == 1 else "candidates"
            count = f": {num} {noun} found"
            msg = item.label_and_qid() + count
            self.item_line(msg)

        assert self.place
        self.place.run_matcher(progress=progress, want_isa=self.want_isa)
