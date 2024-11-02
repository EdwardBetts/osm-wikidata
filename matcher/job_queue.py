"""Job queue."""

import collections
import json
import os.path
import queue
import re
import subprocess
import threading
import traceback
import typing
from datetime import datetime
from time import time

import lxml.etree
from sqlalchemy import text

from matcher import database, mail, model, wikidata_api, wikipedia
from matcher.place import Place, PlaceMatcher, bbox_chunk
from matcher.view import app

re_point = re.compile(r"^Point\(([-E0-9.]+) ([-E0-9.]+)\)$")


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
    return (
        os.path.getsize(filename) < 2000
        and "<remark> runtime error" in open(filename).read()
    )


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


class MatcherJob(threading.Thread):
    """Matcher job within the matcher queue."""

    def __init__(
        self,
        osm_type: str,
        osm_id: int,
        job_manager: "JobManager",
        user: model.User | None = None,
        remote_addr: str | None = None,
        user_agent: str | None = None,
        want_isa: set[str] | None = None,
    ) -> None:
        """Init."""
        super(MatcherJob, self).__init__()
        self.osm_type = osm_type
        self.osm_id = osm_id
        self.start_time = time()
        self.subscribers = {}
        self.t0 = time()
        self.name = f"{osm_type}/{osm_id}  {self.t0}"
        self.user_id = user
        self.remote_addr = remote_addr
        self.user_agent = user_agent
        self.want_isa = set(want_isa) if want_isa else set()
        self._stop_event = threading.Event()
        self.job_manager = job_manager  # dependency injection
        self.log_file = None

    def end_job(self) -> None:
        """End the job."""
        if self.log_file:
            self.log_file.close()
        assert self.job_manager
        self.job_manager.end_job(self.osm_type, self.osm_id)

    def stop(self) -> None:
        """Stop job."""
        self._stop_event.set()

    @property
    def stopping(self) -> bool:
        """Job is stopping."""
        return self._stop_event.is_set()

    def check_for_stop(self) -> None:
        """Check if job is meant to stop."""
        if self._stop_event.is_set():
            raise MatcherJobStopped

    def drop_database_tables(self) -> None:
        """Drop database tables."""
        assert self.place
        gis_tables = self.place.gis_tables
        for t in gis_tables & set(database.get_tables()):
            database.session.execute(text(f"drop table if exists {t}"))
        database.session.commit()
        # make sure all GIS tables for this place have been removed
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
        """Overpass chunk contains error."""
        if not chunk["oql"]:
            return None  # empty chunk
        filename = overpass_chunk_filename(chunk)
        if not error_in_overpass_chunk(filename):
            return None
        self.check_for_stop()
        root = lxml.etree.parse(filename).getroot()
        remark = root.find(".//remark")
        assert remark is not None and remark.text
        self.error("overpass: " + remark.text)
        mail.send_mail("Overpass error", remark.text)
        return True  # FIXME report error to admin

    def matcher(self) -> None:
        """Run matcher."""
        assert self.place
        place = self.place

        self.get_items()
        db_items = {item.qid: item for item in self.place.items}
        item_count = len(db_items)
        self.status("{:,d} Wikidata items found".format(item_count))

        self.check_for_stop()
        self.get_item_detail(db_items)

        chunk_size = 96 if self.want_isa else None
        skip = {"building", "building=yes"} if self.want_isa else set()

        if place.osm_type == "node":
            oql = place.get_oql()
            chunks = [{"filename": f"{place.place_id}.xml", "num": 0, "oql": oql}]
        else:
            chunks = place.get_chunks(chunk_size=chunk_size, skip=skip)
            self.report_empty_chunks(chunks)
        self.check_for_stop()

        overpass_good = self.overpass_request(chunks)
        assert overpass_good
        self.check_for_stop()
        if any(self.overpass_chunk_error(chunk) for chunk in chunks):
            return None  # FIXME report error to admin

        if len(chunks) > 1:
            self.merge_chunks(chunks)

        self.check_for_stop()
        self.run_osm2pgsql()
        self.check_for_stop()
        self.load_isa()
        self.check_for_stop()
        self.run_matcher()
        self.check_for_stop()
        self.place.clean_up()

    def run_in_app_context(self) -> None:
        """Run matcher in app context."""
        self.place = Place.get_by_osm(self.osm_type, self.osm_id)
        if not self.place:
            self.send("not_found")
            self.send("done")
            self.end_job()
            return

        if self.place.state == "ready":
            self.send("already_done")
            self.send("done")
            self.end_job()
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
        self.end_job()

    def run(self):
        with app.app_context():
            try:
                self.run_in_app_context()
            except Exception as e:
                error_str = f"{type(e).__name__}: {e}"
                print(error_str)
                self.send("error", msg=error_str)
                self.end_job()

                info = "matcher queue"
                mail.send_traceback(info, prefix="matcher queue")
                traceback.print_exc()

        print("end thread:", self.name)

    def send(self, msg_type: str, **data: typing.Any) -> None:
        """Send message."""
        data["time"] = time() - self.t0
        data["type"] = msg_type
        for status_queue in self.subscribers.values():
            status_queue.put(data)

        if not self.log_file:
            return None
        print(json.dumps(data), file=self.log_file)

    def status(self, msg: str) -> None:
        """Send status update."""
        if msg:
            self.send("msg", msg=msg)

    def error(self, msg: str) -> None:
        """Send error message."""
        self.send("error", msg=msg)

    def item_line(self, msg: str) -> None:
        """Item line."""
        if msg:
            self.send("item", msg=msg)

    @property
    def subscriber_count(self) -> int:
        """Return subscriber count."""
        return len(self.subscribers)

    def subscribe(self, thread_name: str, status_queue):
        """Subscribe."""
        msg = {
            "time": time() - self.t0,
            "type": "connected",
        }
        status_queue.put(msg)
        print("subscribe", self.name)
        self.subscribers[thread_name] = status_queue
        return status_queue

    def unsubscribe(self, thread_name: str) -> None:
        """Unsubscribe."""
        del self.subscribers[thread_name]

    def wikidata_chunked(self, chunks):
        assert self.place
        items = {}
        num = 0
        while chunks:
            self.check_for_stop()
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

        self.check_for_stop()

        self.status("wikidata query complete")
        pins = build_item_list(wikidata_items)
        self.send("pins", pins=pins)

        self.check_for_stop()

        self.send("load_cat")
        wikipedia.add_enwiki_categories(wikidata_items)
        self.send("load_cat_done")

        self.check_for_stop()

        self.place.save_items(wikidata_items)
        self.send("items_saved")

    def get_items_point(self):
        assert self.place
        return self.place.point_wikidata_items()

    def get_items_bbox(self):
        assert self.place
        ctx = app.test_request_context()
        ctx.push()  # to make url_for work
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

    def overpass_request(self, chunks: list[Chunk]) -> bool:
        assert self.place
        send_queue = queue.Queue()

        fields = ["place_id", "osm_id", "osm_type", "area"]
        msg = {
            "place": {f: getattr(self.place, f) for f in fields},
            "chunks": chunks,
        }

        try:
            area = float(self.place.area)
        except ValueError:
            area = 0

        self.job_manager.task_queue.put(
            (
                area,
                {
                    "place": self.place,
                    "chunks": chunks,
                    "queue": send_queue,
                },
            )
        )

        complete = False
        while True:
            print("read from send queue")
            msg = send_queue.get()
            print("read complete")
            if msg is None:
                print("done (msg is None)")
                break
            print("message type {}".format(repr(msg["type"])))
            if msg["type"] == "run_query":
                chunk_num = msg["num"]
                self.send("get_chunk", chunk_num=chunk_num)
            elif msg["type"] == "chunk":
                chunk_num = msg["num"]
                self.send("chunk_done", chunk_num=chunk_num)
            elif msg["type"] == "done":
                complete = True
                self.send("overpass_done")
                break
            elif msg["type"] == "error":
                self.error(msg["msg"])
            else:
                self.status("from network: " + repr(msg))
        return complete

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
            """Progress update."""
            self.status(msg)
            self.check_for_stop()

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
            self.check_for_stop()

        assert self.place
        self.place.run_matcher(progress=progress, want_isa=self.want_isa)


Task = tuple[float, dict[str, typing.Any]]


class JobManager:
    """Job manager."""

    def __init__(self) -> None:
        """Init."""
        self.active_jobs: dict[tuple[str, int], MatcherJob] = {}
        self.task_queue: queue.PriorityQueue[Task] = queue.PriorityQueue()

    def end_job(self, osm_type: str, osm_id: int) -> None:
        """End job."""
        del self.active_jobs[(osm_type, osm_id)]

    def get_job(self, osm_type: str, osm_id: int) -> MatcherJob | None:
        """Get job."""
        return self.active_jobs.get((osm_type, osm_id))

    def get_next_job(self) -> MatcherJob:
        """Get next job."""
        return self.task_queue.get()

    def new_job(self, osm_type: str, osm_id: int, **kwargs: typing.Any) -> MatcherJob:
        """Add new job."""
        job = MatcherJob(osm_type, osm_id, job_manager=self, **kwargs)
        self.active_jobs[(osm_type, osm_id)] = job
        return job

    def iter_jobs(self) -> collections.abc.Iterator[MatcherJob]:
        """Iterate through jobs."""
        return (job for job in threading.enumerate() if isinstance(job, MatcherJob))

    def stop_job(self, osm_type: str, osm_id: int) -> None:
        """Stop given job."""
        for job in self.iter_jobs():
            if job.osm_type == osm_type and job.osm_id == osm_id:
                job.stop()

    def job_list(self) -> list[dict[str, typing.Any]]:
        """Get job list."""
        return [
            {
                "osm_id": job.osm_id,
                "osm_type": job.osm_type,
                "subscribers": job.subscriber_count,
                "start": str(datetime.utcfromtimestamp(int(job.start_time))),
                "stopping": job.stopping,
            }
            for job in self.iter_jobs()
        ]
