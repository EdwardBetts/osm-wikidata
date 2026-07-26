"""Tests for matcher job administration."""

import asyncio
import json
from types import SimpleNamespace

import procrastinate

from matcher import jobs, procrastinate_app


def test_stop_job_cancels_duplicates_and_finishes_orphan(monkeypatch):
    place = SimpleNamespace(osm_type="relation", osm_id=123)
    active_jobs = [
        {
            "id": 10,
            "osm_type": "relation",
            "osm_id": 123,
            "status": "doing",
            "worker_id": None,
        },
        {
            "id": 11,
            "osm_type": "relation",
            "osm_id": 123,
            "status": "todo",
            "worker_id": None,
        },
        {
            "id": 12,
            "osm_type": "relation",
            "osm_id": 456,
            "status": "todo",
            "worker_id": None,
        },
    ]
    cancelled = []
    executed = []
    commits = []

    monkeypatch.setattr(
        jobs, "get_jobs", lambda *, include_orphaned=False: active_jobs
    )
    monkeypatch.setattr(
        procrastinate_app.procrastinate_app.job_manager,
        "cancel_job_by_id",
        lambda job_id, abort: cancelled.append((job_id, abort)),
    )
    monkeypatch.setattr(
        jobs.database.session,
        "execute",
        lambda statement, parameters: executed.append(parameters),
    )
    monkeypatch.setattr(jobs.database.session, "commit", lambda: commits.append(True))

    assert jobs.stop_job(place) == 2
    assert cancelled == [(10, True), (11, True)]
    assert executed == [{"job_id": 10}]
    assert commits == [True]


def test_stop_job_leaves_live_worker_to_acknowledge_abort(monkeypatch):
    place = SimpleNamespace(osm_type="relation", osm_id=123)
    active_jobs = [
        {
            "id": 10,
            "osm_type": "relation",
            "osm_id": 123,
            "status": "doing",
            "worker_id": 29,
        }
    ]
    cancelled = []
    executed = []

    monkeypatch.setattr(
        jobs, "get_jobs", lambda *, include_orphaned=False: active_jobs
    )
    monkeypatch.setattr(
        procrastinate_app.procrastinate_app.job_manager,
        "cancel_job_by_id",
        lambda job_id, abort: cancelled.append((job_id, abort)),
    )
    monkeypatch.setattr(
        jobs.database.session,
        "execute",
        lambda statement, parameters: executed.append(parameters),
    )

    assert jobs.stop_job(place) == 1
    assert cancelled == [(10, True)]
    assert executed == []


def test_get_queue_status_reports_jobs_ahead(monkeypatch):
    active_jobs = [
        {"osm_type": "relation", "osm_id": 10, "status": "doing"},
        {"osm_type": "relation", "osm_id": 20, "status": "todo"},
        {"osm_type": "relation", "osm_id": 30, "status": "todo"},
    ]
    monkeypatch.setattr(jobs, "get_jobs", lambda: active_jobs)

    assert jobs.get_queue_status("relation", 30) == {
        "status": "todo",
        "jobs_ahead": 2,
    }
    assert jobs.get_queue_status("relation", 99) is None


def test_subscriber_application_name():
    assert (
        jobs.subscriber_application_name("relation", 51827)
        == "owl_places_matcher_relation_51827"
    )


def test_matcher_job_priority_schedules_smaller_places_first():
    assert jobs.matcher_job_priority(16.1) > jobs.matcher_job_priority(30.7)
    assert jobs.matcher_job_priority(30.7) > jobs.matcher_job_priority(10_993.7)
    assert jobs.matcher_job_priority(0.1) == -1


def test_recover_orphaned_jobs_retries_lone_job_and_aborts_duplicates():
    lone = procrastinate.jobs.Job(
        id=10,
        status="doing",
        queue="default",
        lock="matcher_relation_10",
        queueing_lock="matcher_relation_10",
        task_name=jobs.MATCHER_TASK_NAME,
    )
    duplicate = procrastinate.jobs.Job(
        id=20,
        status="doing",
        queue="default",
        lock="matcher_relation_20",
        queueing_lock="matcher_relation_20",
        task_name=jobs.MATCHER_TASK_NAME,
    )
    queued = procrastinate.jobs.Job(
        id=21,
        status="todo",
        queue="default",
        lock="matcher_relation_20",
        queueing_lock="matcher_relation_20",
        task_name=jobs.MATCHER_TASK_NAME,
    )

    class JobManager:
        def __init__(self):
            self.retried = []
            self.finished = []

        async def get_stalled_jobs(self, task_name):
            assert task_name == jobs.MATCHER_TASK_NAME
            return [lone, duplicate]

        async def list_jobs_async(
            self, id=None, task=None, status=None, queueing_lock=None
        ):
            if id is not None:
                return [lone if id == lone.id else duplicate]
            if queueing_lock == queued.queueing_lock:
                return [queued]
            return []

        async def retry_job(self, job, priority=None):
            self.retried.append((job.id, priority))

        async def finish_job(self, job, status, delete_job):
            self.finished.append((job.id, status, delete_job))

    manager = JobManager()
    recovered = asyncio.run(jobs.recover_orphaned_jobs(manager))

    assert recovered == {"retried": [10], "aborted": [20]}
    assert manager.retried == [(10, None)]
    assert manager.finished == [
        (20, procrastinate.jobs.Status.ABORTED, False)
    ]


def test_running_job_progress_summarizes_latest_pipeline_activity(
    monkeypatch, tmp_path
):
    log_file = tmp_path / "matcher.log"
    messages = [
        {"type": "get_wikidata_items", "time": 1},
        {"type": "items_saved", "time": 10},
        {"type": "get_chunk", "chunk_num": 0, "time": 20},
        {"type": "chunk_done", "chunk_num": 0, "time": 30},
        {"type": "overpass_done", "time": 31},
        {"type": "msg", "msg": "running osm2pgsql", "time": 35},
    ]
    log_file.write_text("\n".join(json.dumps(message) for message in messages))

    matcher_run = SimpleNamespace(
        log_exists=lambda: True,
        log_full_filename=str(log_file),
        log_url=lambda: "/admin/log/relation/123/start",
    )

    class Runs:
        def filter(self, *args):
            return self

        def order_by(self, *args):
            return self

        def first(self):
            return matcher_run

    place = SimpleNamespace(matcher_runs=Runs())
    progress = jobs._running_job_progress(
        place, jobs.datetime.now(jobs.timezone.utc)
    )

    assert progress == {
        "stage": "Import OSM data",
        "detail": "running osm2pgsql",
        "elapsed": "35s",
        "log_url": "/admin/log/relation/123/start",
    }
