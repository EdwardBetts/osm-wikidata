"""Tests for matcher job administration."""

from types import SimpleNamespace

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

    monkeypatch.setattr(jobs, "get_jobs", lambda: active_jobs)
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

    monkeypatch.setattr(jobs, "get_jobs", lambda: active_jobs)
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
