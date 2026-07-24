"""Job management via procrastinate."""

import typing
from datetime import datetime, timezone

from sqlalchemy import text

from . import database
from .place import Place

StrDict = dict[str, typing.Any]

MATCHER_TASK_NAME = "matcher.run_matcher"
MATCHER_SUBSCRIBER_PREFIX = "owl_places_matcher_"

_ACTIVE_JOBS_SQL = text("""
    SELECT j.id,
           j.args,
           j.status,
           j.abort_requested,
           j.worker_id,
           (
               SELECT count(*)
               FROM pg_stat_activity a
               WHERE a.datname = current_database()
                 AND a.application_name = concat(
                     'owl_places_matcher_',
                     j.args->>'osm_type',
                     '_',
                     j.args->>'osm_id'
                 )
           ) AS subscribers,
           e.at AS created_at
    FROM procrastinate_jobs j
    JOIN procrastinate_events e
      ON e.job_id = j.id AND e.type = 'deferred'
    WHERE j.task_name = :task_name
      AND j.status IN ('todo', 'doing')
    ORDER BY e.at
    """)

_FINISH_ORPHANED_JOB_SQL = text(
    "SELECT procrastinate_finish_job_v1(:job_id, 'aborted', false)"
)


def get_jobs() -> list[StrDict]:
    """Return active matcher jobs as a list of dicts suitable for the admin UI."""
    rows = database.session.execute(
        _ACTIVE_JOBS_SQL, {"task_name": MATCHER_TASK_NAME}
    ).fetchall()

    job_list = []
    for row in rows:
        args = row.args
        osm_type = args.get("osm_type")
        osm_id = args.get("osm_id")
        place = Place.get_by_osm(osm_type, osm_id)
        job_list.append(
            {
                "id": row.id,
                "osm_type": osm_type,
                "osm_id": osm_id,
                "place": place,
                "start": row.created_at,
                "status": row.status,
                "stopping": row.abort_requested,
                "worker_id": row.worker_id,
                "subscribers": row.subscribers,
            }
        )

    return job_list


def get_job(place: Place) -> StrDict | None:
    """Return the active job for *place*, or None."""
    for job in get_jobs():
        if job["osm_type"] == place.osm_type and job["osm_id"] == place.osm_id:
            return job
    return None


def get_queue_status(osm_type: str, osm_id: int) -> StrDict | None:
    """Return queue status and the number of jobs ahead of a matcher job."""
    active_jobs = get_jobs()
    for index, job in enumerate(active_jobs):
        if job["osm_type"] == osm_type and job["osm_id"] == osm_id:
            return {
                "status": job["status"],
                "jobs_ahead": index,
            }
    return None


def subscriber_application_name(osm_type: str, osm_id: int) -> str:
    """Return the PostgreSQL application name used by matcher subscribers."""
    return f"{MATCHER_SUBSCRIBER_PREFIX}{osm_type}_{osm_id}"


def stop_job(place: Place) -> int:
    """Stop every active job for *place*, including orphaned jobs."""
    from .procrastinate_app import procrastinate_app

    place_jobs = [
        job
        for job in get_jobs()
        if job["osm_type"] == place.osm_type and job["osm_id"] == place.osm_id
    ]
    if not place_jobs:
        raise ValueError(f"No active job found for {place.osm_type}/{place.osm_id}")

    finished_orphan = False
    for job in place_jobs:
        procrastinate_app.job_manager.cancel_job_by_id(job["id"], abort=True)

        # A doing job whose worker has disappeared cannot acknowledge an abort
        # request. Finalize it here so its lock and admin entry are released.
        if job["status"] == "doing" and job["worker_id"] is None:
            database.session.execute(_FINISH_ORPHANED_JOB_SQL, {"job_id": job["id"]})
            finished_orphan = True

    if finished_orphan:
        database.session.commit()

    return len(place_jobs)
