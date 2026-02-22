"""Job management via procrastinate."""

import typing
from datetime import datetime, timezone

from sqlalchemy import text

from . import database
from .place import Place

StrDict = dict[str, typing.Any]

MATCHER_TASK_NAME = "matcher.run_matcher"

_ACTIVE_JOBS_SQL = text(
    """
    SELECT j.id,
           j.args,
           j.status,
           j.abort_requested,
           e.at AS created_at
    FROM procrastinate_jobs j
    JOIN procrastinate_events e
      ON e.job_id = j.id AND e.type = 'deferred'
    WHERE j.task_name = :task_name
      AND j.status IN ('todo', 'doing')
    ORDER BY e.at
    """
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
                "subscribers": 0,
            }
        )

    return job_list


def get_job(place: Place) -> StrDict | None:
    """Return the active job for *place*, or None."""
    for job in get_jobs():
        if job["osm_type"] == place.osm_type and job["osm_id"] == place.osm_id:
            return job
    return None


def stop_job(place: Place) -> None:
    """Request cancellation of the active job for *place*."""
    from .procrastinate_app import procrastinate_app

    job = get_job(place)
    if job is None:
        raise ValueError(f"No active job found for {place.osm_type}/{place.osm_id}")

    procrastinate_app.job_manager.cancel_job_by_id(job["id"], abort=True)
