"""Job management via procrastinate."""

import json
import math
import typing
from datetime import datetime, timedelta, timezone

import procrastinate
from sqlalchemy import text

from . import database
from .place import Place, PlaceMatcher

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
      AND (
          :include_orphaned
          OR j.status = 'todo'
          OR EXISTS (
              SELECT 1
              FROM procrastinate_workers w
              WHERE w.id = j.worker_id
                AND w.last_heartbeat >= CURRENT_TIMESTAMP - INTERVAL '30 seconds'
          )
      )
    ORDER BY
        CASE WHEN j.status = 'doing' THEN 0 ELSE 1 END,
        j.priority DESC,
        e.at
    """)

_FINISH_ORPHANED_JOB_SQL = text(
    "SELECT procrastinate_finish_job_v1(:job_id, 'aborted', false)"
)


def matcher_job_priority(area_in_sq_km: float) -> int:
    """Return a Procrastinate priority that schedules smaller places first."""
    area = max(0, math.ceil(area_in_sq_km))
    return -min(area, 2_147_483_647)


async def recover_orphaned_jobs(
    job_manager: procrastinate.JobManager,
    priority_resolver: typing.Callable[[procrastinate.jobs.Job], int | None]
    | None = None,
) -> dict[str, list[int]]:
    """Recover matcher jobs abandoned by workers that stopped heartbeating.

    A lone orphan is returned to the queue. If a replacement is already queued,
    or an administrator requested an abort, the orphan is finalized instead.
    """
    recovered: dict[str, list[int]] = {"retried": [], "aborted": []}
    stalled_jobs = await job_manager.get_stalled_jobs(task_name=MATCHER_TASK_NAME)

    for stalled_job in stalled_jobs:
        assert stalled_job.id is not None
        current_jobs = list(await job_manager.list_jobs_async(id=stalled_job.id))
        if not current_jobs or current_jobs[0].status != "doing":
            continue
        current_job = current_jobs[0]

        queued_replacements = []
        if current_job.queueing_lock:
            queued_replacements = list(
                await job_manager.list_jobs_async(
                    task=MATCHER_TASK_NAME,
                    status="todo",
                    queueing_lock=current_job.queueing_lock,
                )
            )

        if current_job.abort_requested or queued_replacements:
            await job_manager.finish_job(
                current_job,
                status=procrastinate.jobs.Status.ABORTED,
                delete_job=False,
            )
            recovered["aborted"].append(current_job.id)
        else:
            priority = priority_resolver(current_job) if priority_resolver else None
            await job_manager.retry_job(current_job, priority=priority)
            recovered["retried"].append(current_job.id)

    return recovered


def matcher_job_priority_from_job(job: procrastinate.jobs.Job) -> int | None:
    """Calculate size priority for a matcher job being recovered."""
    osm_type = job.task_kwargs.get("osm_type")
    osm_id = job.task_kwargs.get("osm_id")
    place = Place.get_by_osm(osm_type, osm_id)
    return matcher_job_priority(place.area_in_sq_km) if place else None


def _display_elapsed(seconds: float) -> str:
    """Format an elapsed duration for the active-jobs page."""
    total_seconds = max(0, int(seconds))
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"


def _running_job_progress(place: Place, created_at: datetime) -> StrDict:
    """Summarize the current matcher run's JSON log."""
    # PlaceMatcher timestamps are stored as naive UTC values.
    earliest_start = (
        created_at.astimezone(timezone.utc).replace(tzinfo=None)
        - timedelta(seconds=1)
    )
    matcher_run = (
        place.matcher_runs.filter(
            PlaceMatcher.end.is_(None), PlaceMatcher.start >= earliest_start
        )
        .order_by(PlaceMatcher.start.desc())
        .first()
    )
    if matcher_run is None or not matcher_run.log_exists():
        return {
            "stage": "Starting",
            "detail": "Waiting for the matcher pipeline to report progress",
            "elapsed": None,
            "log_url": None,
        }

    stage = "Starting"
    detail = "Matcher started"
    elapsed: float | None = None
    chunks_done = 0

    with open(matcher_run.log_full_filename) as log_file:
        for line in log_file:
            try:
                message = json.loads(line)
            except (json.JSONDecodeError, TypeError):
                continue

            message_type = message.get("type")
            if isinstance(message.get("time"), (int, float)):
                elapsed = message["time"]

            if message_type == "get_wikidata_items":
                stage = "Fetch Wikidata items"
                detail = "Fetching items from Wikidata"
            elif message_type == "items_saved":
                stage = "Load item details"
                detail = "Wikidata items saved; loading details"
            elif message_type == "wikidata_chunk":
                stage = "Load item details"
                detail = f"Loading Wikidata detail chunk {message.get('chunk', '?')}"
            elif message_type == "wikidata_chunk_done":
                stage = "Load item details"
                detail = (
                    f"Loaded Wikidata detail chunk "
                    f"{message.get('chunk_num', 0) + 1}"
                )
            elif message_type == "get_chunk":
                stage = "Download OSM data"
                detail = f"Downloading OSM chunk {message.get('chunk_num', 0) + 1}"
            elif message_type == "chunk_done":
                stage = "Download OSM data"
                chunks_done += 1
                detail = f"Downloaded {chunks_done} OSM data chunk(s)"
            elif message_type == "overpass_done":
                stage = "Import OSM data"
                detail = "OSM download complete; preparing import"
            elif message_type == "matching_start":
                stage = "Find matches"
                total = message.get("total")
                detail = f"Finding matches for {total:,} items" if total else "Finding matches"
            elif message_type == "matching_progress":
                stage = "Find matches"
                detail = (
                    f"Matched {message.get('num', 0):,} of "
                    f"{message.get('total', 0):,} items"
                )
            elif message_type == "item" and message.get("msg"):
                detail = message["msg"]
            elif message_type == "retry_wait":
                detail = (
                    f"{message.get('service', 'External service')} "
                    f"{message.get('reason', 'unavailable')}; retrying in "
                    f"{message.get('delay', '?')}s"
                )
            elif message_type == "msg" and message.get("msg"):
                detail = message["msg"]
                if "running osm2pgsql" in detail:
                    stage = "Import OSM data"
                elif "osm2pgsql done" in detail:
                    stage = "Find matches"
            elif message_type == "error" and message.get("msg"):
                detail = message["msg"]

    return {
        "stage": stage,
        "detail": detail,
        "elapsed": _display_elapsed(elapsed) if elapsed is not None else None,
        "log_url": matcher_run.log_url(),
    }


def get_jobs(*, include_orphaned: bool = False) -> list[StrDict]:
    """Return matcher jobs suitable for the admin UI.

    Queued jobs are active until a worker claims them. A doing job is only active
    while its owning worker has a current heartbeat. Callers performing cleanup
    can request orphaned doing jobs explicitly.
    """
    # Import lazily: importing model before place has finished initializing creates
    # a model -> matcher -> model cycle in the standalone worker entry point.
    from .model import User

    rows = database.session.execute(
        _ACTIVE_JOBS_SQL,
        {
            "task_name": MATCHER_TASK_NAME,
            "include_orphaned": include_orphaned,
        },
    ).fetchall()

    job_list = []
    for queue_index, row in enumerate(rows):
        args = row.args
        osm_type = args.get("osm_type")
        osm_id = args.get("osm_id")
        user_id = args.get("user_id")
        user = User.query.get(user_id) if user_id else None
        place = Place.get_by_osm(osm_type, osm_id)
        if row.status == "doing":
            progress = _running_job_progress(place, row.created_at)
        else:
            jobs_ahead = queue_index
            progress = {
                "stage": "Queued",
                "detail": (
                    "Next job to run"
                    if jobs_ahead == 0
                    else f"{jobs_ahead} job(s) ahead"
                ),
                "elapsed": None,
                "log_url": None,
            }
        job_list.append(
            {
                "id": row.id,
                "osm_type": osm_type,
                "osm_id": osm_id,
                "place": place,
                "user": user,
                "user_id": user_id,
                "remote_addr": args.get("remote_addr"),
                "start": row.created_at,
                "status": row.status,
                "stopping": row.abort_requested,
                "worker_id": row.worker_id,
                "subscribers": row.subscribers,
                "progress": progress,
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
        for job in get_jobs(include_orphaned=True)
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
