#!/usr/bin/python3
"""Procrastinate worker entry point.

Run this instead of matcher_queue.py:

    python3 worker.py

Or with the procrastinate CLI using an async connector:

    procrastinate --app=matcher.procrastinate_app.procrastinate_app worker

Note: the CLI route requires setting up PG environment variables or modifying
procrastinate_app.py to use PsycopgConnector with the DB URL.
"""

import asyncio

import procrastinate

import matcher.tasks  # noqa: F401 - registers tasks with procrastinate_app
from matcher import database, jobs
from matcher.procrastinate_app import procrastinate_app
from matcher.view import app

app.config.from_object("config.default")
database.init_app(app, echo=False)

async def main() -> None:
    """Recover abandoned matcher jobs, then run the worker."""
    db_url = app.config["DB_URL"]

    with procrastinate_app.replace_connector(
        procrastinate.PsycopgConnector(conninfo=db_url)
    ) as worker_app:
        async with worker_app.open_async():
            with app.app_context():
                recovered = await jobs.recover_orphaned_jobs(
                    worker_app.job_manager,
                    priority_resolver=jobs.matcher_job_priority_from_job,
                )
            if recovered["retried"] or recovered["aborted"]:
                print(
                    "Recovered orphaned matcher jobs: "
                    f"retried={recovered['retried']}, "
                    f"aborted={recovered['aborted']}",
                    flush=True,
                )
            await worker_app.run_worker_async()


if __name__ == "__main__":
    asyncio.run(main())
