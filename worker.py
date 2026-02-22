#!/usr/bin/python3
"""Procrastinate worker entry point.

Run this instead of matcher_queue.py:

    python3 worker.py

Or with the procrastinate CLI using an async connector:

    procrastinate --app=matcher.procrastinate_app.procrastinate_app worker

Note: the CLI route requires setting up PG environment variables or modifying
procrastinate_app.py to use PsycopgConnector with the DB URL.
"""

import procrastinate

import matcher.tasks  # noqa: F401 - registers tasks with procrastinate_app
from matcher import database
from matcher.procrastinate_app import procrastinate_app
from matcher.view import app

app.config.from_object("config.default")
database.init_app(app, echo=False)

db_url = app.config["DB_URL"]

with procrastinate_app.replace_connector(
    procrastinate.PsycopgConnector(conninfo=db_url)
) as worker_app:
    worker_app.run_worker()
