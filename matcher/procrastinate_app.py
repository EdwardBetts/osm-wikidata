"""Procrastinate application."""

import procrastinate
from procrastinate.contrib.sqlalchemy import SQLAlchemyPsycopg2Connector

from config.default import DB_URL

connector = SQLAlchemyPsycopg2Connector()

procrastinate_app = procrastinate.App(
    connector=connector,
    import_paths=["matcher.tasks"],
)

# Async app for use with the procrastinate CLI (migrations, worker, etc.)
# Usage: python3 -m procrastinate --app=matcher.procrastinate_app.cli_app schema --migrations-path
cli_app = procrastinate.App(
    connector=procrastinate.PsycopgConnector(conninfo=DB_URL),
    import_paths=["matcher.tasks"],
)
