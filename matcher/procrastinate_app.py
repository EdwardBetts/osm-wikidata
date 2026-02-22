"""Procrastinate application."""

import procrastinate
from procrastinate.contrib.sqlalchemy import SQLAlchemyPsycopg2Connector

connector = SQLAlchemyPsycopg2Connector()

procrastinate_app = procrastinate.App(
    connector=connector,
    import_paths=["matcher.tasks"],
)
