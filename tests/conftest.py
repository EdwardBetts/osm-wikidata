import pathlib
import shutil

import pytest
from flask import Flask
from sqlalchemy import text
from testing.postgresql import Postgresql
from matcher import database
from matcher.place import Place  # noqa: F401
from matcher.model import Base, Item  # noqa: F401

@pytest.fixture(scope='session')
def postgresql(request):
    psql = Postgresql()

    yield psql
    psql.stop()

@pytest.fixture(scope='session')
def config_default():
    """Materialize config/default.py for tests.

    matcher.procrastinate_app does `from config.default import DB_URL` at
    module scope, but config/default.py is gitignored and absent in tests
    and CI. Copy from config/sample.py for the duration of the session,
    then remove it if we created it.
    """
    default_py = pathlib.Path(__file__).resolve().parent.parent / 'config' / 'default.py'
    sample_py = default_py.with_name('sample.py')
    created = False
    if not default_py.exists() and sample_py.exists():
        shutil.copy(sample_py, default_py)
        created = True
    yield default_py
    if created:
        default_py.unlink()

@pytest.fixture(scope='session')
def app(request, postgresql, config_default):
    app = Flask('test_app')

    class TestConfig():
        DB_URL = postgresql.url()
        DB_PASS = ''
        TESTING = True
        DEBUG = True
        ADMIN_EMAIL = 'tests@osm.wikidata.link'
        SERVER_NAME = 'test'
        SECRET_KEY = 'secret'
        SOCIAL_AUTH_USER_MODEL = 'matcher.model.User'
        DATA_DIR = 'data'

    app.config.from_object(TestConfig)
    database.init_app(app)

    # Establish an application context before running the tests.
    ctx = app.app_context()
    ctx.push()

    # create database tables
    engine = database.session.get_bind()
    with engine.begin() as conn:
        conn.execute(text('create extension if not exists postgis'))
        conn.execute(text('create extension if not exists hstore'))
    Base.metadata.create_all(engine)

    yield app

    ctx.pop()


@pytest.fixture(scope='session')
def osm2pgsql_available():
    """True if osm2pgsql is on PATH. Integration tests skip when False."""
    return shutil.which('osm2pgsql') is not None
