import pytest
from flask import Flask
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
def app(request, postgresql):
    app = Flask('test_app')

    class TestConfig():
        DB_URL = postgresql.url()
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
    engine.execute('create extension postgis')
    Base.metadata.create_all(engine)

    yield app

    ctx.pop()
