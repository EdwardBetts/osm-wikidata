from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.engine import reflection
from social.apps.flask_app.default.models import init_social

session = scoped_session(sessionmaker())

def init_db(db_url):
    session.configure(bind=get_engine(db_url))

def get_engine(db_url, echo=False):
    return create_engine(db_url, pool_recycle=3600, echo=echo)

def get_tables():
    return reflection.Inspector.from_engine(session.bind).get_table_names()

def init_app(app, echo=False):
    db_url = app.config['DB_URL']
    session.configure(bind=get_engine(db_url, echo=echo))
    init_social(app, session)

    @app.teardown_appcontext
    def shutdown_session(exception=None):
        session.remove()
