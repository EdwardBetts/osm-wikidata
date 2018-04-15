from sqlalchemy import create_engine, text
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

def get_big_table_list():
    sql_big_polygon_tables = '''
select place.place_id, place.osm_type, place.osm_id, size, display_name, state, count(changeset.id)
from place left outer join changeset ON changeset.place_id = place.place_id, (
    SELECT cast(substring(relname from '\d+') as integer) as place_id, pg_relation_size(C.oid) AS "size"
    FROM pg_class C
    WHERE relname like 'osm%polygon'
    ORDER BY pg_relation_size(C.oid) DESC
    LIMIT 100) a
where a.place_id = place.place_id group by place.place_id, display_name, state, size order by size desc;'''

    engine = session.bind

    return engine.execute(text(sql_big_polygon_tables))
