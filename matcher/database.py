import typing

import Flask
import sqlalchemy
from sqlalchemy import DATETIME, create_engine, func, text
from sqlalchemy.engine import reflection
from sqlalchemy.orm import scoped_session, sessionmaker

session: sqlalchemy.orm.scoping.scoped_session = scoped_session(sessionmaker())


def init_db(db_url: str) -> None:
    """Initial database with the given URL."""
    session.configure(bind=get_engine(db_url))


def get_engine(db_url: str, echo: bool = False) -> sqlalchemy.engine.base.Engine:
    """Create an engine with the given URL."""
    return create_engine(db_url, pool_recycle=3600, echo=echo)


def get_tables() -> list[str]:
    """Get list of table names."""
    names: list[str] = reflection.Inspector.from_engine(session.bind).get_table_names()
    return names


def init_app(app: Flask, echo: bool = False) -> None:
    """Intialise application."""
    db_url = app.config["DB_URL"]
    session.configure(bind=get_engine(db_url, echo=echo))

    @app.teardown_appcontext
    def shutdown_session(exception: Exception | None = None) -> None:
        session.remove()


def get_old_place_list():
    sql = r"""
select place.place_id, place.osm_type, place.osm_id, place.added, size, display_name, state, count(changeset.id), max(place_matcher.start) as start
from place
    left outer join
changeset ON changeset.osm_id = place.osm_id and changeset.osm_type = place.osm_type
    left outer join
place_matcher ON place_matcher.osm_id = place.osm_id and place_matcher.osm_type = place.osm_type,
       (SELECT cast(substring(relname from '\d+') as integer) as place_id, pg_relation_size(C.oid) AS "size"
        FROM pg_class C
        WHERE relname like 'osm%polygon') a
where a.place_id = place.place_id and start < CURRENT_DATE - INTERVAL '2 months'
group by place.place_id, place.added, display_name, state, size order by start desc"""

    return session.bind.execute(text(sql))


def get_big_table_list():
    sql_big_polygon_tables = r"""
select place.place_id, place.osm_type, place.osm_id, place.added, size, display_name, state, count(changeset.id), max(place_matcher.start)
from place
    left outer join
changeset ON changeset.osm_id = place.osm_id and changeset.osm_type = place.osm_type
    left outer join
place_matcher ON place_matcher.osm_id = place.osm_id and place_matcher.osm_type = place.osm_type,
       (SELECT cast(substring(relname from '\d+') as integer) as place_id, pg_relation_size(C.oid) AS "size"
        FROM pg_class C
        WHERE relname like 'osm%polygon'
        ORDER BY pg_relation_size(C.oid) DESC
        LIMIT 200) a
where a.place_id = place.place_id
group by place.place_id, place.added, display_name, state, size order by size desc;"""

    engine = session.bind

    return engine.execute(text(sql_big_polygon_tables))


DateTimeFunc = sqlalchemy.sql.functions.Function[DATETIME]


def now_utc() -> DateTimeFunc:
    """Database function to return the current time in the UTC timezone."""
    return typing.cast(DateTimeFunc, func.timezone("utc", func.now(), type_=DATETIME))
