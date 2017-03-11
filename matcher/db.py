from flask import current_app
import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

def db_config(param):
    return current_app.config['DB_{}'.format(param.upper())]

def db_connect(dbname):
    return psycopg2.connect(dbname=dbname,
                            user=db_config('user'),
                            password=db_config('pass'),
                            host=db_config('host'))

def create_database(dbname):
    conn = db_connect('postgres')
    # set the isolation level so we can create a new database
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    try:
        cur.execute('create database {}'.format(dbname))
    except psycopg2.ProgrammingError as e:  # already exists
        if e.args[0] != 'database "{}" already exists\n'.format(dbname):
            # print(repr(e.value))
            raise
    cur.close()
    conn.close()

    conn = db_connect(dbname)
    cur = conn.cursor()
    try:
        cur.execute('create extension hstore')
    except psycopg2.ProgrammingError as e:
        if e.args[0] != 'extension "hstore" already exists\n':
            raise
        conn.rollback()
    try:
        cur.execute('create extension postgis')
    except psycopg2.ProgrammingError as e:
        if e.args[0] != 'extension "postgis" already exists\n':
            raise
        conn.rollback()
    conn.commit()

    cur.execute("""
select table_name
from information_schema.tables
where table_schema = 'public'""")
    tables = {t[0] for t in cur.fetchall()}

    cur.close()
    conn.close()

    return tables


