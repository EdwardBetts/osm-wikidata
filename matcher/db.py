from flask import current_app
import psycopg2
import psycopg2.extras

def config(param):
    return current_app.config['DB_{}'.format(param.upper())]

def db_connect():
    conn = psycopg2.connect(dbname=config('name'),
                            user=config('user'),
                            password=config('pass'),
                            host=config('host'))
    psycopg2.extras.register_hstore(conn)
    return conn
