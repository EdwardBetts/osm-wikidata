#!/usr/bin/python3

from matcher.view import app
from matcher import database

if __name__ == "__main__":
    app.config.from_object('config.default')
    database.init_app(app)
    app.debug = True
    app.run('0.0.0.0', port=5001)
