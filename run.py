#!/usr/bin/python3

from matcher.view import app
from matcher import database
from matcher.error_mail import setup_error_mail

if __name__ == "__main__":
    app.config.from_object('config.default')
    database.init_app(app)
    setup_error_mail(app)
    app.debug = False
    app.run('0.0.0.0', port=5001)
