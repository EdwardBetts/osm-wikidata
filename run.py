#!/usr/bin/python3

from matcher.view import app
from matcher import database
from matcher.error_mail import setup_error_mail
from flask_debugtoolbar import DebugToolbarExtension
# from werkzeug.debug import DebuggedApplication
# from werkzeug.serving import run_simple

if __name__ == "__main__":
    app.config.from_object('config.default')
    database.init_app(app)
    setup_error_mail(app)
    app.debug = False
    app.debug = True
    toolbar = DebugToolbarExtension(app)
    # debug_app = DebuggedApplication(app, evalex=False)

    app.run(port=5001)

    # run_simple('localhost', 5001, debug_app, use_reloader=True)
