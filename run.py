#!/usr/bin/python3

from matcher.view import app

if __name__ == "__main__":
    app.config.from_object('config.default')
    app.debug = True
    app.run('0.0.0.0', port=5001)
