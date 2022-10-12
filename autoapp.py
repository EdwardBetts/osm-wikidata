import logging
import time

import termcolor
from flask import request, request_finished
from werkzeug.middleware.proxy_fix import ProxyFix

from matcher import database
from matcher.error_mail import setup_error_mail
from matcher.view import app

logger = logging.getLogger("osm-wikidata")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
logger.addHandler(handler)
color = termcolor.colored

monthname = [
    None,
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
]


def log_date_time_string():
    """Return the current time formatted for logging."""
    now = time.time()
    year, month, day, hh, mm, ss, x, y, z = time.localtime(now)
    return "%02d/%3s %02d:%02d:%02d" % (day, monthname[month], hh, mm, ss)


def log_request(code="-"):
    proto = request.environ.get("SERVER_PROTOCOL")
    msg = request.method + " " + request.path + " " + proto
    code = str(code)

    if code[0] == "1":  # 1xx - Informational
        msg = color(msg, attrs=["bold"])
    if code[0] == "2":  # 2xx - Success
        msg = color(msg, color="white")
    elif code == "304":  # 304 - Resource Not Modified
        msg = color(msg, color="cyan")
    elif code[0] == "3":  # 3xx - Redirection
        msg = color(msg, color="green")
    elif code == "404":  # 404 - Resource Not Found
        msg = color(msg, color="yellow")
    elif code[0] == "4":  # 4xx - Client Error
        msg = color(msg, color="red", attrs=["bold"])
    else:  # 5xx, or any other response
        msg = color(msg, color="magenta", attrs=["bold"])

    logger.info(
        '%s - - [%s] "%s" %s', request.remote_addr, log_date_time_string(), msg, code
    )


def log_response(sender, response, **extra):
    log_request(response.status_code)


app.config.from_object("config.default")
database.init_app(app)
setup_error_mail(app)
request_finished.connect(log_response, app)
app.wsgi_app = ProxyFix(app.wsgi_app, x_host=1)
