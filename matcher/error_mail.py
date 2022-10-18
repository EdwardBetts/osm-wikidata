"""Send mail to admins when there is an error."""

import logging
from logging import Formatter
from logging.handlers import SMTPHandler

import flask

PROJECT = "osm-wikidata"


class MatcherSMTPHandler(SMTPHandler):
    """Custom SMTP handler to change subject line."""

    def getSubject(self, record: logging.LogRecord) -> str:  # noqa: N802
        """Return subject line for error mail."""
        return (
            f"{PROJECT} error: {record.exc_info[0].__name__}"
            if (record.exc_info and record.exc_info[0])
            else f"{PROJECT} error: {record.pathname}:{record.lineno:d}"
        )


class RequestFormatter(Formatter):
    """Custom request formatter."""

    def format(self, record: logging.LogRecord) -> str:
        """Add request to log record."""
        record.request = flask.request
        return super().format(record)


def setup_error_mail(app: flask.Flask) -> None:
    """Configure logging to catch errors and email them."""
    if not app.config.get("ERROR_MAIL"):
        return
    formatter = RequestFormatter(
        """
    Message type:       {levelname}
    Location:           {pathname:s}:{lineno:d}
    Module:             {module:s}
    Function:           {funcName:s}
    Time:               {asctime:s}
    GET args:           {request.args!r}
    URL:                {request.url}

    Message:

    {message:s}
    """,
        style="{",
    )

    mail_handler = MatcherSMTPHandler(
        app.config["SMTP_HOST"],
        app.config["MAIL_FROM"],
        app.config["ADMINS"],
        app.name + " error",
    )
    mail_handler.setFormatter(formatter)

    mail_handler.setLevel(logging.ERROR)
    app.logger.propagate = True
    app.logger.addHandler(mail_handler)
