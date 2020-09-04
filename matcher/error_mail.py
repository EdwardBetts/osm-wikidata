import logging
from logging.handlers import SMTPHandler
from logging import Formatter
from flask import request

PROJECT = 'osm-wikidata'

class MatcherSMTPHandler(SMTPHandler):
    def getSubject(self, record):  # noqa: N802
        return (f'{PROJECT} error: {record.exc_info[0].__name__}'
                if (record.exc_info and record.exc_info[0])
                else f'{PROJECT} error: {record.pathname}:{record.lineno:d}')


class RequestFormatter(Formatter):
    def format(self, record):
        record.request = request
        return super().format(record)


def setup_error_mail(app):
    if not app.config.get('ERROR_MAIL'):
        return
    formatter = RequestFormatter('''
    Message type:       {levelname}
    Location:           {pathname:s}:{lineno:d}
    Module:             {module:s}
    Function:           {funcName:s}
    Time:               {asctime:s}
    GET args:           {request.args!r}
    URL:                {request.url}

    Message:

    {message:s}
    ''', style='{')

    mail_handler = MatcherSMTPHandler(app.config['SMTP_HOST'],
                                      app.config['MAIL_FROM'],
                                      app.config['ADMINS'],
                                      app.name + ' error')
    mail_handler.setFormatter(formatter)

    mail_handler.setLevel(logging.ERROR)
    app.logger.propagate = True
    app.logger.addHandler(mail_handler)
