import logging
from logging.handlers import SMTPHandler
from logging import Formatter

class MatcherSMTPHandler(SMTPHandler):
    def getSubject(self, record):  # noqa: N802
        return ('osm-wikidata error: {}'.format(record.exc_info[0].__name__)
                if (record.exc_info and record.exc_info[0])
                else 'osm-wikidata error: {}:{:d}'.format(record.pathname, record.lineno))

def setup_error_mail(app):
    mail_handler = MatcherSMTPHandler(app.config['SMTP_HOST'],
                                      app.config['MAIL_FROM'],
                                      app.config['ADMINS'],
                                      app.name + ' error')
    mail_handler.setFormatter(Formatter('''
    Message type:       %(levelname)s
    Location:           %(pathname)s:%(lineno)d
    Module:             %(module)s
    Function:           %(funcName)s
    Time:               %(asctime)s

    Message:

    %(message)s
    '''))

    mail_handler.setLevel(logging.ERROR)
    app.logger.propagate = True
    app.logger.addHandler(mail_handler)
