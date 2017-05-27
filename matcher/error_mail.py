import logging
from flask import current_app
from logging.handlers import SMTPHandler
from logging import Formatter
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
import smtplib

class MatcherSMTPHandler(SMTPHandler):
    def getSubject(self, record):
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

def send_error_mail(subject, body):

    mail_to = current_app.config['ADMIN_EMAIL']
    mail_from = current_app.config['MAIL_FROM']
    msg = MIMEText(body, 'plain', 'UTF-8')

    msg['Subject'] = subject
    msg['To'] = current_app.config['ADMIN_EMAIL']
    msg['From'] = current_app.config['MAIL_FROM']
    msg['Date'] = formatdate()
    msg['Message-ID'] = make_msgid()

    s = smtplib.SMTP(current_app.config['SMTP_HOST'])
    s.sendmail(mail_from, [mail_to], msg.as_string())
    s.quit()
