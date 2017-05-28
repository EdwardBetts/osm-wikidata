from flask import current_app, g, request
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
import smtplib

def send_mail(subject, body):

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

def error_mail(subject, wikidata_id, data, r):
    if g.user.is_authenticated:
        user = g.user.username
    else:
        user = 'not authenticated'

    send_mail(subject, '''
URL: {}
wikidata ID: {}
status code: {}
user: {}

request data:
{}

reply:
{}
'''.format(request.url, wikidata_id, r.status_code, user, data, r.text))
