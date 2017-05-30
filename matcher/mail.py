from flask import current_app, g, request, has_request_context
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

def error_mail(subject, data, r, via_web=True):
    body = '''
remote URL: {r.url}
status code: {r.status_code}

request data:
{data}

reply:
{r.text}
'''.format(r=r, data=data)

    if not has_request_context():
        via_web = False

    if via_web:
        if hasattr(g, 'user'):
            if g.user.is_authenticated:
                user = g.user.username
            else:
                user = 'not authenticated'
        else:
            user = 'no user'
        body = 'site URL: {}\nuser: {}\n'.format(request.url, user) + body

    send_mail(subject, body)
