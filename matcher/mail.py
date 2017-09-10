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

def get_username():
    if hasattr(g, 'user'):
        if g.user.is_authenticated:
            user = g.user.username
        else:
            user = 'not authenticated'
    else:
        user = 'no user'

    return user

def get_area(place):
    return ('{:,.2f} sq km'.format(place.area_in_sq_km)
            if place.area
            else 'n/a')

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
        user = get_username()
        body = 'site URL: {}\nuser: {}\n'.format(request.url, user) + body

    send_mail(subject, body)

def announce_change(change):
    place = change.place
    body = '''
user: {change.user.username}
name: {name}
page: {url}
items: {change.update_count}
comment: {change.comment}

https://www.openstreetmap.org/changeset/{change.id}

'''.format(name=place.display_name,
           url=place.candidates_url(_external=True),
           change=change)

    send_mail('tags added: {}'.format(place.name_for_changeset), body)

def place_error(place, error_type, error_detail):
    template = '''
user: {}
name: {}
page: {}
area: {}
error:
{}
'''

    body = template.format(get_username(),
                           place.display_name,
                           place.candidates_url(_external=True),
                           get_area(place),
                           error_detail)

    if len(error_detail) > 100:
        error_detail = '[long error message]'

    subject = '{}: {} - {}'.format(error_type, place.name, error_detail)
    send_mail(subject, body)

def open_changeset_error(place, changeset, r):
    template = '''
user: {change.user.username}
name: {name}
page: {url}

sent:

{sent}

reply:

{reply}

'''
    body = template.format(name=place.display_name,
                           url=place.candidates_url(_external=True),
                           sent=changeset,
                           reply=r.text)

    send_mail('error creating changeset:' + place.name, body)
