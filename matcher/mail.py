from flask import current_app, g, request, has_request_context
from email.mime.text import MIMEText
from email.utils import formatdate, make_msgid
from pprint import pformat
import smtplib
import traceback
import sys

def send_mail(subject, body, config=None):
    try:
        send_mail_main(subject, body, config=config)
    except smtplib.SMTPDataError:
        pass  # ignore email errors

def send_mail_main(subject, body, config=None):
    if config is None:
        config = current_app.config

    mail_to = config['ADMIN_EMAIL']
    mail_from = config['MAIL_FROM']
    msg = MIMEText(body, 'plain', 'UTF-8')

    msg['Subject'] = subject
    msg['To'] = mail_to
    msg['From'] = mail_from
    msg['Date'] = formatdate()
    msg['Message-ID'] = make_msgid()

    s = smtplib.SMTP(config['SMTP_HOST'])
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

status code: {r.status_code}
content-type: {r.headers[content-type]}

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

    if error_detail is None:
        error_detail = '[None]'
    elif len(error_detail) > 100:
        error_detail = '[long error message]'

    subject = '{}: {} - {}'.format(error_type, place.name, error_detail)
    send_mail(subject, body)

def open_changeset_error(place, changeset, r):
    url = place.candidates_url(_external=True)
    body = f'''
user: {g.user.username}
name: {place.display_name}
page: {url}

sent:

{changeset}

reply:

{r.text}

'''

    send_mail('error creating changeset:' + place.name, body)

def send_traceback(info, prefix='osm-wikidata'):
    exception_name = sys.exc_info()[0].__name__
    subject = f'{prefix} error: {exception_name}'
    body = f'user: {get_username()}\n' + info + '\n' + traceback.format_exc()
    send_mail(subject, body)

def datavalue_missing(field, entity):
    qid = entity['title']
    body = f'https://www.wikidata.org/wiki/{qid}\n\n{pformat(entity)}'

    subject = f'{qid}: datavalue missing in {field}'
    send_mail(subject, body)
