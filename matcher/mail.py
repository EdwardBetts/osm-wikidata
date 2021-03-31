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

    mail_to = config["ADMIN_EMAIL"]
    mail_from = config["MAIL_FROM"]
    msg = MIMEText(body, "plain", "UTF-8")

    msg["Subject"] = subject
    msg["To"] = mail_to
    msg["From"] = mail_from
    msg["Date"] = formatdate()
    msg["Message-ID"] = make_msgid()

    s = smtplib.SMTP(config["SMTP_HOST"])
    s.sendmail(mail_from, [mail_to], msg.as_string())
    s.quit()


def get_username():
    if hasattr(g, "user"):
        if g.user.is_authenticated:
            user = g.user.username
        else:
            user = "not authenticated"
    else:
        user = "no user"

    return user


def get_area(place):
    return f"{place.area_in_sq_km:,.2f} sq km" if place.area else "n/a"


def error_mail(subject, data, r, via_web=True):
    body = f"""
remote URL: {r.url}
status code: {r.status_code}

request data:
{data}

status code: {r.status_code}
content-type: {r.headers["content-type"]}

reply:
{r.text}
"""

    if has_request_context():
        body = f"site URL: {request.url}\nuser: {get_username()}\n" + body

    send_mail(subject, body)


def announce_change(change):
    body = f"""
user: {change.user.username}
name: {change.place.display_name}
page: {change.place.candidates_url(_external=True)}
items: {change.update_count}
comment: {change.comment}

https://www.openstreetmap.org/changeset/{change.id}

"""

    send_mail(f"tags added: {change.place.name_for_changeset}", body)


def place_error(place, error_type, error_detail):
    body = f"""
user: {get_username()}
name: {place.display_name}
page: {place.candidates_url(_external=True)}
area: {get_area(place)}
error:
{error_detail}
"""

    if error_detail is None:
        error_detail = "[None]"
    elif len(error_detail) > 100:
        error_detail = "[long error message]"

    subject = f"{error_type}: {place.name} - {error_detail}"
    send_mail(subject, body)


def open_changeset_error(place, changeset, r):
    url = place.candidates_url(_external=True)
    username = g.user.username
    body = f"""
user: {username}
name: {place.display_name}
page: {url}

message user: https://www.openstreetmap.org/message/new/{username}

sent:

{changeset}

reply:

{r.text}

"""

    send_mail("error creating changeset:" + place.name, body)


def send_traceback(info, prefix="osm-wikidata"):
    exception_name = sys.exc_info()[0].__name__
    subject = f"{prefix} error: {exception_name}"
    body = f"user: {get_username()}\n" + info + "\n" + traceback.format_exc()
    send_mail(subject, body)


def datavalue_missing(field, entity):
    qid = entity["title"]
    body = f"https://www.wikidata.org/wiki/{qid}\n\n{pformat(entity)}"

    subject = f"{qid}: datavalue missing in {field}"
    send_mail(subject, body)
