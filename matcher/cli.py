from .view import app
from .model import Changeset
from . import database, mail
from datetime import datetime, timedelta

@app.cli.command()
def mail_recent():
    app.config.from_object('config.default')
    database.init_app(app)

    app.config['SERVER_NAME'] = 'osm.wikidata.link'
    ctx = app.test_request_context()
    ctx.push()  # to make url_for work

    # this works if run from cron once per hour
    # better to report new items since last run
    since = datetime.now() - timedelta(hours=1)

    q = (Changeset.query.filter(Changeset.update_count > 0,
                                Changeset.created > since)
                        .order_by(Changeset.id.desc()))

    template = '''
user: {change.user.username}
name: {name}
page: {url}
items: {change.update_count}
comment: {change.comment}

https://www.openstreetmap.org/changeset/{change.id}
'''

    total_items = 0
    body = ''
    for change in q:
        place = change.place
        url = place.candidates_url(_external=True, _scheme='https')
        body += template.format(name=place.display_name, url=url, change=change)
        total_items += change.update_count

    if total_items > 0:
        subject = 'tags added: {} changesets / {} objects'
        mail.send_mail(subject.format(q.count(), total_items), body)

    ctx.pop()
