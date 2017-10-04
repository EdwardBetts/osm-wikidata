from .view import app, get_top_existing
from .model import Changeset
from . import database, mail
from datetime import datetime, timedelta
from tabulate import tabulate

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

@app.cli.command()
def show_big_tables():
    app.config.from_object('config.default')
    database.init_app(app)
    for row in database.get_big_table_list():
        print(row)

@app.cli.command()
def recent():
    app.config.from_object('config.default')
    database.init_app(app)
    q = (Changeset.query.filter(Changeset.update_count > 0)
                        .order_by(Changeset.id.desc()))

    rows = [(obj.created.strftime('%F %T'),
             obj.user.username,
             obj.update_count,
             obj.place.name_for_changeset) for obj in q.limit(25)]
    print(tabulate(rows,
                   headers=['when', 'who', '#', 'where'],
                   tablefmt='simple'))

@app.cli.command()
def top():
    app.config.from_object('config.default')
    database.init_app(app)
    top_places = get_top_existing()
    headers = ['id', 'name', 'area', 'state', 'candidates', 'items',
               'changesets']

    places = []
    for p, changeset_count in top_places:
        name = p.display_name
        if len(name) > 60:
            name = name[:56] + ' ...'
        places.append((p.place_id,
                       name,
                       p.area,
                       p.state,
                       p.candidate_count,
                       p.item_count,
                       changeset_count))

    print(tabulate(places,
                   headers=headers,
                   tablefmt='simple'))
