from flask import g
from . import user_agent_headers, database, osm_oauth, mail
from .model import Changeset
import requests
import html

really_save = True
osm_api_base = 'https://api.openstreetmap.org/api/0.6'

def new_changeset(comment):
    return '''
<osm>
  <changeset>
    <tag k="created_by" v="https://osm.wikidata.link/"/>
    <tag k="comment" v="{}"/>
  </changeset>
</osm>'''.format(html.escape(comment))

def osm_request(path, **kwargs):
    return osm_oauth.api_put_request(path, **kwargs)

def create_changeset(changeset):
    try:
        return osm_request('/changeset/create', data=changeset.encode('utf-8'))
    except requests.exceptions.HTTPError as r:
        print(changeset)
        print(r.response.text)
        raise

def close_changeset(changeset_id):
    return osm_request(f'/changeset/{changeset_id}/close')

def save_element(osm_type, osm_id, element_data):
    osm_path = f'/{osm_type}/{osm_id}'
    r = osm_request(osm_path, data=element_data)
    reply = r.text.strip()
    if reply.isdigit():
        return r

    subject = f'matcher error saving element: {osm_path}'
    username = g.user.username
    body = f'''
https://www.openstreetmap.org{osm_path}

user: {username}
message user: https://www.openstreetmap.org/message/new/{username}

error:
{reply}
'''

    mail.send_mail(subject, body)


def record_changeset(**kwargs):
    change = Changeset(created=database.now_utc(), user=g.user, **kwargs)

    database.session.add(change)
    database.session.commit()

    return change

def get_existing(osm_type, osm_id):
    url = '{}/{}/{}'.format(osm_api_base, osm_type, osm_id)
    return requests.get(url, headers=user_agent_headers())
