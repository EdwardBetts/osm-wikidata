from flask import g
from . import user_agent_headers

really_save = True
osm_api_base = 'https://api.openstreetmap.org/api/0.6'

def get_backend_and_auth():
    if not really_save:
        return None, None

    user = g.user
    assert user.is_authenticated

    social_user = user.social_auth.one()
    osm_backend = social_user.get_backend_instance()
    auth = osm_backend.oauth_auth(social_user.access_token)

    return osm_backend, auth

def new_changeset(comment):
    return '''
<osm>
  <changeset>
    <tag k="created_by" v="https://osm.wikidata.link/"/>
    <tag k="comment" v="{}"/>
  </changeset>
</osm>'''.format(comment)

def osm_request(path, **kwargs):
    osm_backend, auth = get_backend_and_auth()
    r = osm_backend.request(osm_api_base + path,
                            method='PUT',
                            auth=auth,
                            headers=user_agent_headers(),
                            **kwargs)
    return r

def create_changeset(changeset):
    r = osm_request('/changeset/create', data=changeset.encode('utf-8'))
    return r.text.strip()

def close_changeset(changeset_id):
    r = osm_request(f'/changeset/{changeset_id}/close')
    return r

def save_element(osm_type, osm_id, element_data):
    r = osm_request(f'{osm_type}/{osm_id}', data=element_data)
    assert(r.text.strip().isdigit())
    return r
