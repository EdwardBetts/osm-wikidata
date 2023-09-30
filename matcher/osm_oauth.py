import json
from datetime import datetime
from urllib.parse import urlencode

import lxml.etree
from flask import current_app, g, session, url_for
from requests_oauthlib import OAuth2Session

from . import user_agent_headers
from .model import User

osm_api_base = "https://api.openstreetmap.org/api/0.6"
scope = ["read_prefs", "write_api"]


def get_session():
    token = session.get("oauth_token")
    if not token:
        user = g.user
        assert user.is_authenticated
        token = json.loads(user.osm_oauth_token)
        session["oauth_token"] = token

    callback = url_for("oauth_callback", _external=True)
    return OAuth2Session(
        current_app.config["CLIENT_KEY"],
        redirect_uri=callback,
        scope=scope,
        token=token,
    )


def api_put_request(path, **kwargs):
    oauth = get_session()
    return oauth.request(
        "PUT", osm_api_base + path, headers=user_agent_headers(), **kwargs
    )


def api_request(path, **params):
    url = osm_api_base + path
    if params:
        url += "?" + urlencode(params)
    oauth = get_session()
    return oauth.get(url, timeout=4)


def parse_iso_date(value):
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")


def parse_userinfo_call(xml):
    root = lxml.etree.fromstring(xml)
    user = root[0]
    img = user.find(".//img")

    account_created = parse_iso_date(user.get("account_created"))

    assert user.tag == "user"

    return {
        "account_created": account_created,
        "id": int(user.get("id")),
        "username": user.get("display_name"),
        "description": user.findtext(".//description"),
        "img": (img.get("href") if img is not None else None),
    }


def get_username():
    if "user_id" not in session:
        return  # not authorized

    user_id = session["user_id"]

    user = User.query.get(user_id)
    return user.username
