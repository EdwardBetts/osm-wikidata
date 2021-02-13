from flask import current_app, session
from requests_oauthlib import OAuth1Session
from urllib.parse import urlencode
from datetime import datetime
from flask import g

from .model import User

from . import user_agent_headers

import lxml.etree

osm_api_base = "https://api.openstreetmap.org/api/0.6"


def api_put_request(path, **kwargs):
    user = g.user
    assert user.is_authenticated
    oauth = OAuth1Session(
        current_app.config["CLIENT_KEY"],
        client_secret=current_app.config["CLIENT_SECRET"],
        resource_owner_key=user.osm_oauth_token,
        resource_owner_secret=user.osm_oauth_token_secret,
    )
    return oauth.request(
        "PUT", osm_api_base + path, headers=user_agent_headers(), **kwargs
    )


def api_request(path, **params):
    user = g.user
    assert user.is_authenticated
    app = current_app
    url = osm_api_base + path
    if params:
        url += "?" + urlencode(params)
    client_key = app.config["CLIENT_KEY"]
    client_secret = app.config["CLIENT_SECRET"]
    oauth = OAuth1Session(
        client_key,
        client_secret=client_secret,
        resource_owner_key=user.osm_oauth_token,
        resource_owner_secret=user.osm_oauth_token_secret,
    )
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
