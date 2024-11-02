"""OSM Authentication."""

import json
import typing
from datetime import datetime
from urllib.parse import urlencode

import flask
import lxml.etree
import requests
from requests_oauthlib import OAuth2Session

from . import user_agent_headers
from .model import User

osm_api_base = "https://api.openstreetmap.org/api/0.6"
scope = ["read_prefs", "write_api"]


def get_session() -> OAuth2Session:
    """Get session."""
    token = flask.session.get("oauth_token")
    if not token:
        user = flask.g.user
        assert user.is_authenticated
        token = json.loads(user.osm_oauth_token)
        flask.session["oauth_token"] = token

    callback = flask.url_for("oauth_callback", _external=True)
    return OAuth2Session(
        flask.current_app.config["CLIENT_KEY"],
        redirect_uri=callback,
        scope=scope,
        token=token,
    )


def api_put_request(path: str, **kwargs: typing.Any) -> requests.Response:
    """Send OSM API PUT request."""
    oauth = get_session()

    return oauth.request(
        "PUT", osm_api_base + path, headers=user_agent_headers(), **kwargs
    )


def api_request(path: str, **params: typing.Any) -> requests.Response:
    """Send OSM API request."""
    url = osm_api_base + path
    if params:
        url += "?" + urlencode(params)

    oauth = get_session()
    return oauth.get(url, timeout=4)


def parse_iso_date(value: str) -> datetime:
    """Parse ISO date."""
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")


def parse_userinfo_call(xml: bytes) -> dict[str, typing.Any]:
    """Parse userinfo call."""
    root = lxml.etree.fromstring(xml)
    user = root[0]
    img = user.find(".//img")

    account_created_date = user.get("account_created")
    assert account_created_date
    account_created = parse_iso_date(account_created_date)

    assert user.tag == "user"

    id_str = user.get("id")
    assert id_str and isinstance(id_str, str)

    return {
        "account_created": account_created,
        "id": int(id_str),
        "username": user.get("display_name"),
        "description": user.findtext(".//description"),
        "img": (img.get("href") if img is not None else None),
    }


def get_username() -> str | None:
    """Get username of current user."""
    if "user_id" not in flask.session:
        return None  # not authorized

    user_id = flask.session["user_id"]

    user = User.query.get(user_id)
    return typing.cast(str, user.username)
