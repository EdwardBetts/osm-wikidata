"""Wikidata OAuth helpers."""

import json
import sys
import typing
import urllib.parse

import flask
import requests
from requests_oauthlib import OAuth2Session

from . import database
from . import user_agent_headers

wiki_hostname = "www.wikidata.org"
oauth_hostname = "meta.wikimedia.org"
api_url = f"https://{wiki_hostname}/w/api.php"
oauth_rest_url = f"https://{oauth_hostname}/w/rest.php"
authorize_url = f"{oauth_rest_url}/oauth2/authorize"
access_token_url = f"{oauth_rest_url}/oauth2/access_token"
profile_url = f"{oauth_rest_url}/oauth2/resource/profile"


class LoginNeeded(Exception):
    """Raised when a Wikidata OAuth request needs a connected account."""


def get_token() -> dict[str, typing.Any]:
    """Return the current user's Wikidata OAuth 2 token."""
    token = flask.session.get("wikidata_oauth_token")
    if token:
        return typing.cast(dict[str, typing.Any], token)
    user = flask.g.user
    if not user.is_authenticated:
        raise LoginNeeded

    if not user.wikidata_oauth_token:
        raise LoginNeeded

    token = json.loads(user.wikidata_oauth_token)
    flask.session["wikidata_oauth_token"] = token
    return typing.cast(dict[str, typing.Any], token)


def save_token(token: dict[str, typing.Any]) -> None:
    """Persist a refreshed Wikidata OAuth 2 token."""
    flask.session["wikidata_oauth_token"] = token
    user = flask.g.user
    if user.is_authenticated:
        user.wikidata_oauth_token = json.dumps(token)
        database.session.commit()


def get_session() -> OAuth2Session:
    """Return an authenticated Wikidata OAuth session for the current user."""
    app = flask.current_app
    oauth = OAuth2Session(
        app.config["WIKIDATA_CLIENT_KEY"],
        token=get_token(),
        auto_refresh_url=access_token_url,
        auto_refresh_kwargs={
            "client_id": app.config["WIKIDATA_CLIENT_KEY"],
            "client_secret": app.config["WIKIDATA_CLIENT_SECRET"],
        },
        token_updater=save_token,
    )
    oauth.headers.update(user_agent_headers())
    return oauth


def raw_request(params: typing.Mapping[str, str | int]) -> requests.Response:
    """Low-level Wikidata API request using OAuth."""
    url = api_url + "?" + urllib.parse.urlencode(params)
    return get_session().get(url, timeout=4)


def api_request(params: typing.Mapping[str, str | int]) -> dict[str, typing.Any]:
    """Make a Wikidata API request using OAuth."""
    r = raw_request(params)
    try:
        return typing.cast(dict[str, typing.Any], r.json())
    except Exception:
        print(f"Wikidata API request failed: HTTP {r.status_code}", file=sys.stderr)
        print(f"Response body: {r.text!r}", file=sys.stderr)
        raise


def userinfo_call() -> typing.Mapping[str, typing.Any]:
    """Request Wikidata user information via OAuth."""
    return typing.cast(dict[str, typing.Any], get_session().get(profile_url).json())


def get_username() -> str | None:
    """Return the connected Wikidata username, if available."""
    user = flask.g.user
    if not user.is_authenticated:
        return None

    if user.wikidata_username:
        return typing.cast(str, user.wikidata_username)

    if not user.wikidata_oauth_token:
        return None

    try:
        reply = userinfo_call()
    except Exception as e:
        print(f"get Wikidata username failed, clearing token: {e}", file=sys.stderr)
        clear_connection()
        return None

    if "username" not in reply:
        return None

    username = typing.cast(str, reply["username"])
    user.wikidata_username = username
    return username


def clear_connection() -> None:
    """Remove Wikidata OAuth data from the session and current user."""
    for key in (
        "wikidata_oauth_token",
        "wikidata_username",
        "wikidata_after_login",
        "wikidata_oauth_state",
    ):
        flask.session.pop(key, None)

    user = flask.g.user
    if user.is_authenticated:
        user.wikidata_username = None
        user.wikidata_oauth_token = None
