"""Tests for Wikidata OAuth token persistence."""

import json
from types import SimpleNamespace
from unittest.mock import patch

import flask
import pytest

from matcher import wikidata_oauth


@pytest.fixture
def oauth_app():
    """Minimal Flask app; these tests do not need the database fixture."""
    app = flask.Flask(__name__)
    app.secret_key = "test"
    return app


def test_get_token_uses_database_instead_of_stale_session(oauth_app):
    """A rotated refresh token in the database must win over cookie state."""
    database_token = {"access_token": "new", "refresh_token": "new-refresh"}
    stale_token = {"access_token": "old", "refresh_token": "old-refresh"}
    user = SimpleNamespace(
        is_authenticated=True,
        wikidata_oauth_token=json.dumps(database_token),
    )

    with oauth_app.test_request_context("/"):
        flask.g.user = user
        flask.session["wikidata_oauth_token"] = stale_token

        assert wikidata_oauth.get_token() == database_token
        assert "wikidata_oauth_token" not in flask.session


def test_save_token_only_persists_in_database(oauth_app):
    """Refreshed credentials must not be copied into the client-side session."""
    token = {"access_token": "new", "refresh_token": "new-refresh"}
    user = SimpleNamespace(is_authenticated=True, wikidata_oauth_token=None)

    with oauth_app.test_request_context("/"):
        flask.g.user = user
        flask.session["wikidata_oauth_token"] = {"access_token": "stale"}

        with patch.object(wikidata_oauth.database.session, "commit") as commit:
            wikidata_oauth.save_token(token)

        assert json.loads(user.wikidata_oauth_token) == token
        assert "wikidata_oauth_token" not in flask.session
        commit.assert_called_once_with()
