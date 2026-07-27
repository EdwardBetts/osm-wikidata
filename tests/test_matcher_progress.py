"""Tests for matcher progress and completion handling."""

import json
from types import SimpleNamespace

import flask
import flask_login

from matcher import matcher_view, websocket


class MatcherRuns:
    """Minimal stand-in for the dynamic matcher_runs relationship."""

    def __init__(self, run):
        self.run = run

    def first(self):
        return self.run


def test_matcher_progress_requires_login(monkeypatch):
    app = flask.Flask(__name__)
    app.secret_key = "test"
    app.register_blueprint(matcher_view.matcher_blueprint)
    login_manager = flask_login.LoginManager(app)
    login_manager.login_view = "login"
    login_manager.user_loader(lambda user_id: None)

    @app.route("/login")
    def login():
        return "login"

    def unexpected_place_lookup(osm_type, osm_id):
        raise AssertionError("anonymous requests must not look up or run a place")

    monkeypatch.setattr(
        matcher_view.Place, "get_or_abort", unexpected_place_lookup
    )

    response = app.test_client().get("/matcher/relation/2965156")

    assert response.status_code == 302
    assert response.location == "/login?next=%2Fmatcher%2Frelation%2F2965156"


def test_matcher_websocket_requires_login():
    app = flask.Flask(__name__)
    login_manager = flask_login.LoginManager(app)
    login_manager.user_loader(lambda user_id: None)
    messages = []
    ws_sock = SimpleNamespace(send=messages.append)

    with app.test_request_context("/websocket/matcher/relation/2965156"):
        rejected = websocket.reject_anonymous_matcher(ws_sock)

    assert rejected
    assert [json.loads(message) for message in messages] == [
        {
            "type": "error",
            "msg": "You need to be logged in to run the matcher.",
        }
    ]


def test_recent_matcher_messages_does_not_replay_completed_run(tmp_path):
    log_filename = tmp_path / "matcher.log"
    log_filename.write_text(json.dumps({"type": "done"}) + "\n")
    run = SimpleNamespace(
        end=object(),
        log_exists=lambda: True,
        log_full_filename=str(log_filename),
    )
    place = SimpleNamespace(matcher_runs=MatcherRuns(run))

    assert websocket.recent_matcher_messages(place) == []


def test_recent_matcher_messages_excludes_terminal_messages(tmp_path):
    log_filename = tmp_path / "matcher.log"
    messages = [
        {"type": "msg", "msg": "working"},
        {"type": "item", "msg": "item progress"},
        {"type": "matching_progress", "num": 1, "total": 2},
        {"type": "failed", "msg": "old failure"},
        {"type": "done"},
    ]
    log_filename.write_text("".join(json.dumps(message) + "\n" for message in messages))
    run = SimpleNamespace(
        end=None,
        log_exists=lambda: True,
        log_full_filename=str(log_filename),
    )
    place = SimpleNamespace(matcher_runs=MatcherRuns(run))

    assert [
        json.loads(message) for message in websocket.recent_matcher_messages(place)
    ] == [{"type": "msg", "msg": "working"}]


def test_matcher_done_does_not_mark_unfinished_place_ready(monkeypatch):
    app = flask.Flask(__name__)
    app.secret_key = "test"
    app.register_blueprint(matcher_view.matcher_blueprint)
    place = SimpleNamespace(
        state="refresh",
        too_big=False,
        matcher_progress_url=lambda: "/matcher/relation/1",
        candidates_url=lambda: "/candidates/relation/1",
    )
    monkeypatch.setattr(
        matcher_view.Place, "get_or_abort", lambda osm_type, osm_id: place
    )

    response = app.test_client().get("/matcher/relation/1/done")

    assert response.status_code == 302
    assert response.location == "/matcher/relation/1"
    assert place.state == "refresh"
