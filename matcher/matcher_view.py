"""Matcher views."""

import re

import flask
import werkzeug

from . import database, mail, utils
from .place import Place

re_point = re.compile(r"^Point\((-?[0-9.]+) (-?[0-9.]+)\)$")

matcher_blueprint = flask.Blueprint("matcher", __name__)


def announce_matcher_progress(place: Place) -> None:
    """Send mail to announce when somebody runs the matcher."""
    if flask.current_app.config["DEBUG"]:
        return
    if flask.g.user.is_authenticated:
        user = flask.g.user.username
        subject = f"matcher: {place.name} (user: {user})"
    elif utils.is_bot():
        return  # don't announce bots
    else:
        user = "not authenticated"
        subject = f"matcher: {place.name} (no auth)"

    user_agent = flask.request.headers.get("User-Agent", "[header missing]")
    body = f"""
user: {user}
IP: {flask.request.remote_addr}
agent: {user_agent}
name: {place.display_name}
page: {place.candidates_url(_external=True)}
area: {mail.get_area(place)}
"""
    mail.send_mail(subject, body)


@matcher_blueprint.route("/matcher/<osm_type>/<int:osm_id>")
def matcher_progress(osm_type: str, osm_id: int) -> werkzeug.wrappers.Response | str:
    """Matcher progress page."""
    place = Place.get_or_abort(osm_type, osm_id)
    if place.state == "ready":
        return flask.redirect(place.candidates_url())

    if place.too_big or place.too_complex:
        return flask.render_template("too_big.html", place=place)

    is_refresh = place.state == "refresh"

    announce_matcher_progress(place)

    url_scheme = flask.request.environ.get("wsgi.url_scheme")
    ws_scheme = "wss" if url_scheme == "https" else "ws"

    return flask.render_template(
        "matcher.html",
        place=place,
        is_refresh=is_refresh,
        ws_scheme=ws_scheme,
    )


@matcher_blueprint.route("/matcher/<osm_type>/<int:osm_id>/done")
def matcher_done(osm_type: str, osm_id: int) -> werkzeug.wrappers.Response | str:
    """Matcher done redirect."""
    place = Place.get_or_abort(osm_type, osm_id)
    if place.too_big:
        return flask.render_template("too_big.html", place=place)

    if place.state != "ready":
        place.state = "ready"
        database.session.commit()

    flask.flash("The matcher has finished.")
    return flask.redirect(place.candidates_url())


@matcher_blueprint.route("/replay/<osm_type>/<int:osm_id>")
def replay(osm_type: str, osm_id: int) -> str:
    """Replay matcher run."""
    place = Place.get_or_abort(osm_type, osm_id)

    replay_log = True
    url_scheme = flask.request.environ.get("wsgi.url_scheme")
    ws_scheme = "wss" if url_scheme == "https" else "ws"

    return flask.render_template(
        "matcher.html", place=place, ws_scheme=ws_scheme, replay_log=replay_log
    )
