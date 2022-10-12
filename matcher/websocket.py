"""Websocket code to make the system more interactive."""

import json
import re
import select
import traceback

import requests
from flask import g, request
from flask_login import current_user
from flask_sock import Sock
from lxml import etree
from sqlalchemy.orm.attributes import flag_modified

from . import chat, database, edit, mail
from .model import ChangesetEdit, ItemCandidate
from .place import Place

sock = Sock()

re_point = re.compile(r"^Point\(([-E0-9.]+) ([-E0-9.]+)\)$")

PING_SECONDS = 10

# TODO: different coloured icons
# - has enwiki article
# - match found
# - match not found


class VersionMismatch(Exception):
    """The version of the OSM object we're saving doesn't match."""


def add_wikipedia_tag(root, m) -> None:
    """Add a wikipedia tag to the XML of an OSM object."""
    lang = m.get("wiki_lang")
    if not lang or root.find(f'.//tag[@k="wikipedia:{lang}"]') is not None:
        return
    value = lang + ":" + m["wiki_title"]
    existing = root.find('.//tag[@k="wikipedia"]')
    if existing is not None:
        existing.set("v", value)
        return
    tag = etree.Element("tag", k="wikipedia", v=value)
    root[0].append(tag)


@sock.route("/websocket/matcher/<osm_type>/<int:osm_id>")
def ws_matcher(ws_sock, osm_type, osm_id):
    """Run matcher for given place."""
    # idea: catch exceptions, then pass to pass to web page as status update
    # also e-mail them
    place = None

    def send(msg):
        return ws_sock.send(msg)

    try:
        place = Place.get_by_osm(osm_type, osm_id)

        if place.state == "ready":
            send(json.dumps({"type": "already_done"}))
            return  # FIXME - send error mail

        user_agent = request.headers.get("User-Agent")
        user_id = current_user.id if current_user.is_authenticated else None
        queue_socket = chat.connect_to_queue()
        params = {
            "type": "match",
            "osm_type": osm_type,
            "osm_id": osm_id,
            "user": user_id,
            "remote_addr": request.remote_addr,
            "user_agent": user_agent,
        }
        chat.send_command(queue_socket, "match", **params)

        while ws_sock.connected:
            readable = select.select([queue_socket], [], [], timeout=PING_SECONDS)[0]
            if readable:
                item = chat.read_line(queue_socket)
            else:  # timeout
                item = json.dumps({"type": "ping"})

            if not item:
                ws_sock.close()
                break
            send(item)
            if not ws_sock.connected:
                break
            reply = ws_sock.receive()
            if reply is None:
                break
            if reply != "ack":
                print("reply: ", repr(reply))
            assert reply == "ack", "No ack."

        queue_socket.close()

    except Exception as e:
        msg = type(e).__name__ + ": " + str(e)
        print(msg)
        print(traceback.format_exc())
        send(json.dumps({"type": "error", "msg": msg}))

        g.user = current_user

        name = place.display_name if place else "unknown place"
        info = f"""
place: {name}
https://openstreetmap.org/{osm_type}/{osm_id}

exception in matcher websocket
"""
        mail.send_traceback(info)


def check_if_already_tagged(r, osm) -> bool:
    """Is this match already tagged? If yes then update database."""
    if b"wikidata" not in r.content:
        return False
    root = etree.fromstring(r.content)
    existing = root.find('.//tag[@k="wikidata"]')
    if existing is None:
        return False

    osm.tags["wikidata"] = existing.get("v")
    flag_modified(osm, "tags")
    database.session.commit()
    return True


def get_osm_object(m):
    """Given a match item retrieve the OSM object."""
    osm_type, osm_id = m["osm_type"], m["osm_id"]
    item_id = m["qid"][1:]

    osm = ItemCandidate.query.filter_by(
        item_id=item_id, osm_type=osm_type, osm_id=osm_id
    ).one_or_none()

    return osm


def build_updated_xml(content, m, changeset_id):
    """Update the OSM XML with wikidata tag and possibly a wikipedia tag."""
    root = etree.fromstring(content)
    tag = etree.Element("tag", k="wikidata", v=m["qid"])
    root[0].set("changeset", changeset_id)
    root[0].append(tag)
    add_wikipedia_tag(root, m)
    element_data = etree.tostring(root)

    return element_data


def save_changeset_edit(m, changeset_id):
    """Save the details of an individual edit to the database."""
    db_edit = ChangesetEdit(
        changeset_id=changeset_id,
        item_id=m["qid"][1:],
        osm_id=m["osm_id"],
        osm_type=m["osm_type"],
    )
    database.session.add(db_edit)
    database.session.commit()


def edit_failed(r, e, element_data):
    """Handle a failure to save."""
    if e.response.status_code == 409 and "Version mismatch" in r.text:
        raise VersionMismatch
    mail.error_mail("error saving element", element_data.decode("utf-8"), e.response)
    database.session.commit()


def process_match(changeset_id, m):
    """Upload an individual match to OSM as part of a changeset."""
    osm_type, osm_id = m["osm_type"], m["osm_id"]

    r = edit.get_existing(osm_type, osm_id)
    if r.status_code == 410 or r.content == b"":
        return "deleted"

    osm = get_osm_object(m)
    if check_if_already_tagged(r, osm):
        return "already_tagged"

    element_data = build_updated_xml(r.content, m, changeset_id)

    try:
        success = edit.save_element(osm_type, osm_id, element_data)
    except requests.exceptions.HTTPError as e:
        edit_failed(r, e, element_data)
        return "element-error"

    if not success:
        return "element-error"

    osm.tags["wikidata"] = m["qid"]
    flag_modified(osm, "tags")
    # TODO: also update wikipedia tag if appropriate
    save_changeset_edit(m, changeset_id)

    return "saved"


def handle_match(change, num, m):
    """Create a changeset."""
    while True:
        try:
            result = process_match(change.id, m)
        except VersionMismatch:  # FIXME: limit number of attempts
            continue  # retry
        else:
            break
    if result == "saved":
        change.update_count += 1
    database.session.commit()
    return result


def add_tags(ws_sock, osm_type, osm_id):
    """Add tags or OSM object."""

    def send(msg_type, **kwars):
        """Send message to socket."""
        ws_sock.send(json.dumps({"type": msg_type, **kwars}))

    place = Place.get_by_osm(osm_type, osm_id)

    data = json.loads(ws_sock.receive())
    comment = data["comment"]
    changeset = edit.new_changeset(comment)
    r = edit.create_changeset(changeset)
    reply = r.text.strip()

    if reply == "Couldn't authenticate you":
        mail.open_changeset_error(place, changeset, r)
        send("auth-fail")
        return

    if not reply.isdigit():
        mail.open_changeset_error(place, changeset, r)
        send("changeset-error", msg=reply)
        return

    # clear the match cache
    place.match_cache = None
    database.session.commit()

    changeset_id = reply
    send("open", id=int(changeset_id))

    change = edit.record_changeset(
        id=changeset_id,
        place=place,
        comment=comment,
        update_count=0,
    )

    for num, m in enumerate(data["matches"]):
        send("progress", qid=m["qid"], num=num)
        result = handle_match(change, num, m)
        send(result, qid=m["qid"], num=num)

    send("closing")
    edit.close_changeset(changeset_id)
    send("done")

    # make sure the match cache is cleared
    place.match_cache = None
    database.session.commit()


@sock.route("/websocket/add_tags/<osm_type>/<int:osm_id>")
def ws_add_tags(ws_sock, osm_type, osm_id):
    """Upload tags for OSM object."""
    g.user = current_user

    def send(msg_type, **kwars):
        ws_sock.send(json.dumps({"type": msg_type, **kwars}))

    place = None
    try:
        add_tags(ws_sock, osm_type, osm_id)
    except Exception as e:
        msg = type(e).__name__ + ": " + str(e)
        print(msg)
        send("error", msg=msg)

        if place:
            name = place.display_name
        else:
            name = "unknown place"
        info = f"""
place: {name}
https://openstreetmap.org/{osm_type}/{osm_id}

exception in add tags websocket
"""
        mail.send_traceback(info)
