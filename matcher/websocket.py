from flask import Blueprint, g, request
from flask_login import current_user
from .place import Place
from . import database, edit, mail, chat
from .model import ItemCandidate, ChangesetEdit
from lxml import etree
from sqlalchemy.orm.attributes import flag_modified
import requests
import re
import json
import select
import traceback

ws = Blueprint("ws", __name__)
re_point = re.compile(r"^Point\(([-E0-9.]+) ([-E0-9.]+)\)$")

PING_SECONDS = 10

# TODO: different coloured icons
# - has enwiki article
# - match found
# - match not found


class VersionMismatch(Exception):
    pass


def add_wikipedia_tag(root, m):
    lang = m.get("wiki_lang")
    if not lang or root.find(f'.//tag[@k="wikipedia:{lang}"]') is not None:
        return
    value = lang + ":" + m["wiki_title"]
    existing = root.find(f'.//tag[@k="wikipedia"]')
    if existing is not None:
        existing.set("v", value)
        return
    tag = etree.Element("tag", k="wikipedia", v=value)
    root[0].append(tag)


@ws.route("/websocket/matcher/<osm_type>/<int:osm_id>")
def ws_matcher(ws_sock, osm_type, osm_id):
    # idea: catch exceptions, then pass to pass to web page as status update
    # also e-mail them

    place = None

    try:
        place = Place.get_by_osm(osm_type, osm_id)

        if place.state == "ready":
            ws_sock.send(json.dumps({"type": "already_done"}))
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

        while not ws_sock.closed:
            readable = select.select([queue_socket], [], [], timeout=PING_SECONDS)[0]
            if readable:
                item = chat.read_line(queue_socket)
            else:  # timeout
                item = json.dumps({"type": "ping"})

            if not item:
                ws_sock.close()
                break
            ws_sock.send(item)
            if ws_sock.closed:
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
        ws_sock.send(json.dumps({"type": "error", "msg": msg}))

        g.user = current_user

        name = place.display_name if place else "unknown place"
        info = f"""
place: {name}
https://openstreetmap.org/{osm_type}/{osm_id}

exception in matcher websocket
"""
        mail.send_traceback(info)


def process_match(ws_sock, changeset_id, m):
    osm_type, osm_id = m["osm_type"], m["osm_id"]
    item_id = m["qid"][1:]

    r = edit.get_existing(osm_type, osm_id)
    if r.status_code == 410 or r.content == b"":
        return "deleted"

    osm = ItemCandidate.query.filter_by(
        item_id=item_id, osm_type=osm_type, osm_id=osm_id
    ).one_or_none()

    if b"wikidata" in r.content:
        root = etree.fromstring(r.content)
        existing = root.find('.//tag[@k="wikidata"]')
        if existing is not None:
            osm.tags["wikidata"] = existing.get("v")
            flag_modified(osm, "tags")
            database.session.commit()
            return "already_tagged"

    root = etree.fromstring(r.content)
    tag = etree.Element("tag", k="wikidata", v=m["qid"])
    root[0].set("changeset", changeset_id)
    root[0].append(tag)

    add_wikipedia_tag(root, m)

    element_data = etree.tostring(root)
    try:
        success = edit.save_element(osm_type, osm_id, element_data)
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409 and "Version mismatch" in r.text:
            raise VersionMismatch
        mail.error_mail(
            "error saving element", element_data.decode("utf-8"), e.response
        )
        database.session.commit()
        return "element-error"

    if not success:
        return "element-error"

    osm.tags["wikidata"] = m["qid"]
    flag_modified(osm, "tags")
    # TODO: also update wikipedia tag if appropriate
    db_edit = ChangesetEdit(
        changeset_id=changeset_id, item_id=item_id, osm_id=osm_id, osm_type=osm_type
    )
    database.session.add(db_edit)
    database.session.commit()

    return "saved"


@ws.route("/websocket/add_tags/<osm_type>/<int:osm_id>")
def ws_add_tags(ws_sock, osm_type, osm_id):
    g.user = current_user

    def send(msg_type, **kwars):
        ws_sock.send(json.dumps({"type": msg_type, **kwars}))

    place = None
    try:
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

        changeset_id = reply
        send("open", id=int(changeset_id))

        update_count = 0
        change = edit.record_changeset(
            id=changeset_id, place=place, comment=comment, update_count=update_count
        )

        for num, m in enumerate(data["matches"]):
            send("progress", qid=m["qid"], num=num)
            while True:
                try:
                    result = process_match(ws_sock, changeset_id, m)
                except VersionMismatch:  # FIXME: limit number of attempts
                    continue  # retry
                else:
                    break
            if result == "saved":
                update_count += 1
                change.update_count = update_count
            database.session.commit()
            send(result, qid=m["qid"], num=num)

        send("closing")
        edit.close_changeset(changeset_id)
        send("done")

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
