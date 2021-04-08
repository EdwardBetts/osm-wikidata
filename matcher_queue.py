#!/usr/bin/python3

import threading
import socketserver
import json
import os.path
import requests.exceptions
import queue

from matcher import (
    database,
    mail,
    overpass,
    space_alert,
    chat,
)
from time import sleep
from matcher.view import app
from matcher.job_queue import JobManager

app.config.from_object("config.default")
database.init_app(app)

job_manager = JobManager()


def wait_for_slot(send_queue):
    print("get status")
    try:
        status = overpass.get_status()
    except overpass.OverpassError as e:
        r = e.args[0]
        body = f"URL: {r.url}\n\nresponse:\n{r.text}"
        mail.send_mail("Overpass API unavailable", body)
        send_queue.put({"type": "error", "msg": "Can't access overpass API"})
        return False
    except requests.exceptions.Timeout:
        body = "Timeout talking to overpass API"
        mail.send_mail("Overpass API timeout", body)
        send_queue.put({"type": "error", "msg": "Can't access overpass API"})
        return False

    print("status:", status)
    if not status["slots"]:
        return True
    secs = status["slots"][0]
    if secs <= 0:
        return True
    send_queue.put({"type": "status", "wait": secs})
    sleep(secs)
    return True


def to_client(send_queue, msg_type, msg):
    msg["type"] = msg_type
    send_queue.put(msg)


def process_queue_loop():
    with app.app_context():
        while True:
            process_queue()


def process_queue():
    area, item = job_manager.get_next_job()
    place = item["place"]
    send_queue = item["queue"]
    for num, chunk in enumerate(item["chunks"]):
        oql = chunk.get("oql")
        if not oql:
            continue
        filename = "overpass/" + chunk["filename"]
        msg = {
            "num": num,
            "filename": chunk["filename"],
            "place": place,
        }
        if not os.path.exists(filename):
            space_alert.check_free_space(app.config)
            if not wait_for_slot(send_queue):
                return
            to_client(send_queue, "run_query", msg)
            while True:
                print("run query")
                try:
                    r = overpass.run_query(oql)
                    break
                except overpass.RateLimited:
                    print("rate limited")
                    wait_for_slot(send_queue)

            print("query complete")
            with open(filename, "wb") as out:
                out.write(r.content)
            space_alert.check_free_space(app.config)
        print(msg)
        to_client(send_queue, "chunk", msg)
    print("item complete")
    send_queue.put({"type": "done"})


def get_pins(place):
    """ Build pins from items in database. """
    pins = []
    for item in place.items:
        lat, lon = item.coords()
        pin = {
            "qid": item.qid,
            "lat": lat,
            "lon": lon,
            "label": item.label(),
        }
        if item.tags:
            pin["tags"] = list(item.tags)
        pins.append(pin)
    return pins


class RequestHandler(socketserver.BaseRequestHandler):
    def send_msg(self, msg):
        return chat.send_json(self.request, msg)

    def match_place(self, msg):
        osm_type, osm_id = msg["osm_type"], msg["osm_id"]
        t = threading.current_thread()
        job_need_start = False
        if not self.job_thread:
            job_need_start = True
            kwargs = {
                key: msg.get(key) for key in ("user", "remote_addr", "user_agent")
            }

            kwargs["want_isa"] = set(msg.get("want_isa") or [])
            self.job_thread = job_manager.new_job(osm_type, osm_id, **kwargs)

        status_queue = queue.Queue()
        updates = self.job_thread.subscribe(t.name, status_queue)

        if job_need_start:
            self.job_thread.start()

        while True:
            msg = updates.get()
            try:
                self.send_msg(msg)
                if msg["type"] in ("done", "error"):
                    break
            except BrokenPipeError:
                self.job_thread.unsubscribe(t.name)
                break

    def handle_message(self, msg):
        print(f"handle: {msg!r}")
        if msg == "ping":
            self.send_msg({"type": "pong"})
            return
        if msg.startswith("match"):
            json_msg = json.loads(msg[6:])
            self.job_thread = job_manager.get_job(
                json_msg["osm_type"], json_msg["osm_id"]
            )
            return self.match_place(json_msg)
        if msg == "jobs":
            self.send_msg({"type": "jobs", "items": job_manager.job_list()})
            return
        if msg.startswith("stop"):
            json_msg = json.loads(msg[5:])
            job_manager.stop_job(json_msg["osm_type"], json_msg["osm_id"])
            self.send_msg({"type": "stop", "success": True})
            return

    def handle(self):
        print("New connection from %s:%s" % self.client_address)
        msg = chat.read_line(self.request)

        with app.app_context():
            try:
                return self.handle_message(msg)
            except Exception as e:
                error_str = f"{type(e).__name__}: {e}"
                self.send_msg({"type": "error", "msg": error_str})

                info = "matcher queue"
                mail.send_traceback(info, prefix="matcher queue")


def main():
    HOST, PORT = "localhost", 6030

    overpass_thread = threading.Thread(target=process_queue_loop)
    overpass_thread.daemon = True
    overpass_thread.start()

    socketserver.ThreadingTCPServer.allow_reuse_address = True
    server = socketserver.ThreadingTCPServer((HOST, PORT), RequestHandler)
    ip, port = server.server_address

    server_thread = threading.Thread(target=server.serve_forever)
    server_thread.name = "server thread"

    server_thread.start()
    print("Server loop running in thread:", server_thread.name)
    server_thread.join()


if __name__ == "__main__":
    main()
