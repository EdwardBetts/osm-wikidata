"""Protocol for communication over a socket."""

import json
import socket
from typing import Any


def connect_to_queue() -> socket.socket:
    """Connect to the matcher queue."""
    address = ("localhost", 6030)
    sock = socket.create_connection(address)
    sock.setblocking(True)
    return sock


def read_line(sock: socket.socket) -> str | None:
    """Read a single line from a socket. The line is terminated with CR+LF."""
    char = sock.recv(1)
    if char == b"":
        return None
    buf = b""
    while char != b"\r":
        buf += char
        char = sock.recv(1)

    assert sock.recv(1) == b"\n"
    return buf.decode("utf-8")


def send_msg(sock: socket.socket, msg: str) -> None:
    """Send a message over a socket."""
    return sock.sendall(msg.encode("utf-8") + b"\r\n")


def send_json(sock: socket.socket, msg: Any) -> None:
    """Send JSON over a socket."""
    return send_msg(sock, json.dumps(msg))


def send_command(sock: socket.socket, cmd: str, **params: dict[Any, Any]) -> None:
    """Send a command over a socket."""
    if params:
        msg = cmd + " " + json.dumps(params)
    else:
        msg = cmd
    return send_msg(sock, msg)


def read_json_line(sock: socket.socket) -> Any | None:
    """Read a line of JSON from a socket."""
    line = read_line(sock)
    return json.loads(line) if line else None
