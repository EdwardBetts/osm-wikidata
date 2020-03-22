import json
import socket

def connect_to_queue():
    address = ('localhost', 6030)
    sock = socket.create_connection(address)
    sock.setblocking(True)
    return sock

def read_line(sock):
    char = sock.recv(1)
    if char == b'':
        return
    buf = b''
    while char != b'\r':
        buf += char
        char = sock.recv(1)

    assert sock.recv(1) == b'\n'
    return buf.decode('utf-8')

def send_msg(sock, msg):
    return sock.sendall(msg.encode('utf-8') + b'\r\n')

def send_json(sock, msg):
    return send_msg(sock, json.dumps(msg))

def send_command(sock, cmd, **params):
    if params:
        msg = cmd + ' ' + json.dumps(params)
    else:
        msg = cmd
    return send_msg(sock, msg)

def read_json_line(sock):
    line = read_line(sock)
    if line:
        return json.loads(line)
