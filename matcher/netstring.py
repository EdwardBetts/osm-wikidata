def write(sock, to_send):
    sock.sendall(b'%d' % len(to_send))
    sock.sendall(b':')
    sock.sendall(to_send.encode('utf-8'))
    sock.sendall(b',')

def read(sock):
    char = sock.recv(1)
    if char == b'':
        return
    buf = b''
    while char != b':':
        if not char.isdigit():
            print('char:', repr(char))
        assert char.isdigit()
        buf += char
        char = sock.recv(1)
    byte_len = int(buf.decode('ASCII'))
    buf = b''
    while len(buf) < byte_len:
        buf += sock.recv(byte_len - len(buf))
    assert len(buf) == byte_len
    char = sock.recv(1)
    while char == b'':
        char = sock.recv(1)
    assert char == b','
    return buf.decode('utf-8')
