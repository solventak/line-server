import socket
import pytest


def checksum(frame: bytes) -> int:
    return sum(frame) % 256


class Client:
    QUIT_FRAME = b"1\x00\x00\x00\x00" + bytes([checksum(b"1\x00\x00\x00\x00")]) + b"\n"
    SHUTDOWN_FRAME = (
        b"2\x00\x00\x00\x00" + bytes([checksum(b"2\x00\x00\x00\x00")]) + b"\n"
    )

    s: socket.socket

    def __init__(self, port: int):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect(("localhost", port))

    def recvall(self) -> list[bytes]:
        resp = []
        while len(resp) < 2:
            data = self.s.recv(1024)
            resp += data.strip().split(b"\n")
            if b"ERR" in resp:
                break
        return resp

    def quit(self):
        self.s.sendall(Client.QUIT_FRAME)
        self.s.close()

    def shutdown(self):
        shutdown_frame = (
            b"2\x00\x00\x00\x00" + bytes([checksum(b"2\x00\x00\x00\x00")]) + b"\n"
        )
        self.s.sendall(shutdown_frame)
        while True:
            pass

    def make_request(self, frame: bytes, quit: bool = True, calc_checksum: bool = True) -> list[bytes]:
        bytes_to_send = frame
        if calc_checksum:
            bytes_to_send += bytes([checksum(frame)])
        bytes_to_send += b"\n"
        self.s.sendall(bytes_to_send)
        resp = self.recvall()
        if quit:
            self.quit()
        return resp


@pytest.fixture
def client():
    return Client(10497)


def test_ok(client):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", 10497))
    frame = b"0\x00" + bytes([0x20, 0xD9, 0x6C])  # 20D96C
    resp = client.make_request(frame)
    assert resp[0] == b"OK"


def test_out_of_bounds(client):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", 10497))
    frame = b"0" + bytes([0xFF, 0xFF, 0xFF, 0xFF])  # 20D96C
    resp = client.make_request(frame)
    assert resp[0] == b"ERR"


def test_zero_index(client):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", 10497))
    frame = b"0\x00\x00\x00\x00"
    resp = client.make_request(frame)
    assert resp[0] == b"ERR"
    s.close()


def test_starts_at_first_line(client):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", 10497))
    frame = b"0\x00\x00\x00\x01"
    resp = client.make_request(frame)
    assert resp[0] == b"OK"
    assert resp[1] == b"Lorem ipsum dolor sit amet, consectetur adipiscing elit,"


def test_invalid_command(client):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", 10497))
    frame = b"3\x00\x00\x00\x00"
    resp = client.make_request(frame)
    assert resp[0] == b"ERR"


def test_invalid_checksum(client):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", 10497))
    frame = b"0\x00\x00\x00\x01\x02"
    resp = client.make_request(frame, calc_checksum=False)
    assert resp[0] == b"ERR"


def test_invalid_frame(client):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", 10497))
    frame = b"0\x00\x00\x00"
    resp = client.make_request(frame)
    assert resp[0] == b"ERR"
