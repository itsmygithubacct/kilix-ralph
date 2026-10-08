import json
import socket
import struct
import time
import unittest

from kilix_ralph.native import CodexSocket
from kilix_ralph.store import Error


class SocketFrames(unittest.TestCase):
    def setUp(self):
        client, self.server = socket.socketpair()
        self.addCleanup(client.close)
        self.addCleanup(self.server.close)
        self.client = CodexSocket.__new__(CodexSocket)
        self.client.socket = client
        self.client.buffer = bytearray()
        self.client.timeout = .2

    def read(self):
        return self.client.read(time.monotonic() + .2)

    def test_fragmented_text_and_ping(self):
        self.server.sendall(b'\x01\x05{"id"' + b'\x89\x01x' + b'\x80\x03:1}')
        self.assertEqual(self.read(), {"id": 1})
        frame = self.server.recv(100)
        self.assertEqual(frame[0], 0x8a)
        self.assertEqual(frame[1], 0x81)
        self.assertEqual(frame[-1] ^ frame[2], ord("x"))

    def test_batched_messages_do_not_lose_frame_boundary(self):
        self.server.sendall(b'\x81\x08{"id":1}' + b'\x81\x08{"id":2}')
        self.assertEqual(self.read(), {"id": 1})
        self.assertEqual(self.read(), {"id": 2})

    def test_masked_client_frame_roundtrips_unicode(self):
        value = {"text": "é" * 100}
        self.client.send(value)
        raw = self.server.recv(4096)
        self.assertEqual(raw[0], 0x81)
        self.assertEqual(raw[1], 0xfe)
        count = struct.unpack("!H", raw[2:4])[0]
        mask, payload = raw[4:8], raw[8:]
        self.assertEqual(len(payload), count)
        decoded = bytes(v ^ mask[i % 4] for i, v in enumerate(payload))
        self.assertEqual(json.loads(decoded), value)

    def test_oversized_and_invalid_frames_fail_before_body(self):
        self.server.sendall(b'\x81\x7f' + struct.pack("!Q", 17000000))
        with self.assertRaises(Error):
            self.read()

    def test_masked_server_frame_is_refused(self):
        self.server.sendall(b'\x81\x80')
        with self.assertRaises(Error):
            self.read()

    def test_unexpected_continuation_is_refused(self):
        self.server.sendall(b'\x80\x00')
        with self.assertRaises(Error):
            self.read()
