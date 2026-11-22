"""Exercise the Mac's actual outbound WebSocket negotiation and message delivery."""
import asyncio
import contextlib
import json
import struct
import unittest

from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer

from biomerte.rover_transport import Outbound


def wire(metadata, payload=b""):
    header = json.dumps(metadata, separators=(",", ":")).encode()
    return struct.pack("!I", len(header)) + header + payload


class VisionUplinkNegotiation(unittest.IsolatedAsyncioTestCase):
    async def assert_link(self, lane, server_accepts, expected_compression):
        # A retained world bootstrap and fresh frame have different delivery
        # policies, but both must survive the negotiated link byte for byte.
        messages = [
            wire({"type": "world_snapshot", "features": ["landmark"] * 24000}),
            wire({"type": "frame", "camera_id": "front", "frame_id": "boot:1"},
                 b"\xff\xd8" + bytes(range(256)) * 150 + b"\xff\xd9"),
        ]
        receipt = asyncio.get_running_loop().create_future()

        async def local(request):
            ws = web.WebSocketResponse()
            await ws.prepare(request)
            for value in messages:
                await ws.send_bytes(value)
            await ws.receive()
            return ws

        async def remote(request):
            ws = web.WebSocketResponse(compress=server_accepts)
            await ws.prepare(request)
            values = [await ws.receive_bytes() for _ in messages]
            receipt.set_result((bool(ws.compress), values))
            await ws.close()
            return ws

        local_app = web.Application()
        local_app.router.add_get("/api/live/{lane}", local)
        remote_app = web.Application()
        remote_app.router.add_get("/rover/data/{id}", remote)
        async with TestServer(local_app) as local_server, TestServer(remote_app) as remote_server:
            outbound = Outbound(str(remote_server.make_url("/")), str(local_server.make_url("/")), "s" * 64)
            async with ClientSession() as session:
                task = asyncio.create_task(outbound.open(session, "viewer", "/api/live/" + lane))
                try:
                    compression, received = await asyncio.wait_for(receipt, timeout=5)
                    self.assertEqual(received, messages)
                    self.assertEqual(compression, expected_compression)
                finally:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task

    async def test_vision_negotiates_lossless_compression(self):
        await self.assert_link("vision", True, True)

    async def test_vision_with_a_peer_that_declines_compression(self):
        await self.assert_link("vision", False, False)

    async def test_control_keeps_its_existing_negotiation(self):
        await self.assert_link("control", True, False)
