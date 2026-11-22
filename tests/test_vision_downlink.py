"""Public vision clients independently confirm receipt on real WebSockets."""
import asyncio
import unittest
from unittest.mock import patch

from aiohttp import ClientSession, WSMsgType, web
from aiohttp.test_utils import TestServer

from biomerte import rover_transport
from biomerte.rover_transport import Ingress, derived
from biomerte.stream_router import frame as wire_frame
from test_vision_uplink_freshness import record


class PublicVisionFlow(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ingress = Ingress("downlink-fixture")
        app = web.Application()
        self.ingress.mount(app)
        self.server = TestServer(app)
        await self.server.start_server()
        self.addAsyncCleanup(self.server.close)
        self.session = ClientSession()
        self.addAsyncCleanup(self.session.close)
        self.bridge_headers = {"Authorization": "Bridge " + derived("downlink-fixture", "rover-link")}
        self.site_headers = {"Authorization": "Bearer " + derived("downlink-fixture", "rover-site")}
        self.bridge = await self.session.ws_connect(self.server.make_url("/rover/bridge"),
                                                    headers=self.bridge_headers)
        self.addAsyncCleanup(self.bridge.close)

    async def open(self, lane="vision", compress=0):
        attach = asyncio.ensure_future(self.session.ws_connect(
            self.server.make_url("/api/live/" + lane), headers=self.site_headers,
            compress=compress, autoping=False, max_msg_size=0))
        notice = await self.bridge.receive_json(timeout=2)
        self.assertEqual(notice["type"], "open")
        upstream = await self.session.ws_connect(self.server.make_url("/rover/data/" + notice["id"]),
                                                 headers=self.bridge_headers, max_msg_size=0)
        client = await asyncio.wait_for(attach, 2)
        self.addAsyncCleanup(upstream.close)
        self.addAsyncCleanup(client.close)
        return upstream, client

    async def expect(self, socket, kind, value=None):
        message = await socket.receive(timeout=2)
        self.assertEqual(message.type, kind)
        if value is not None:
            self.assertEqual(message.data, value)
        return message.data

    async def test_slow_raw_peer_resumes_at_latest_while_fast_peer_advances(self):
        raw, slow = await self.open()
        other, fast = await self.open()
        manifest = record("manifest", "sensor_manifest")
        world = wire_frame({"type": "world_snapshot", "map": "w" * 219000}, b"")
        for value in [manifest, world]:
            await raw.send_bytes(value)
            await self.expect(slow, WSMsgType.BINARY, value)
        marker = await self.expect(slow, WSMsgType.PING)
        terminal = "ordered text after final image"
        observed = asyncio.Event()
        original = web.WebSocketResponse.receive

        async def monitored(socket, *args, **kwargs):
            message = await original(socket, *args, **kwargs)
            if message.type == WSMsgType.TEXT and message.data == terminal:
                observed.set()
            return message

        self.enterContext(patch.object(web.WebSocketResponse, "receive", monitored))
        latest = []
        for serial in range(16):
            latest = [record(str(serial), kind, camera)
                      for camera in ("front", "rear") for kind in ("frame", "sensor_tick")]
            for value in latest:
                await raw.send_bytes(value)
            frame = record("fast-" + str(serial)) + b"f" * 66000
            await other.send_bytes(frame)
            await self.expect(fast, WSMsgType.BINARY, frame)
            await fast.pong(await self.expect(fast, WSMsgType.PING))
        second_world = record("new-world", "world_snapshot")
        await raw.send_bytes(second_world)
        tail = record("final")
        await raw.send_bytes(tail)
        await raw.send_str(terminal)
        await asyncio.wait_for(observed.wait(), 2)
        # Peer and heartbeat control frames are independent of delivery receipts.
        await slow.pong(b"wrong-marker")
        await slow.ping(b"liveness")
        await self.expect(slow, WSMsgType.PONG, b"liveness")
        await slow.send_str("client text still reaches upstream")
        await self.expect(raw, WSMsgType.TEXT, "client text still reaches upstream")
        await slow.pong(marker)
        for value in [*latest, second_world, tail]:
            await self.expect(slow, WSMsgType.BINARY, value)
        await self.expect(slow, WSMsgType.TEXT, terminal)
        close = asyncio.create_task(raw.close(code=1012, message=b"restart"))
        message = await slow.receive(timeout=2)
        self.assertEqual((message.type, message.data, message.extra), (WSMsgType.CLOSE, 1012, "restart"))
        await close

    async def test_only_matching_cumulative_receipts_free_the_window(self):
        raw, client = await self.open()
        data = b"x" * 65536
        markers = []
        for _ in range(2):
            await raw.send_bytes(data)
            await self.expect(client, WSMsgType.BINARY, data)
            markers.append(await self.expect(client, WSMsgType.PING))
        self.assertNotEqual(*markers)
        await raw.send_bytes(b"after-two")
        # RFC 6455 permits answering only the newest outstanding challenge.
        await client.pong(markers[1])
        await self.expect(client, WSMsgType.BINARY, b"after-two")
        await client.pong(markers[0])
        await raw.send_bytes(data)
        await self.expect(client, WSMsgType.BINARY, data)
        await self.expect(client, WSMsgType.PING)

    async def test_negotiated_vision_and_map_keep_existing_data_and_ping_behavior(self):
        for lane, compress in [("vision", 15), ("map", 0)]:
            raw, client = await self.open(lane, compress)
            for value in (b"x" * 200000, b"second", b"third"):
                await raw.send_bytes(value)
                await self.expect(client, WSMsgType.BINARY, value)
            await client.ping(b"browser")
            await self.expect(client, WSMsgType.PONG, b"browser")

    async def test_stalled_client_pings_cannot_renew_its_credit_or_lane(self):
        self.enterContext(patch.object(rover_transport._PublicVision, "receipt_timeout", 0.2))
        raw, client = await self.open()
        value = b"a" * 150000
        await raw.send_bytes(value)
        await self.expect(client, WSMsgType.BINARY, value)
        await self.expect(client, WSMsgType.PING)

        async def keep_pinging():
            while not client.closed:
                await client.ping(b"alive")
                await asyncio.sleep(.02)

        task = asyncio.create_task(keep_pinging())
        try:
            while True:
                message = await client.receive(timeout=2)
                if message.type != WSMsgType.PONG:
                    break
            self.assertEqual((message.type, message.data), (WSMsgType.CLOSE, 1013))
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await self.expect(raw, WSMsgType.CLOSE)
        notice = await self.bridge.receive_json(timeout=2)
        self.assertEqual(notice["type"], "cancel")
        self.assertFalse(self.ingress.lanes)
