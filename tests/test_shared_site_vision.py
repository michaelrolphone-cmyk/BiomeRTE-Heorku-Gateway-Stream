"""Exercise the authenticated shared and legacy routes using real public and reverse sockets."""
import asyncio
import unittest

from aiohttp import ClientSession, WSMsgType, WSServerHandshakeError, web
from aiohttp.test_utils import TestServer

from biomerte.rover_transport import Ingress, Outbound, derived
from biomerte.stream_router import frame


def packet(kind, number, *, sequence=None, data=b""):
    header = {"type": kind, "camera_id": "front", "frame_id": f"boot:{number}",
              "sequence": number if sequence is None else sequence}
    if kind == "sensor_tick":
        header.update(sensor_tick=True, image_available=False)
    return frame(header, data or (b"jpeg" if kind == "frame" else b""))


class SharedSiteVisionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ingress = Ingress("site-vision")
        app = web.Application()
        self.ingress.mount(app)
        self.server = TestServer(app)
        await self.server.start_server()
        self.addAsyncCleanup(self.server.close)
        self.session = ClientSession()
        self.addAsyncCleanup(self.session.close)
        self.token = {"Authorization": "Bearer " + derived("site-vision", "rover-site")}
        self.credentials = {"Authorization": "Bridge " + derived("site-vision", "rover-link")}

    async def connect(self, shared=True):
        headers = dict(self.credentials)
        if shared:
            headers["X-Bridge-Capabilities"] = "shared_site_vision_v1"
        self.bridge = await self.session.ws_connect(self.server.make_url("/rover/bridge"), headers=headers)
        self.addAsyncCleanup(self.bridge.close)

    async def notice(self, kind):
        while True:
            data = await self.bridge.receive_json(timeout=2)
            if data["type"] == kind:
                return data

    async def open(self, lane="vision", compress=15):
        client = asyncio.ensure_future(self.session.ws_connect(
            self.server.make_url("/api/live/" + lane), headers=self.token, compress=compress,
            autoping=False, max_msg_size=0))
        notice = await self.notice("open")
        self.assertEqual(notice["path"], "/api/live/" + lane)
        origin = await self.session.ws_connect(self.server.make_url("/rover/data/" + notice["id"]),
                                               headers=self.credentials, max_msg_size=0)
        self.addAsyncCleanup(origin.close)
        ws = await asyncio.wait_for(client, 2)
        self.addAsyncCleanup(ws.close)
        return origin, ws

    async def read(self, ws, kind, value=None):
        msg = await ws.receive(timeout=2)
        while msg.type == WSMsgType.PING and kind != WSMsgType.PING:
            await ws.pong(msg.data)
            msg = await ws.receive(timeout=2)
        self.assertEqual(msg.type, kind)
        if value is not None:
            self.assertEqual(msg.data, value)
        return msg

    async def deliver(self, upstream, clients, value):
        await upstream.send_bytes(value)
        for client in clients:
            await self.read(client, WSMsgType.BINARY, value)

    async def cleanup(self):
        async with asyncio.timeout(2):
            while self.ingress.lanes or self.ingress.vision_viewers:
                await asyncio.sleep(.001)

    async def test_shared_payload_once_with_fresh_per_viewer_bootstrap(self):
        await self.connect()
        primary, first = await self.open()
        manifest = packet("sensor_manifest", 1)
        world = packet("world_snapshot", 20)
        for value in [manifest, world]:
            await self.deliver(primary, [first], value)
        origin, second = await self.open()
        # Only the newly opened origin knows the newer map-lane-only world.
        newer = packet("world_snapshot", 91)
        for value in [manifest, newer]:
            await self.deliver(origin, [second], value)
        # A fresh mailbox can renumber a vision packet after the newer map
        # sequence. The same frame ID and JPEG are insufficient proof.
        await self.deliver(primary, [first], packet("frame", 42))
        await self.deliver(origin, [second], packet("frame", 42, sequence=92))
        self.assertEqual(len(self.ingress.lanes), 2)
        matched = packet("frame", 100)
        await self.deliver(primary, [first], matched)
        barrier = packet("world_snapshot", 101)
        await self.deliver(primary, [first], barrier)
        await self.deliver(origin, [second], matched)
        await self.read(second, WSMsgType.BINARY, barrier)
        await self.read(origin, WSMsgType.CLOSE)
        await self.notice("cancel")
        self.assertEqual(len(self.ingress.lanes), 1)
        self.assertEqual(len(self.ingress.vision_viewers), 2)
        # The same complete application data arrives at both public peers from
        # exactly one measured copy on the reverse WAN socket.
        traffic = [packet("frame", n, data=b"j" * 65536) for n in range(102, 112)]
        for value in traffic:
            await self.deliver(primary, [first, second], value)
        reverse = sum(map(len, traffic))
        self.assertGreater(reverse, 655360)
        await first.close()
        await self.deliver(primary, [second], packet("sensor_tick", 112))
        await second.close()
        await self.read(primary, WSMsgType.CLOSE)
        await self.cleanup()

    async def test_simultaneous_empty_sources_dedicated_first_and_admission(self):
        await self.connect()
        self.ingress.limit = 2
        with self.assertRaises(WSServerHandshakeError) as denied:
            await self.session.ws_connect(self.server.make_url("/api/live/vision"))
        self.assertEqual(denied.exception.status, 401)
        primary, first = await self.open()
        warm, second = await self.open()
        # A source after a grant reset can be empty. Both handshakes complete
        # without the first subscriber's bootstrap being replayed to the new one.
        await second.ping(b"empty-origin")
        await self.read(second, WSMsgType.PONG, b"empty-origin")
        matched = packet("sensor_tick", 1)
        await self.deliver(warm, [second], matched)
        await self.deliver(primary, [first], matched)
        await self.read(warm, WSMsgType.CLOSE)
        await self.notice("cancel")
        with self.assertRaises(WSServerHandshakeError) as full:
            await self.session.ws_connect(self.server.make_url("/api/live/map"), headers=self.token)
        self.assertEqual(full.exception.status, 429)
        await second.send_str("ignored application input")
        await second.ping(b"alive")
        await self.read(second, WSMsgType.PONG, b"alive")
        await self.deliver(primary, [first, second], packet("frame", 2))
        await second.close()
        await self.deliver(primary, [first], packet("frame", 3))
        await first.close()
        await self.read(primary, WSMsgType.CLOSE)
        await self.cleanup()

    async def test_shared_vision_isolation_with_withheld_raw_delivery_credit(self):
        await self.connect()
        primary, fast = await self.open()
        warm, slow = await self.open(compress=0)
        anchor = packet("frame", 1)
        await self.deliver(primary, [fast], anchor)
        await self.deliver(warm, [slow], anchor)
        await self.read(warm, WSMsgType.CLOSE)
        await self.notice("cancel")
        large = packet("world_snapshot", 2, data=b"w" * 219000)
        await self.deliver(primary, [fast, slow], large)
        marker = (await self.read(slow, WSMsgType.PING)).data
        for n in range(3, 32):
            await self.deliver(primary, [fast], packet("frame", n))
        barrier = packet("world_snapshot", 32)
        await self.deliver(primary, [fast], barrier)
        await slow.pong(marker)
        await self.read(slow, WSMsgType.BINARY, packet("frame", 31))
        await self.read(slow, WSMsgType.BINARY, barrier)

    async def test_legacy_and_control_preserve_per_viewer_inputs_and_origins(self):
        self.assertNotIn("X-Bridge-Capabilities", Outbound("public", "local", "s").headers)
        both = Outbound("public", "local", "s", "ws://localhost:8751", shared_site_vision=True)
        self.assertEqual(set(both.headers["X-Bridge-Capabilities"].split(",")),
                         {"native_router", "owner_inspect", "shared_site_vision_v1"})
        await self.connect(shared=False)
        a, first = await self.open()
        b, second = await self.open()
        for origin, client in [(a, first), (b, second)]:
            await self.deliver(origin, [client], packet("frame", 1))
            await client.send_str("dedicated input")
            await self.read(origin, WSMsgType.TEXT, "dedicated input")
        self.assertEqual(len(self.ingress.lanes), 2)
        self.assertIsNone(self.ingress.site_vision)
        await self.bridge.close()
        await self.read(first, WSMsgType.CLOSE)
        await self.read(second, WSMsgType.CLOSE)
        await self.read(a, WSMsgType.CLOSE)
        await self.read(b, WSMsgType.CLOSE)
        await self.connect()
        for lane in ("map", "control"):
            origin, client = await self.open(lane)
            await client.send_str("unchanged")
            await self.read(origin, WSMsgType.TEXT, "unchanged")
            self.assertIsNone(self.ingress.site_vision)
            await client.close()
            await self.read(origin, WSMsgType.CLOSE)

    async def test_first_viewer_leaves_before_other_origins_align(self):
        await self.connect()
        main, first = await self.open()
        a, second = await self.open()
        b, third = await self.open()
        await first.close()
        await self.read(main, WSMsgType.CLOSE)
        for origin, client in ((a, second), (b, third)):
            await self.deliver(origin, [client], packet("frame", 47))
        # The common lane may be either remaining origin; exactly one retires.
        async with asyncio.timeout(2):
            while len(self.ingress.lanes) != 1:
                await asyncio.sleep(.001)
        owner = self.ingress.site_vision.origin
        self.assertFalse(owner.done())
        peers = [(a, second), (b, third)]
        readers = {asyncio.create_task(ws.receive(timeout=2)): ws for ws, _ in peers}
        done, remaining = await asyncio.wait(readers, return_when=asyncio.FIRST_COMPLETED)
        retired = done.pop()
        self.assertEqual(retired.result().type, WSMsgType.CLOSE)
        retained = remaining.pop()
        surviving = readers[retained]
        retained.cancel()
        await asyncio.gather(retained, return_exceptions=True)
        await self.deliver(surviving, [second, third], packet("frame", 48))

    async def test_parallel_opens_never_replay_old_anchor_or_dead_origin_history(self):
        await self.connect()
        tasks = [asyncio.ensure_future(self.session.ws_connect(
            self.server.make_url("/api/live/vision"), headers=self.token, compress=15))
            for _ in range(2)]
        sockets = []
        for _ in tasks:
            notice = await self.notice("open")
            source = await self.session.ws_connect(self.server.make_url("/rover/data/" + notice["id"]),
                                                   headers=self.credentials)
            sockets.append(source)
            self.addAsyncCleanup(source.close)
        clients = await asyncio.gather(*tasks)
        for ws in clients:
            self.addAsyncCleanup(ws.close)
        # Whichever request won the common lane is identified by an actual
        # origin record, not its place in a scheduling-dependent task list.
        old = packet("frame", 6)
        await sockets[0].send_bytes(old)
        pending = {asyncio.create_task(ws.receive(timeout=2)): ws for ws in clients}
        done, outstanding = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        delivered = done.pop()
        self.assertEqual(delivered.result().data, old)
        first = pending[delivered]
        extra = outstanding.pop()
        second = pending[extra]
        extra.cancel()
        await asyncio.gather(extra, return_exceptions=True)
        # End the common source while the second reader still has an
        # independent origin. An exact old anchor on that still-live origin
        # must not migrate it onto the ended source.
        closing = asyncio.create_task(sockets[0].close(code=1012, message=b"common restart"))
        self.assertEqual((await self.read(first, WSMsgType.CLOSE)).data, 1012)
        await closing
        await self.deliver(sockets[1], [second], old)
        await self.deliver(sockets[1], [second], packet("frame", 7))
        await second.close()
        await self.read(sockets[1], WSMsgType.CLOSE)
        await self.cleanup()

    async def test_replacement_and_canonical_source_close(self):
        await self.connect()
        primary, first = await self.open()
        await self.deliver(primary, [first], packet("sensor_manifest", 400))
        await self.bridge.close()
        await self.read(first, WSMsgType.CLOSE)
        await self.read(primary, WSMsgType.CLOSE)
        await self.cleanup()
        await self.connect()
        upstream, replacement = await self.open()
        await replacement.ping(b"new-generation")
        await self.read(replacement, WSMsgType.PONG, b"new-generation")
        await self.deliver(upstream, [replacement], packet("sensor_manifest", 1))
        closing = asyncio.create_task(upstream.close(code=1012, message=b"source restarted"))
        msg = await self.read(replacement, WSMsgType.CLOSE)
        self.assertEqual((msg.data, msg.extra), (1012, "source restarted"))
        await closing
        await self.cleanup()
