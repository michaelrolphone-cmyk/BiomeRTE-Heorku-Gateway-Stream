"""Verify exact vision records and reliable barriers across a real delayed reverse WebSocket."""
import asyncio
import json
import struct
import unittest
from unittest.mock import patch

from aiohttp import WSMessage, ClientSession, ClientWebSocketResponse, WSServerHandshakeError, WSMsgType, web
from aiohttp.test_utils import TestServer

from biomerte.rover_transport import Outbound, _VisionRelayBuffer


def record(identity, kind="frame", camera="front"):
    header = {"frame_id": identity, "camera_id": camera, "type": kind}
    if kind in ("sensor_tick", "telemetry"):
        header.update(sensor_tick=True, image_available=False)
    metadata = json.dumps(header).encode()
    data = b"\xff\xd8fixture\xff\xd9" if kind == "frame" else b""
    return struct.pack("!I", len(metadata)) + metadata + data


class VisionUplinkFreshness(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.local_socket = asyncio.Queue()
        self.received = asyncio.Queue()
        self.dial = asyncio.Event()
        self.ready = asyncio.Event()
        self.reject_local = False
        self.reject_remote = False
        self.local_closed = asyncio.Event()

        async def local(request):
            if self.reject_local:
                raise web.HTTPServiceUnavailable()
            socket = web.WebSocketResponse()
            await socket.prepare(request)
            await self.local_socket.put(socket)
            try:
                async for _ in socket:
                    pass
            finally:
                self.local_closed.set()
            return socket

        async def remote(request):
            self.dial.set()
            await self.ready.wait()
            if self.reject_remote:
                raise web.HTTPForbidden()
            socket = web.WebSocketResponse()
            await socket.prepare(request)
            async for message in socket:
                await self.received.put(message)
            return socket

        self.urls = []
        for path, handler in [("/api/live/{lane}", local), ("/rover/data/{id}", remote)]:
            app = web.Application()
            app.router.add_get(path, handler)
            server = TestServer(app)
            await server.start_server()
            self.addAsyncCleanup(server.close)
            self.urls.append(str(server.make_url("/")).rstrip("/"))
        self.session = ClientSession()
        self.addAsyncCleanup(self.session.close)
        self.outbound = Outbound(self.urls[1], self.urls[0], "test-only")

    async def open(self, lane):
        task = asyncio.create_task(self.outbound.open(self.session, "scope", "/api/live/" + lane))

        async def stop():
            self.ready.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        self.addAsyncCleanup(stop)
        local = await asyncio.wait_for(self.local_socket.get(), 2)
        await asyncio.wait_for(self.dial.wait(), 2)
        return local, task

    async def read(self):
        return (await asyncio.wait_for(self.received.get(), 2)).data

    def observe_local_read(self, target):
        """Release tests only after the collector actually read the last source message."""
        event = asyncio.Event()
        original = ClientWebSocketResponse.receive

        async def observed(client, *args, **kwargs):
            message = await original(client, *args, **kwargs)
            if message.type == WSMsgType.BINARY and message.data == target:
                event.set()
            return message

        self.enterContext(patch.object(ClientWebSocketResponse, "receive", observed))
        return event

    async def test_delayed_upgrade_retains_bootstrap_and_last_frame(self):
        final = record("newest")
        observed = self.observe_local_read(final)
        local, _ = await self.open("vision")
        manifest = record("manifest", "sensor_manifest")
        world = record("world", "world_snapshot")
        for value in [manifest, world, record("old"), record("older-than-newest"), final]:
            await local.send_bytes(value)
        await asyncio.wait_for(observed.wait(), 2)
        self.ready.set()
        self.assertEqual([await self.read() for _ in range(3)], [manifest, world, final])

    async def test_slow_established_send_preserves_sources_and_reliable_barriers(self):
        in_flight = record("in-flight")
        groups = [[record(str(n), kind, camera)
                   for camera in ("front", "rear") for kind in ("frame", "sensor_tick")]
                  for n in range(4)]
        observed = self.observe_local_read(groups[-1][-1])
        blocked, release = asyncio.Event(), asyncio.Event()
        original = ClientWebSocketResponse.send_bytes

        async def gated(client, value, *args, **kwargs):
            if value == in_flight:
                blocked.set()
                await release.wait()
            return await original(client, value, *args, **kwargs)

        self.enterContext(patch.object(ClientWebSocketResponse, "send_bytes", gated))
        self.ready.set()
        local, _ = await self.open("vision")
        await local.send_bytes(in_flight)
        await asyncio.wait_for(blocked.wait(), 2)
        before = record("before-barrier")
        world, extension = record("world", "world_snapshot"), record("new-protocol", "extension")
        reliable = [world, extension, b"opaque future binary"]
        legacy = record("prior-format", "telemetry", "front")
        for value in [before, *reliable, legacy, *(value for group in groups for value in group)]:
            await local.send_bytes(value)
        await asyncio.wait_for(observed.wait(), 2)
        release.set()
        expected = [in_flight, before, *reliable, *groups[-1]]
        self.assertEqual([await self.read() for _ in expected], expected)

    async def test_map_keeps_every_ordered_frame(self):
        local, _ = await self.open("map")
        messages = [record(str(i)) for i in range(5)]
        for item in messages:
            await local.send_bytes(item)
        self.ready.set()
        self.assertEqual([await self.read() for _ in messages], messages)

    async def test_local_failure_does_not_mark_remote_ready(self):
        self.reject_local = True
        with self.assertRaises(WSServerHandshakeError) as failure:
            await self.outbound.open(self.session, "scope", "/api/live/vision")
        self.assertEqual(failure.exception.status, 503)
        self.assertFalse(self.dial.is_set())

    async def test_failed_reverse_upgrade_closes_local_subscription(self):
        self.reject_remote = True
        _, task = await self.open("vision")
        self.ready.set()
        with self.assertRaises(WSServerHandshakeError) as failure:
            await task
        self.assertEqual(failure.exception.status, 403)
        await asyncio.wait_for(self.local_closed.wait(), 2)


class VisionRelayScheduling(unittest.IsolatedAsyncioTestCase):
    async def make_buffer(self):
        source = asyncio.Queue()
        class Local:
            receive = source.get

        relay = _VisionRelayBuffer()
        collector = asyncio.create_task(relay.collect(Local()))

        async def stop():
            collector.cancel()
            await asyncio.gather(collector, return_exceptions=True)
        self.addAsyncCleanup(stop)

        async def deliver(offers):
            for value in offers:
                source.put_nowait(WSMessage(WSMsgType.BINARY, value, None))
            # Give the actual relay reader the offered burst before the
            # next simulated transport write completes.
            while not source.empty():
                await asyncio.sleep(0)

        return relay, deliver

    async def assert_turns(self, feeds):
        relay, deliver = await self.make_buffer()
        delivered = []
        for cycle in range(24):
            offers = [record(f"{cycle * len(feeds) + n}", kind, camera)
                      for n, (kind, camera) in enumerate(feeds)]
            await deliver(offers)
            wire = (await relay.receive()).data
            header = json.loads(wire[4:4 + int.from_bytes(wire[:4], "big")])
            delivered.append(int(header["frame_id"]))
        self.assertEqual(delivered, sorted(set(delivered)))
        counts = [sum(seq % len(feeds) == n for seq in delivered) for n in range(len(feeds))]
        self.assertEqual(counts, [24 // len(feeds)] * len(feeds))

    async def test_replacing_latest_cannot_starve_a_waiting_jpeg(self):
        await self.assert_turns([("sensor_tick", "rear"), ("frame", "front")])

    async def test_every_camera_and_kind_gets_write_opportunities(self):
        await self.assert_turns([("sensor_tick", "front"), ("frame", "rear"),
                                 ("sensor_tick", "rear"), ("frame", "front")])

    async def test_slow_write_refreshes_the_other_waiting_camera(self):
        relay, deliver = await self.make_buffer()
        ticks = [record(str(i), "sensor_tick", "rear") for i in range(24)]
        jpegs = [record(str(i), "frame", "front") for i in range(24)]
        await deliver([ticks[0], jpegs[0]])
        self.assertEqual((await relay.receive()).data, ticks[0])
        for i in range(1, 24):
            await deliver([ticks[i], jpegs[i]])
        # The new JPEG immediately preceding the last pending tick is the
        # newest that can be sent first without reordering or losing that tick.
        self.assertEqual((await relay.receive()).data, jpegs[-2])
        self.assertEqual((await relay.receive()).data, ticks[-1])
        self.assertEqual((await relay.receive()).data, jpegs[-1])

    async def test_stale_alternative_does_not_replay_after_a_long_blocked_write(self):
        now = 100.
        self.enterContext(patch("biomerte.rover_transport.monotonic", side_effect=lambda: now))
        relay, deliver = await self.make_buffer()
        old_tick = record("served", "sensor_tick", "rear")
        old = record("stale", "frame", "front")
        await deliver([old_tick, old])
        self.assertEqual((await relay.receive()).data, old_tick)
        waiting_tick = record("only-remaining-tick", "sensor_tick", "rear")
        await deliver([waiting_tick])
        now += 5.
        fresh = record("fresh", "frame", "front")
        await deliver([fresh])
        self.assertEqual((await relay.receive()).data, waiting_tick)
        self.assertEqual((await relay.receive()).data, fresh)
