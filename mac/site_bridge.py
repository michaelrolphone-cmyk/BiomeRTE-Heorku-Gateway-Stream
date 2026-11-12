"""Authenticated Site adapter for biomerte.rover.Rover (Python 3.9+).

Native leases belong to Rover.start/close, never a browser subscription. This
adapter deliberately has no controller polling or per-message persistence.
"""
import asyncio
import contextlib
import hmac
import json
import math
import time
import uuid
from aiohttp import web
if __package__:
    from .camera_light import CameraLight
else:
    from camera_light import CameraLight


class ControlOutbox:
    """Bounded latest-value feedback; only this task writes a control socket.

    Browser input handling never waits for a slow downstream receipt or ACK.
    Rejections and state changes have their own slots, so an ACK burst cannot
    evict a release/repress instruction or a fresh binding.
    """
    priority = ('state', 'rejected', 'clock', 'accepted', 'ack')

    def __init__(self, socket, metrics):
        self.socket, self.metrics = socket, metrics
        self.pending = {}
        self.wake = asyncio.Event()

    def offer(self, kind, value):
        if kind not in self.priority:
            raise ValueError('Unknown control feedback kind')
        self.pending[kind] = value
        self.wake.set()

    async def run(self):
        while True:
            await self.wake.wait()
            while self.pending:
                kind = next(k for k in self.priority if k in self.pending)
                value = self.pending.pop(kind)
                started = time.monotonic()
                try:
                    if isinstance(value, bytes):
                        await self.socket.send_bytes(value)
                    elif isinstance(value, str):
                        await self.socket.send_str(value)
                    else:
                        await self.socket.send_json(value)
                finally:
                    self.metrics['max_reply_write_ms'] = max(
                        self.metrics['max_reply_write_ms'],
                        (time.monotonic() - started) * 1000)
            self.wake.clear()


class SiteBridge:
    def __init__(self, rover, token, channels, snapshot=None, intent_ttl_ms=500):
        if not isinstance(token, str) or len(token) < 32:
            raise ValueError('A private bridge token of at least 32 characters is required')
        if (set(channels) != {'forward', 'reverse', 'left', 'right'} or
                any(type(n) is not int or not 1 <= n <= 6 for n in channels.values()) or
                len(set(channels.values())) != 4):
            raise ValueError('Four distinct physical direction channels are required')
        if type(intent_ttl_ms) is not int or not 20 <= intent_ttl_ms <= 1000:
            raise ValueError('Fresh intent lifetime must be 20..1000 milliseconds')
        self.intent_ttl_ms = intent_ttl_ms
        self.rover, self.token, self.channels = rover, token, dict(channels)
        self.snapshot = snapshot
        self.camera_light = CameraLight()
        self.epoch = uuid.uuid4().hex
        self.stop_generation = 0
        self.clients = set()
        self.monitor = None
        self.signature = None
        self.control_metrics = dict(accepted_inputs=0, max_ingress_gap_ms=0.,
                                    max_dispatch_ms=0., max_reply_write_ms=0.)

    def state(self):
        return dict(type='state', epoch=self.epoch, stop_generation=self.stop_generation,
                    ready=bool(self.rover.control_ready), channels=self.channels,
                    server_time=time.time(), lease_owner='server', intent_ttl_ms=self.intent_ttl_ms,
                    control_transport=dict(self.control_metrics),
                    control_clock=self.rover.clock.status() if getattr(self.rover,'clock',None) else None,
                    native={k:self.rover.descriptors.get('device.outputs.stream:1', {}).get(k)
                            for k in ('authority', 'generation', 'epoch')})

    def synchronize(self):
        # A lost native control connection invalidates every browser's old input.
        descriptor = self.rover.descriptors.get('device.outputs.stream:1', {})
        signature = (bool(self.rover.control_ready), descriptor.get('epoch'))
        if signature != self.signature:
            self.signature = signature
            self.epoch = uuid.uuid4().hex
            self.stop_generation += 1
            self.rover.discard_intent()
            self.broadcast(self.state())

    def broadcast(self, value):
        for queue in self.clients:
            if queue.full():
                queue.get_nowait()
            queue.put_nowait(value)

    def stop(self):
        self.stop_generation += 1
        self.rover.discard_intent()
        receipt = self.rover.desired(0, self.intent_ttl_ms) if self.rover.control_ready else None
        value = dict(self.state(), accepted=receipt, applied=False)
        self.broadcast(value)
        return value

    def accept(self, value, previous):
        self.synchronize()
        seq = value.get('sequence')
        if type(seq) is not int or seq <= previous:
            raise ValueError('Old or duplicate browser sequence')
        if value.get('type') == 'stop':
            return seq, self.stop()
        if value.get('type') != 'intent':
            raise ValueError('Unknown browser stream message')
        if value.get('epoch') != self.epoch or value.get('stop_generation') != self.stop_generation:
            raise ValueError('Superseded browser control epoch')
        issued = value.get('issued_at')
        if type(issued) not in (int, float) or not 0 <= time.time() - issued <= .5:
            raise ValueError('Expired browser intent')
        directions = value.get('directions')
        if (not isinstance(directions, dict) or set(directions) != set(self.channels) or
                any(type(v) is not bool for v in directions.values())):
            raise ValueError('Expected four Boolean directions')
        desired = dict(directions)
        for a, b in (('forward', 'reverse'), ('left', 'right')):
            if desired[a] and desired[b]:
                desired[a] = desired[b] = False
        mask = sum(1 << (self.channels[k] - 1) for k, active in desired.items() if active)
        receipt = self.rover.desired(mask, self.intent_ttl_ms)
        return seq, dict(type='accepted', browser_sequence=seq, **receipt)

    async def watch(self):
        while True:
            self.synchronize()
            await asyncio.sleep(.1)

    def app(self):
        @web.middleware
        async def authenticate(request, handler):
            supplied = request.headers.get('Authorization', '')
            if not hmac.compare_digest(supplied, 'Bearer ' + self.token):
                raise web.HTTPUnauthorized()
            # Only the Site Worker has this token. It authenticates its users
            # and checks their browser origin before forwarding any request.
            return await handler(request)

        async def state(request):
            self.synchronize()
            return web.json_response(self.state())

        async def controls(request):
            body = await request.json()
            if isinstance(body, dict) and set(body) == {'led'}:
                return await self.camera_light.handle(request)
            if body != {'stop': True}:
                raise web.HTTPBadRequest(text='Directions require the live socket')
            return web.json_response(self.stop())

        async def snapshot(request):
            body = await request.json()
            if body.get('world') is True:
                if __package__:
                    from .world_snapshot import world_snapshot
                else:
                    from world_snapshot import world_snapshot
                return await world_snapshot(body)
            if self.snapshot is None:
                return web.json_response({'error': {'code': 'MAP_SNAPSHOT_UNAVAILABLE',
                    'message': 'A revision-pinned map snapshot adapter is not configured.'}}, status=503)
            # Backend callback must return a complete map pinned to map_id and
            # revision, or reject. Never substitute unpinned retained geometry.
            value = await self.snapshot(body.get('map_id'), body.get('revision'))
            return web.json_response(value)

        async def live(request):
            lane = request.match_info['lane']
            if lane not in ('vision', 'map', 'control'):
                raise web.HTTPNotFound()
            socket = web.WebSocketResponse(heartbeat=20, max_msg_size=4096)
            await socket.prepare(request)
            status_queue = asyncio.Queue(1)
            if lane == 'control':
                self.clients.add(status_queue)
                self.synchronize()
                if status_queue.empty():status_queue.put_nowait(self.state())
            outbox = ControlOutbox(socket, self.control_metrics) if lane == 'control' else None
            async with self.rover.subscribe() as queues:
                async def send(queue):
                    while True:
                        value = await queue.get()
                        if isinstance(value, bytes):await socket.send_bytes(value)
                        elif isinstance(value, str):await socket.send_str(value)
                        else:await socket.send_json(value)
                async def collect(queue, kind):
                    while True:
                        outbox.offer(kind, await queue.get())
                async def receive():
                    previous = -1
                    last_input = None
                    previously_held = False
                    async for message in socket:
                        if lane != 'control':continue
                        if message.type != web.WSMsgType.TEXT:continue
                        value = None
                        try:
                            value = json.loads(message.data)
                            if isinstance(value, dict) and value.get('type') == 'clock':
                                if type(value.get('id')) is not int or not 0 <= value['id'] <= 9007199254740991:
                                    raise ValueError('Invalid control clock request')
                                outbox.offer('clock', {'type':'clock', 'id':value['id'], 'server_time':time.time()})
                                continue
                            received = time.monotonic()
                            previous, receipt = self.accept(value, previous)
                            if receipt.get('type') == 'accepted':
                                metrics = self.control_metrics
                                metrics['accepted_inputs'] += 1
                                if last_input is not None and previously_held:
                                    metrics['max_ingress_gap_ms'] = max(
                                        metrics['max_ingress_gap_ms'], (received-last_input)*1000)
                                metrics['max_dispatch_ms'] = max(
                                    metrics['max_dispatch_ms'], (time.monotonic()-received)*1000)
                                last_input = received
                                previously_held = any(value['directions'].values())
                            outbox.offer(receipt['type'], receipt)
                        except (ValueError, KeyError, TypeError, RuntimeError) as error:
                            reasons = {'Old or duplicate browser sequence', 'Unknown browser stream message',
                                       'Superseded browser control epoch', 'Expired browser intent',
                                       'Expected four Boolean directions', 'Control stream unavailable',
                                       'Invalid control clock request'}
                            reason = str(error) if str(error) in reasons else 'Invalid control input'
                            detail = ''
                            issued = value.get('issued_at') if isinstance(value, dict) else None
                            if reason == 'Expired browser intent' and type(issued) in (int, float) and math.isfinite(issued):
                                detail = ' (age ' + str(round((time.time()-issued)*1000)) + ' ms)'
                            outbox.offer('rejected', {'type': 'rejected', 'message': reason + detail + '; release and press again.'})
                tasks = [asyncio.create_task(receive())]
                if lane == 'control':
                    tasks.extend((asyncio.create_task(outbox.run()),
                                  asyncio.create_task(collect(queues[lane], 'ack')),
                                  asyncio.create_task(collect(status_queue, 'state'))))
                else:
                    tasks.append(asyncio.create_task(send(queues[lane])))
                try:
                    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                    for task in done:task.result()
                finally:
                    self.clients.discard(status_queue)
                    for task in tasks:task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
            return socket

        app = web.Application(middlewares=[authenticate], client_max_size=0)
        app.router.add_post('/api/led', self.camera_light.handle)
        from video_jobs import VideoJobs, video_handler
        from pathlib import Path
        app['video_jobs'] = VideoJobs(Path.home() / '.local/share/biome-robot-vision')
        app.router.add_post('/api/video', video_handler)
        app.router.add_get('/api/state', state)
        app.router.add_post('/api/controls', controls)
        app.router.add_post('/api/map/snapshot', snapshot)
        app.router.add_get('/api/live/{lane}', live)
        async def start(app):
            await self.rover.start()
            self.monitor = asyncio.create_task(self.watch())
        async def close(app):
            if self.monitor:
                self.monitor.cancel()
                with contextlib.suppress(asyncio.CancelledError):await self.monitor
            await self.rover.close()
        app.on_startup.append(start)
        app.on_cleanup.append(close)
        return app

