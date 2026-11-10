"""Bounded reverse ingress for a Mac-owned Rover service.

Each live lane uses its own outbound connection. The control link carries only
attachment notices and bounded HTTP requests; no video crosses that link.
"""
import asyncio
import contextlib
import hashlib
import hmac
import json
import uuid
from aiohttp import web, ClientSession, WSMsgType, ClientError


def derived(secret, purpose):
    return hmac.new(secret.encode(), purpose.encode(), hashlib.sha256).hexdigest()


async def copy_socket(source, target):
    async for message in source:
        if message.type == WSMsgType.BINARY:
            await target.send_bytes(message.data)
        elif message.type == WSMsgType.TEXT:
            await target.send_str(message.data)
        elif message.type == WSMsgType.ERROR:
            raise source.exception() or ConnectionError('Socket failed')


async def splice(first, second):
    tasks = [asyncio.create_task(copy_socket(first, second)),
             asyncio.create_task(copy_socket(second, first))]
    try:
        done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await first.close()
        await second.close()


class Ingress:
    def __init__(self, secret, limit=32):
        self.key = derived(secret, 'rover-link')
        self.site_token = derived(secret, 'rover-site')
        self.link = None
        self.pending = {}
        self.lanes = {}
        self.active = set()
        self.limit = limit

    def authenticate(self, request, token, prefix):
        if not hmac.compare_digest(request.headers.get('Authorization', ''), prefix + token):
            raise web.HTTPUnauthorized()

    async def bridge(self, request):
        self.authenticate(request, self.key, 'Bridge ')
        if self.link is not None:
            raise web.HTTPConflict(text='Rover bridge already connected')
        socket = web.WebSocketResponse(heartbeat=15, max_msg_size=4194304)
        await socket.prepare(request)
        self.link = socket
        try:
            async for message in socket:
                if message.type != WSMsgType.TEXT:
                    continue
                value = json.loads(message.data)
                future = self.pending.get(value.get('id'))
                if future and not future.done():
                    future.set_result(value)
        finally:
            self.link = None
            for future in list(self.pending.values()) + list(self.lanes.values()):
                if not future.done():
                    future.set_exception(ConnectionError('Rover bridge disconnected'))
            await asyncio.gather(*(s.close(code=1011, message=b'Rover bridge lost')
                                   for s in list(self.active)), return_exceptions=True)
        return socket

    async def data(self, request):
        self.authenticate(request, self.key, 'Bridge ')
        future = self.lanes.get(request.match_info['id'])
        if future is None or future.done():
            raise web.HTTPNotFound()
        socket = web.WebSocketResponse(heartbeat=15, max_msg_size=4194304,
                                       writer_limit=65536)
        await socket.prepare(request)
        finished = asyncio.get_running_loop().create_future()
        future.set_result((socket, finished))
        try:
            await finished
        finally:
            await socket.close()
        return socket

    async def public(self, request):
        self.authenticate(request, self.site_token, 'Bearer ')
        path = request.path
        if path not in ('/api/state', '/api/controls', '/api/map/snapshot',
                        '/api/live/vision', '/api/live/map', '/api/live/control'):
            raise web.HTTPNotFound()
        if self.link is None or self.link.closed:
            raise web.HTTPServiceUnavailable(text='Rover service disconnected')
        if len(self.pending) + len(self.lanes) >= self.limit:
            raise web.HTTPTooManyRequests()
        identity = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        if path.startswith('/api/live/'):
            if request.headers.get('Upgrade', '').lower() != 'websocket':
                raise web.HTTPBadRequest()
            self.lanes[identity] = future
            finished = None
            try:
                await self.link.send_json({'type': 'open', 'id': identity, 'path': path})
                upstream, finished = await asyncio.wait_for(future, 10)
                socket = web.WebSocketResponse(heartbeat=15, max_msg_size=4096,
                                               writer_limit=65536)
                await socket.prepare(request)
                self.active.update((upstream, socket))
                try:
                    await splice(upstream, socket)
                finally:
                    self.active.discard(upstream)
                    self.active.discard(socket)
                return socket
            finally:
                self.lanes.pop(identity, None)
                if finished is not None and not finished.done():
                    finished.set_result(None)
                if self.link is not None and not self.link.closed:
                    await self.link.send_json({'type':'cancel', 'id':identity})
        if request.method not in ('GET', 'POST'):
            raise web.HTTPMethodNotAllowed(request.method, ('GET', 'POST'))
        body = await request.read()
        if len(body) > 4096:
            raise web.HTTPRequestEntityTooLarge(max_size=4096, actual_size=len(body))
        self.pending[identity] = future
        try:
            await self.link.send_json({'type':'request', 'id':identity, 'path':path,
                                      'method':request.method, 'body':body.decode()})
            value = await asyncio.wait_for(future, 15)
            return web.Response(status=value['status'], text=value['body'],
                                content_type='application/json')
        finally:
            self.pending.pop(identity, None)

    def mount(self, app):
        app.router.add_get('/rover/bridge', self.bridge)
        app.router.add_get('/rover/data/{id}', self.data)
        app.router.add_route('*', '/api/{tail:.*}', self.public)


class Outbound:
    def __init__(self, public, local, secret):
        self.public = public.rstrip('/')
        self.local = local.rstrip('/')
        self.headers = {'Authorization':'Bridge ' + derived(secret, 'rover-link')}
        self.site_headers = {'Authorization':'Bearer ' + derived(secret, 'rover-site')}
        self.tasks = {}

    async def open(self, session, identity, path):
        async with session.ws_connect(self.local + path, headers=self.site_headers,
                                      heartbeat=15, max_msg_size=4194304) as local:
            async with session.ws_connect(self.public + '/rover/data/' + identity,
                                          headers=self.headers, heartbeat=15,
                                          max_msg_size=4194304) as remote:
                await splice(local, remote)

    async def request(self, session, link, value):
        try:
            async with session.request(value['method'], self.local + value['path'],
                data=value['body'].encode(), headers={**self.site_headers,
                'Content-Type':'application/json'}, allow_redirects=False) as response:
                body = await response.text()
                if len(body) > 4000000:
                    raise ValueError('Snapshot response too large')
                reply = {'id':value['id'], 'status':response.status, 'body':body}
        except Exception:
            reply = {'id':value['id'], 'status':503, 'body':'{"error":{"code":"ROVER_UNAVAILABLE"}}'}
        await link.send_json(reply)

    async def run(self):
        async with ClientSession() as session:
            while True:
                try:
                    async with session.ws_connect(self.public + '/rover/bridge',
                        headers=self.headers, heartbeat=15, max_msg_size=65536) as link:
                        async for message in link:
                            if message.type != WSMsgType.TEXT:
                                continue
                            value = json.loads(message.data)
                            identity = value['id']
                            if value['type'] == 'cancel':
                                task = self.tasks.pop(identity, None)
                                if task:
                                    task.cancel()
                            elif value['type'] in ('open', 'request'):
                                if len(self.tasks) >= 32 or identity in self.tasks:
                                    continue
                                coroutine = (self.open(session, identity, value['path'])
                                    if value['type']=='open' else self.request(session, link, value))
                                task = asyncio.create_task(coroutine)
                                self.tasks[identity] = task
                                def done(task, identity=identity):
                                    self.tasks.pop(identity, None)
                                    if not task.cancelled():
                                        task.exception()
                                task.add_done_callback(done)
                except (ClientError, ConnectionError, OSError, asyncio.TimeoutError):
                    pass
                finally:
                    tasks = list(self.tasks.values())
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    self.tasks.clear()
                await asyncio.sleep(1)
