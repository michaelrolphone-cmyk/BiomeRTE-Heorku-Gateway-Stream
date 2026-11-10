"""Authority-keyed read-only inspection over outbound WebSockets."""
import asyncio
import hashlib
import hmac
import json
import re
import time
import uuid
from aiohttp import web, WSMsgType


def authority_key(secret, authority):
    return hmac.new(secret.encode(), ('authority-inspect:' + authority).encode(),
                    hashlib.sha256).hexdigest()


class Authorities:
    def __init__(self, secrets, limit=64, timeout=5):
        self.secrets = dict(secrets)
        self.links = {}
        self.pending = {}
        self.limit = limit
        self.timeout = timeout

    async def connect(self, request):
        authority = request.match_info['authority']
        secret = self.secrets.get(authority)
        expected = 'Authority ' + authority_key(secret, authority) if secret else ''
        if (not expected or not hmac.compare_digest(
                request.headers.get('Authorization', ''), expected)):
            raise web.HTTPUnauthorized()
        if authority in self.links:
            raise web.HTTPConflict(text='Authority already connected')
        socket = web.WebSocketResponse(heartbeat=15, max_msg_size=32768)
        await socket.prepare(request)
        self.links[authority] = socket
        try:
            async for message in socket:
                if message.type != WSMsgType.TEXT:
                    continue
                try:
                    value = json.loads(message.data)
                except (ValueError, TypeError):
                    continue
                if not isinstance(value, dict):
                    continue
                item = self.pending.get(value.get('id'))
                if (item is None or item['socket'] is not socket or
                        value.get('type') != 'inspect_result' or item['future'].done()):
                    continue
                status = value.get('status')
                if type(status) is not int or status not in (200, 400, 403, 404, 409, 429, 503):
                    continue
                if status == 200:
                    grant = value.get('grant')
                    if (not isinstance(grant, dict) or grant.get('authority') != authority
                            or grant.get('id') != item['session']):
                        continue
                item['future'].set_result(value)
        finally:
            if self.links.get(authority) is socket:
                self.links.pop(authority, None)
            for item in list(self.pending.values()):
                if item['socket'] is socket and not item['future'].done():
                    item['future'].set_exception(ConnectionError('Authority disconnected'))
        return socket

    async def inspect(self, authority, session):
        socket = self.links.get(authority)
        if socket is None or socket.closed:
            raise web.HTTPServiceUnavailable(text='Authority disconnected')
        if len(self.pending) >= self.limit:
            raise web.HTTPTooManyRequests()
        identity = uuid.uuid4().hex
        future = asyncio.get_running_loop().create_future()
        self.pending[identity] = dict(socket=socket, session=session, future=future)
        try:
            await socket.send_json(dict(type='inspect', id=identity,
                                        authority=authority, session=session))
            value = await asyncio.wait_for(future, self.timeout)
            return value
        except (ConnectionError, asyncio.TimeoutError):
            raise web.HTTPServiceUnavailable(text='Authority unavailable')
        finally:
            self.pending.pop(identity, None)

    async def request(self, request):
        # This is a private router RPC, with no browser identity or CORS flow.
        if request.headers.get('Origin'):
            raise web.HTTPForbidden()
        raw = await request.read()
        try:
            value = json.loads(raw)
            if not isinstance(value, dict) or set(value) != {'authority', 'session', 'timestamp'}:
                raise ValueError()
            authority, session, timestamp = (value[k] for k in ('authority', 'session', 'timestamp'))
            secret = self.secrets[authority]
            if (not isinstance(session, str) or not re.fullmatch('[a-f0-9]{32}', session)
                    or type(timestamp) is not int or abs(time.time() - timestamp) > 30):
                raise ValueError()
            expected = 'Router ' + hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
            if not hmac.compare_digest(request.headers.get('Authorization', ''), expected):
                raise ValueError()
        except (KeyError, ValueError, TypeError):
            raise web.HTTPUnauthorized()
        value = await self.inspect(authority, session)
        if value['status'] != 200:
            return web.json_response({'error': 'Authority rejected inspection'}, status=value['status'])
        return web.json_response(value['grant'], headers={'Cache-Control': 'no-store'})

    async def close(self):
        await asyncio.gather(*(socket.close(code=1001, message=b'Inspection service stopped')
                               for socket in list(self.links.values())), return_exceptions=True)

    def mount(self, app):
        app.router.add_get('/authority/{authority:[a-f0-9]{64}}/connect', self.connect)
        app.router.add_post('/stream/inspect', self.request)
