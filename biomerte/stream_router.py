"""Self-contained leased-stream router for the Heroku dyno.

Vendored from BiomeRTE stream_router.py with TLS optional and secrets
supplied by config instead of local controller directories.
"""
import asyncio
import base64
import hashlib
import hmac
import json
import ssl
import struct
import time
import urllib.request


class Fault(Exception):
    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)


def credential(value, secrets, now=None):
    try:
        raw, signature = value.split('.')
        data = base64.urlsafe_b64decode(raw + '=' * (-len(raw) % 4))
        grant = json.loads(data.decode())
        secret = secrets[grant['authority']]
        if not hmac.compare_digest(hmac.new(secret.encode(), data, hashlib.sha256).hexdigest(), signature):
            raise ValueError()
        if min(grant['credential_expires_at'], grant['expires_at']) <= (time.time() if now is None else now):
            raise ValueError()
        if grant['role'] not in ('server', 'provider'):
            raise ValueError()
        return grant
    except (ValueError, KeyError, TypeError):
        raise Fault('Invalid stream attachment credential', 403)


def decode(message, maximum=None):
    if maximum is not None and len(message) > maximum:
        raise Fault('Stream message too large', 413)
    if isinstance(message, str):
        metadata = json.loads(message)
        payload = None
    else:
        if len(message) < 4:
            raise Fault('Missing frame metadata')
        size = struct.unpack('!I', message[:4])[0]
        if size + 4 > len(message):
            raise Fault('Invalid frame metadata')
        metadata = json.loads(message[4:4 + size].decode())
        payload = message[4 + size:]
    if not isinstance(metadata, dict):
        raise Fault('Expected stream metadata')
    return metadata, payload


def frame(metadata, data):
    raw = encode(metadata).encode()
    return struct.pack('!I', len(raw)) + raw + data


class Session:
    def __init__(self, grant):
        self.grant = grant
        self.peers = {}
        self.sequences = {}
        self.closed = False

    def check(self, role, lane, message, now=None):
        g = self.grant
        meta, payload = decode(message, None if g.get('contract') == 'robot.vision.stream:1' else g['limits']['max_message_bytes'])
        if lane not in g.get('lanes', ['data']):
            raise Fault('Lane outside stream grant', 403)
        if (meta.get('authority'), meta.get('generation'), meta.get('epoch')) != (
                g['authority'], g['generation'], g['epoch']):
            raise Fault('Superseded stream epoch', 409)
        seq = meta.get('sequence')
        if type(seq) is not int or seq <= self.sequences.get((role, lane), -1):
            raise Fault('Old or duplicate sequence', 409)
        ack = meta.get('type') == 'ack'
        direction = g['direction']
        if not ack and ((direction == 'provider_to_server' and role != 'provider') or
                        (direction == 'server_to_provider' and role != 'server')):
            raise Fault('Wrong stream direction', 403)
        now = time.time() if now is None else now
        if lane == 'control' and not ack:
            if role != 'server' or meta.get('type') != 'desired' or not isinstance(meta.get('outputs'), dict):
                raise Fault('Expected desired output state')
            issued, expires = meta.get('issued_at'), meta.get('expires_at')
            if (isinstance(issued, bool) or not isinstance(issued, (int, float)) or
                    isinstance(expires, bool) or not isinstance(expires, (int, float)) or
                    not issued <= now < expires <= issued + 1):
                raise Fault('Expired or invalid driving intent', 409)
        if lane == 'vision' and not ack:
            for field in ('frame_id', 'source_timestamp', 'processing_timestamp',
                          'calibration_revision', 'map_revision'):
                if field not in meta:
                    raise Fault('Missing vision metadata: ' + field)
            if payload is None:
                raise Fault('Vision requires frame bytes')
        self.sequences[(role, lane)] = seq
        return meta, payload

    async def close(self, reason='Stream revoked'):
        if self.closed:
            return
        self.closed = True
        peers = list(self.peers.values())
        self.peers.clear()
        await asyncio.gather(*(p.close(1008, reason) for p in peers), return_exceptions=True)


class Peer:
    def __init__(self, socket, depth, delivery):
        self.socket = socket
        self.queue = asyncio.Queue(depth)
        self.delivery = delivery
        self.closed = False

    def put(self, message):
        if self.closed:
            return
        if self.queue.full():
            if self.delivery == 'supersedable':
                self.queue.get_nowait()
            else:
                raise Fault('Ordered stream consumer too slow', 429)
        self.queue.put_nowait(message)

    async def pump(self):
        while not self.closed:
            message = await self.queue.get()
            await asyncio.wait_for(self.socket.send(message), timeout=1)

    async def close(self, code, reason):
        self.closed = True
        await self.socket.close(code, reason)


class Router:
    def __init__(self, secrets, validators):
        self.secrets = secrets
        self.validators = validators
        self.sessions = {}
        self.tasks = {}

    async def validate(self, grant):
        fn = self.validators.get(grant['authority'])
        if fn is None:
            raise Fault('Unknown lease authority', 403)
        value = await asyncio.to_thread(fn, grant['id'])
        keys = ('id', 'epoch', 'authority', 'generation', 'contract', 'resource', 'lease', 'provider', 'server', 'realm', 'site')
        if any(value.get(k) != grant.get(k) for k in keys) or value.get('state') != 'open':
            raise Fault('Superseded stream grant', 409)
        return value

    async def watch(self, session):
        try:
            while not session.closed:
                await self.validate(session.grant)
                await asyncio.sleep(1)
        except Exception:
            await session.close()
        finally:
            self.sessions.pop(session.grant['id'], None)
            self.tasks.pop(session.grant['id'], None)

    async def attach(self, socket, path, authorization):
        grant = credential(authorization.removeprefix('Stream '), self.secrets)
        session_id, lane = path.strip('/').split('/')
        if session_id != grant['id'] or lane not in grant.get('lanes', ['data']):
            raise Fault('Attachment binding mismatch', 403)
        await self.validate(grant)
        session = self.sessions.get(session_id)
        if not session:
            session = self.sessions[session_id] = Session(grant)
            self.tasks[session_id] = asyncio.create_task(self.watch(session))
        if session.closed or session.grant['epoch'] != grant['epoch']:
            raise Fault('Superseded stream epoch', 409)
        role = grant['role']
        key = (role, lane)
        previous = session.peers.get(key)
        if previous:
            await previous.close(1000, 'Reattached')
        delivery = grant.get('policies', {}).get(lane, grant['delivery'])
        peer = Peer(socket, 1 if delivery == 'supersedable' else grant['limits']['buffer_depth'], delivery)
        session.peers[key] = peer
        pump = asyncio.create_task(peer.pump())
        window, count = time.monotonic(), 0

        async def receive():
            nonlocal window, count
            async for message in socket:
                if pump.done():
                    pump.result()
                now = time.monotonic()
                if now - window >= 1:
                    window, count = now, 0
                count += 1
                if count > grant['limits']['messages_per_second']:
                    raise Fault('Stream rate exceeded', 429)
                session.check(role, lane, message)
                other = session.peers.get(('provider' if role == 'server' else 'server', lane))
                if other:
                    other.put(message)

        reader = asyncio.create_task(receive())
        try:
            done, pending = await asyncio.wait((reader, pump), return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            if session.peers.get(key) is peer:
                session.peers.pop(key, None)
            reader.cancel()
            pump.cancel()
            await asyncio.gather(reader, pump, return_exceptions=True)
            await peer.close(1000, 'Detached')


def _inspect(url, session_id, authority, secret):
    raw = encode({'session': session_id, 'authority': authority, 'timestamp': time.time()}).encode()
    signature = hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
    req = urllib.request.Request(url, data=raw,
        headers={'Content-Type': 'application/json', 'Authorization': 'Router ' + signature,
                 'User-Agent': 'BiomeRTE-Stream-Router/1'})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode())


async def serve(config):
    from websockets.asyncio.server import serve as websocket_server
    secrets = dict(config.get('secrets') or {})
    validators = {}
    inspect = config.get('inspect_url') or ''
    if not inspect.startswith('https://'):
        raise ValueError('Authenticated HTTPS stream.inspect URL is required')
    for node in secrets:
        validators[node] = lambda sid, node=node: _inspect(inspect, sid, node, secrets[node])
    router = Router(secrets, validators)
    ssl_context = None
    if config.get('certificate') and config.get('key'):
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ssl_context.load_cert_chain(config['certificate'], config['key'])

    async def health(connection, request):
        if request.path == '/health':
            return connection.respond(200, encode({'state': 'ready',
                'protocol': 'leased-stream-inspect-v1', 'authorities': sorted(secrets),
                'sessions': len(router.sessions)}))

    async def handle(socket):
        try:
            await router.attach(socket, socket.request.path,
                                socket.request.headers.get('Authorization', ''))
        except Exception:
            await socket.close(1008, 'Stream rejected')

    async with websocket_server(
        handle,
        config.get('listen', '0.0.0.0'),
        int(config.get('port', 8751)),
        ssl=ssl_context,
        process_request=health,
        max_size=None,
        max_queue=1,
        write_limit=65536,
    ):
        await asyncio.Future()

