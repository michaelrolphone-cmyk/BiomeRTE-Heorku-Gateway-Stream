"""Heroku public ingress: native streams plus reverse Mac Rover transport."""
import asyncio
from aiohttp import web, WSMsgType
from .stream_router import Router, _inspect
from .rover_transport import Ingress
from .authority_transport import Authorities


class Socket:
    def __init__(self, socket):
        self.socket = socket
    def __aiter__(self):
        return self.messages()
    async def messages(self):
        async for message in self.socket:
            if message.type in (WSMsgType.BINARY, WSMsgType.TEXT):
                yield message.data
            elif message.type == WSMsgType.ERROR:
                raise self.socket.exception() or ConnectionError('Socket failed')
    async def send(self, value):
        if isinstance(value, bytes):
            await self.socket.send_bytes(value)
        else:
            await self.socket.send_str(value)
    async def close(self, code, reason):
        await self.socket.close(code=code, message=reason.encode())


def application(config):
    secrets = config['secrets']
    inspect = config['inspect_url']
    if not inspect.startswith('https://'):
        raise ValueError('Authenticated HTTPS stream.inspect URL required')
    ingress = Ingress(next(iter(secrets.values())))
    authorities = Authorities(secrets)
    loop=asyncio.get_running_loop()
    def validate(sid,node):
        if node==config.get('rover_authority') and ingress.link is not None:
            return asyncio.run_coroutine_threadsafe(ingress.inspect(node,sid),loop).result(6)
        return _inspect(inspect,sid,node,secrets[node])
    validators={node:(lambda sid,node=node:validate(sid,node)) for node in secrets}
    router=Router(secrets,validators)
    owner=config.get('router_owner','hosted')
    if owner not in ('hosted','mac_bridge'):
        raise ValueError('Unknown native session owner')
    app = web.Application(client_max_size=0)
    async def health(request):
        return web.json_response({'state':'ready','protocol':'leased-stream-inspect-v1',
            'authorities':sorted(secrets),
            'authority_inspection_links':sorted(authorities.links),'sessions':len(router.sessions),
            'rover_bridge':ingress.link is not None,
            'session_owner':owner,
            'telemetry_header_limit_bytes':None,'telemetry_packet_limit_bytes':None,
            'native_owner_connected':'native_router' in ingress.capabilities,
            'owner_inspection_configured':bool(config.get('rover_authority')),
            'owner_inspection_connected':bool(config.get('rover_authority') and ingress.link is not None),
            'stream_peers':{node:sum(len(session.peers) for session in router.sessions.values() if session.grant['authority']==node) for node in secrets}})
    async def stream(request):
        if owner=='mac_bridge':
            return await ingress.native(request)
        socket = web.WebSocketResponse(heartbeat=15, max_msg_size=0,
                                       writer_limit=65536)
        await socket.prepare(request)
        try:
            await router.attach(Socket(socket), request.path,
                                request.headers.get('Authorization',''))
        except Exception:
            await socket.close(code=1008, message=b'Stream rejected')
        return socket
    app.router.add_get('/health', health)
    ingress.mount(app)
    authorities.mount(app)
    app.router.add_get('/{session:[a-f0-9]{32}}/{lane:[a-z_]+}', stream)
    async def close(app):
        await authorities.close()
        await asyncio.gather(*(s.close('Router stopped') for s in list(router.sessions.values())),
                             return_exceptions=True)
        tasks = list(router.tasks.values())
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
    app.on_cleanup.append(close)
    return app


async def serve(config):
    runner = web.AppRunner(application(config))
    await runner.setup()
    try:
        await web.TCPSite(runner, config.get('listen','0.0.0.0'), config['port']).start()
        await asyncio.Future()
    finally:
        await runner.cleanup()
