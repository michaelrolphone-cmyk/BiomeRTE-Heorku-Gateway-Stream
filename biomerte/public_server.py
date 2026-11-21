"""Heroku public ingress: native streams plus reverse Mac Rover transport."""
import asyncio
from aiohttp import ClientSession, web, WSMsgType
from .stream_router import Router, _inspect, service_routes
from .rover_transport import Ingress
from .authority_transport import Authorities
from .service_coordinator import Coordinator
from .service_transport import ServiceAuthorities
from . import service_demand
from .checkpoint_transport import CheckpointWorkers
from .durable_witness import DurableWitness


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
    coordinator = Coordinator(
        config.get('service_coordinator_secret', next(iter(secrets.values()))),
        max_lease_ttl_s=config.get('service_coordinator_max_lease_ttl_s', 30),
        reserved_authorities=secrets)
    for manifest in config.get("service_manifests", []):
        coordinator.configure(manifest)
    services = ServiceAuthorities(coordinator,
        config.get("service_authority_secret", next(iter(secrets.values()))))
    durable = None
    if config.get("durable_witness_origin") or config.get("durable_witness_secret"):
        durable = DurableWitness(None, coordinator, config.get("durable_witness_origin", ""),
            config.get("durable_witness_secret", ""),
            witness_id=config.get("durable_witness_id", "biomerte-services"),
            lease_s=config.get("durable_witness_lease_s", 120))
    checkpoints = CheckpointWorkers(coordinator, durable_witness=durable)
    authorities = Authorities(secrets, services=services)
    loop=asyncio.get_running_loop()
    def validate(sid,node):
        if node==config.get('rover_authority') and ingress.link is not None:
            return asyncio.run_coroutine_threadsafe(ingress.inspect(node,sid),loop).result(6)
        return _inspect(inspect,sid,node,secrets[node])
    validators={node:(lambda sid,node=node:validate(sid,node)) for node in secrets}
    def inspect_service(node, sid):
        reply = asyncio.run_coroutine_threadsafe(services.inspect(node, sid), loop).result(6)
        if reply["status"] != 200:
            raise ConnectionError("Logical authority rejected stream inspection")
        return reply["grant"]
    router_secrets, router_validators = service_routes(
        secrets, validators, services.secret, inspect_service)
    router=Router(router_secrets,router_validators)
    owner=config.get('router_owner','hosted')
    if owner not in ('hosted','mac_bridge'):
        raise ValueError('Unknown native session owner')
    app = web.Application(client_max_size=0)
    async def watch_capability_demand(app):
        task = asyncio.create_task(service_demand.watch(coordinator, services))
        try:
            yield
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
    app.cleanup_ctx.append(watch_capability_demand)
    if durable:
        async def start_witness(app):
            durable.session = ClientSession()
            durable.start()
        app.on_startup.append(start_witness)
    async def health(request):
        return web.json_response({'state':'ready','protocol':'leased-stream-inspect-v1',
            'authorities':sorted(secrets),
            'authority_inspection_links':sorted(authorities.links),'sessions':len(router.sessions),
            'service_authority_links':sorted(services.links),
            'service_checkpoint_workers':sorted({worker for worker, service in checkpoints.links}),
            'service_durable_witness_ready':durable.current() if durable else None,
            'service_durable_checkpoint_refs':len(durable.heads) if durable else None,
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
    # Specific coordinator routes precede the Rover /api/{tail:.*} fallback.
    coordinator.mount(app)
    services.mount(app)
    checkpoints.mount(app)
    ingress.mount(app)
    authorities.mount(app)
    app.router.add_get('/{session:[a-f0-9]{32}}/{lane:[a-z_]+}', stream)
    async def close(app):
        if durable:
            await durable.close()
            if durable.session:
                await durable.session.close()
        await authorities.close()
        await services.close()
        await checkpoints.close()
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
