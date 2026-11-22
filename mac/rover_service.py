"""Mac paired Rover owner and outbound-only public ingress."""
import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import os
import pathlib
import signal
import sys
import time
import urllib.request
import uuid
from aiohttp import web
from biomerte.app import read_json
from biomerte.identity import sign
from biomerte.rover import Rover
from biomerte.control_clock import ControlClock
from biomerte.service_routing import ServiceOwnerResolver
from rover_transport import Outbound, VISION_FRESHNESS_SECONDS, derived, local_router
from site_bridge import SiteBridge
from rover_routes import ControllerRoutes

GATEWAY='https://biomerte-controller.cantikash.chatgpt.site'
PUBLIC='https://biomerte-stream-gateway-e0322b2b60c9.herokuapp.com'


class LocalAuthority:
    """Use the running controller API; never run a bundled authority on its database."""
    def __init__(self, directory, caller_home):
        self.config = read_json(str(pathlib.Path(directory)/'node.json'))
        self.node = self.config['node']
        self.caller_home = pathlib.Path(caller_home)
        self.identity = read_json(str(self.caller_home/'node.json'))

    def call(self, operation, arguments, request_id, auth=None, realm='site', controller=None):
        envelope = dict(site=self.config['site'], realm=realm, operation=operation,
                        arguments=arguments, id=request_id, controller=controller or self.node)
        envelope['auth'] = auth or dict(actor=self.identity['node'],
            signature=sign(str(self.caller_home/'node.key'), envelope))
        request = urllib.request.Request('http://127.0.0.1:8740/v1/call',
            data=json.dumps(envelope).encode(), headers={'Content-Type':'application/json',
            'User-Agent':'BiomeRTE-Rover-Server/1'})
        with urllib.request.urlopen(request, timeout=20) as response:
            value = json.load(response)
        if 'error' in value:
            raise ConnectionError('Controller rejected Rover request: '+str(value['error']))
        return value['result']


class RoutedRemote(ControllerRoutes):
    def __init__(self, app, home, *, resolver=None):
        self.app, self.home = app, home
        self.identity = read_json(str(home/'node.json'))
        self.service_resolver = resolver if resolver is not None else ServiceOwnerResolver(
            PUBLIC, declarations=lambda: read_json(str(home/'node.json')).get(
                'service_authority_declarations', []))
        super().__init__(app.node, home, self.signed_request)

    def resolve_new(self, contract, resource):
        owner = self.service_resolver(resource)
        return owner if owner is not None else super().resolve_new(contract, resource)

    def signed_request(self, controller, operation, arguments):
        request_id = uuid.uuid4().hex
        envelope = dict(site=self.app.config['site'], realm='site', operation=operation,
                        arguments=arguments, id=request_id, controller=controller)
        auth = dict(actor=self.identity['node'],
                    signature=sign(str(self.home/'node.key'), envelope))
        if controller == self.app.node:
            result = self.app.call(operation, arguments, request_id, auth=auth,
                                   realm='site', controller=controller)
        else:
            envelope['auth'] = auth
            request = urllib.request.Request(GATEWAY+'/v1/call',
                data=json.dumps(envelope).encode(), headers={
                'Content-Type':'application/json','User-Agent':'BiomeRTE-Rover-Server/1'})
            with urllib.request.urlopen(request, timeout=20) as response:
                value = json.load(response)
            if 'error' in value:
                raise ConnectionError('Controller rejected Rover request')
            result = value['result']
        return result



class FreshRover(Rover):
    def report(self, operation, error):
        if not hasattr(self,'_reported'):self._reported={}
        key=(operation,type(error).__name__,str(error)[:200])
        if time.monotonic()-self._reported.get(key,-100)>30:
            self._reported[key]=time.monotonic()
            logging.warning('Rover %s: %s: %s',operation,type(error).__name__,str(error)[:200])

    async def call(self, operation, **args):
        try:return await super().call(operation,**args)
        except Exception as error:
            self.report(operation,error)
            raise

    async def lane(self, descriptor, lane):
        if getattr(self,'local_router',None):
            descriptor={**descriptor,'url':self.local_router+'/'+descriptor['id']}
        try:return await super().lane(descriptor,lane)
        except Exception as error:
            self.report(lane,error)
            raise

    def broadcast(self, lane, message):
        if lane == 'vision':
            from biomerte.stream_router import decode
            metadata, unused = decode(message)
            if metadata.get('type') in ('sensor_manifest', 'world_snapshot'):
                return super().broadcast(lane, message)
            processed = metadata.get('processed_monotonic')
            # Provider and server share this Mac's monotonic clock. This bound
            # is explicitly processing-to-server, not camera capture latency.
            if type(processed) not in (float,int) or not 0 <= time.monotonic()-processed <= VISION_FRESHNESS_SECONDS:
                return
        super().broadcast(lane, message)


def map_snapshot(rover):
    async def read(map_id, revision):
        if not isinstance(map_id, str) or type(revision) is not int or revision < 0:
            raise web.HTTPBadRequest(text='Expected map ID and revision')
        return await rover.read_capability('robot.map.snapshot:1', {'map_id':map_id, 'revision':revision})
    return read


async def run():
    home=pathlib.Path.home()/'.local/share/biome-rover-server'
    from logging.handlers import RotatingFileHandler
    logging.basicConfig(level=logging.WARNING,handlers=[RotatingFileHandler(
        home/'runtime.log',maxBytes=1048576,backupCount=1)])
    app=LocalAuthority(pathlib.Path.home()/'.local/share/biomerte-controller',home)
    secret=app.config['stream_router_secret']
    remote=RoutedRemote(app,home)
    clock=await ControlClock().start()
    rover=FreshRover(remote,{'robot.vision.stream:1':'robotvision.mac.ae3c74c06d1909a1ccdc',
                           'device.outputs.stream:1':'relay6ch.30eda0d95f24'},task='rover-site-server',clock=clock)
    rover.local_router=local_router(os.environ.get('BIOMERTE_LOCAL_STREAM_ROUTER'))
    if rover.local_router:
        if not hasattr(rover,'require_provider_ready'):
            raise RuntimeError('Native runtime requires versioned provider-peer readiness support')
        rover.require_provider_ready=True
    bridge=SiteBridge(rover,derived(secret,'rover-site'),
        {'forward':1,'reverse':2,'left':3,'right':4},snapshot=map_snapshot(rover),
        intent_ttl_ms=int(os.environ.get('ROVER_INTENT_TTL_MS','500')))
    application=bridge.app()
    async def inspect_stream(request):
        if not hmac.compare_digest(request.headers.get('X-Router-Inspect',''),derived(secret,'router-inspect')):
            raise web.HTTPUnauthorized()
        value=await request.json()
        if (not isinstance(value,dict) or set(value)!={'authority','session'} or
            value['authority']!=app.node or not isinstance(value['session'],str) or
            len(value['session'])!=32 or any(c not in '0123456789abcdef' for c in value['session'])):
            raise web.HTTPBadRequest()
        try:
            grant=await asyncio.to_thread(app.call,'stream.inspect',{'session':value['session']},uuid.uuid4().hex,realm='site')
            return web.json_response(grant)
        except Exception:
            raise web.HTTPConflict(text='Stream authority unavailable')
    application.router.add_post('/internal/stream/inspect',inspect_stream)
    runner=web.AppRunner(application)
    await runner.setup()
    transport=None
    stopped=asyncio.Event()
    loop=asyncio.get_running_loop()
    for sig in (signal.SIGTERM,signal.SIGINT):
        loop.add_signal_handler(sig,stopped.set)
    waiter=None
    try:
        await web.TCPSite(runner,'127.0.0.1',8093).start()
        transport=asyncio.create_task(Outbound(PUBLIC,'http://127.0.0.1:8093',secret,rover.local_router,shared_site_vision=True).run())
        waiter=asyncio.create_task(stopped.wait())
        done,unused=await asyncio.wait((transport,waiter),return_when=asyncio.FIRST_COMPLETED)
        if transport in done:await transport
    finally:
        if transport:
            transport.cancel()
            with contextlib.suppress(asyncio.CancelledError):await transport
        if waiter:
            waiter.cancel()
            with contextlib.suppress(asyncio.CancelledError):await waiter
        await runner.cleanup()
        await clock.close()

if __name__=='__main__':
    try:asyncio.run(run())
    except KeyboardInterrupt:pass
