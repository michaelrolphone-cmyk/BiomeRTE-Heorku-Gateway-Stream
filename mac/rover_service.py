"""Mac paired Rover owner and outbound-only public ingress."""
import asyncio
import contextlib
import hashlib
import json
import pathlib
import sys
import time
import urllib.request
import uuid
from aiohttp import web
from biomerte.app import Application, read_json
from biomerte.identity import sign
from biomerte.rover import Rover
from rover_transport import Outbound, derived
from site_bridge import SiteBridge

MAC='217339021b2b3d61b16660da5562889be8e57e0636bdd9a5d129e3570c67ae4b'
PI='687eda586198b9e7c2bdad0c6f69731b63552e6a650e6514c663f3d472c8f714'
SITE='f45540bf9910412680734d0613e84460'
GATEWAY='https://biomerte-controller.cantikash.chatgpt.site'
PUBLIC='https://biomerte-stream-gateway-e0322b2b60c9.herokuapp.com'


class RoutedRemote:
    def __init__(self, app, home):
        self.app, self.home = app, home
        self.identity = read_json(str(home/'node.json'))
        self.leases = {}
        self.sessions = {}

    def call(self, operation, arguments):
        if operation == 'cap.acquire':
            controller = PI if arguments['resource'].startswith('relay6ch.') else MAC
        elif 'lease' in arguments:
            controller = self.leases[arguments['lease']]
        elif 'session' in arguments:
            controller = self.sessions[arguments['session']]
        else:
            raise ValueError('Unroutable Rover operation')
        request_id = uuid.uuid4().hex
        envelope = dict(site=SITE, realm='site', operation=operation,
                        arguments=arguments, id=request_id, controller=controller)
        auth = dict(actor=self.identity['node'],
                    signature=sign(str(self.home/'node.key'), envelope))
        if controller == MAC:
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
        if operation == 'cap.acquire':
            self.leases[result['id']] = controller
        if operation in ('stream.open','stream.attach'):
            self.sessions[result['id']] = controller
        if operation == 'stream.close':
            self.sessions.pop(arguments['session'], None)
        if operation == 'cap.release' and result.get('state') == 'released':
            self.leases.pop(arguments['lease'], None)
        return result


class FreshRover(Rover):
    def broadcast(self, lane, message):
        if lane == 'vision':
            from biomerte.stream_router import decode
            metadata, unused = decode(message, 4194304)
            processed = metadata.get('processed_monotonic')
            # Provider and server share this Mac's monotonic clock. This bound
            # is explicitly processing-to-server, not camera capture latency.
            if type(processed) not in (float,int) or not 0 <= time.monotonic()-processed <= .5:
                return
        super().broadcast(lane, message)


def map_snapshot():
    from biome_provider import VisionClient
    client = VisionClient(pathlib.Path.home()/'.local/share/biome-robot-vision/vision.sock', timeout=10)
    async def snapshot(map_id, revision):
        if not isinstance(map_id,str) or type(revision) is not int or revision<0:
            raise web.HTTPBadRequest()
        entities, cursor, seen, metadata = [], None, set(), None
        while True:
            inputs={'map_id':map_id,'revision':revision,'limit':500}
            if cursor is not None:
                inputs['cursor']=cursor
            page=await asyncio.to_thread(client.query,'robot.map.read',inputs)
            current=page['map']
            if current.get('map_id',current.get('id'))!=map_id or current['revision']!=revision:
                raise web.HTTPConflict(text='Map snapshot revision changed')
            metadata=current
            entities.extend(page['entities'])
            cursor=page.get('next_cursor')
            if not cursor:
                break
            if cursor in seen:
                raise web.HTTPConflict(text='Repeated map cursor')
            seen.add(cursor)
        return dict(type='snapshot',map_id=map_id,revision=revision,complete=True,
                    data=dict(map=metadata,entities=entities,next_cursor=None))
    return snapshot


async def run():
    home=pathlib.Path.home()/'.local/share/biome-rover-server'
    app=Application(str(pathlib.Path.home()/'.local/share/biomerte-controller'))
    secret=app.config['stream_router_secret']
    remote=RoutedRemote(app,home)
    rover=FreshRover(remote,{'robot.vision.stream:1':'robotvision.mac.ae3c74c06d1909a1ccdc',
                           'device.outputs.stream:1':'relay6ch.30eda0d95f24'},task='rover-site-server')
    bridge=SiteBridge(rover,derived(secret,'rover-site'),
        {'forward':1,'reverse':2,'left':3,'right':4},snapshot=map_snapshot())
    runner=web.AppRunner(bridge.app())
    await runner.setup()
    transport=None
    try:
        await web.TCPSite(runner,'127.0.0.1',8093).start()
        transport=asyncio.create_task(Outbound(PUBLIC,'http://127.0.0.1:8093',secret).run())
        await transport
    finally:
        if transport:
            transport.cancel()
            with contextlib.suppress(asyncio.CancelledError):await transport
        await runner.cleanup()

if __name__=='__main__':
    try:asyncio.run(run())
    except KeyboardInterrupt:pass
