import asyncio,unittest
from unittest.mock import patch
from aiohttp import web,ClientSession,WSMsgType
from biomerte.public_server import application, Socket
from biomerte.stream_router import Router
from biomerte.stream_router import frame
from biomerte.rover_transport import Outbound,derived
import contextlib
from test_router_integration import grant,token,NODE,SECRET

class PublicServerTests(unittest.IsolatedAsyncioTestCase):
 async def test_mac_owner_pairs_local_server_with_public_provider_and_revokes(self):
  g=grant();router=Router({NODE:SECRET},{NODE:lambda sid:dict(g)})
  async def start(app):
   runner=web.AppRunner(app);await runner.setup();self.addAsyncCleanup(runner.cleanup)
   site=web.TCPSite(runner,'127.0.0.1',0);await site.start()
   return 'http://127.0.0.1:'+str(site._server.sockets[0].getsockname()[1])
  local=web.Application()
  async def native(request):
   ws=web.WebSocketResponse();await ws.prepare(request)
   try:await router.attach(Socket(ws),request.path,request.headers.get('Authorization',''))
   except Exception:await ws.close(code=1008)
   return ws
  local.router.add_get('/{session}/{lane}',native)
  origin=await start(local)
  base=await start(application({'secrets':{NODE:SECRET},'inspect_url':'https://gateway/inspect','router_owner':'mac_bridge'}))
  outbound=Outbound(base,origin,SECRET,origin.replace('http:','ws:'))
  task=asyncio.create_task(outbound.run())
  try:
   async with ClientSession() as c:
    for _ in range(100):
     async with c.get(base+'/health') as r:
      if (await r.json())['native_owner_connected']:break
     await asyncio.sleep(.01)
    path='/'+g['id']+'/vision'
    async with c.ws_connect(origin+path,headers={'Authorization':'Stream '+token(dict(g,role='server'))}) as receiver:
     async with c.ws_connect(base+path,headers={'Authorization':'Stream '+token(g)}) as sender:
      raw=frame(dict(authority=NODE,epoch=g['epoch'],generation=g['generation'],sequence=1,frame_id='f',source_timestamp=None,processing_timestamp=0,calibration_revision=1,map_revision=1),b'jpeg')
      await sender.send_bytes(raw)
      self.assertEqual(raw,(await receiver.receive(timeout=2)).data)
      self.assertEqual(1,len(router.sessions))
      g['state']='revoked'
      self.assertEqual(WSMsgType.CLOSE,(await receiver.receive(timeout=3)).type)
      self.assertIn((await sender.receive(timeout=3)).type,(WSMsgType.CLOSE,WSMsgType.CLOSED))
  finally:
   task.cancel()
   with contextlib.suppress(asyncio.CancelledError):await task
   for watcher in list(router.tasks.values()):watcher.cancel()
   await asyncio.gather(*list(router.tasks.values()),return_exceptions=True)

 async def test_native_stream_adapter_and_authoritative_revocation(self):
  g=grant()
  with patch('biomerte.public_server._inspect',lambda *args:dict(g)):
   runner=web.AppRunner(application({'secrets':{NODE:SECRET},'inspect_url':'https://gateway/inspect'}))
   await runner.setup();self.addAsyncCleanup(runner.cleanup)
   site=web.TCPSite(runner,'127.0.0.1',0);await site.start()
   base='http://127.0.0.1:'+str(site._server.sockets[0].getsockname()[1])
   async with ClientSession() as client:
    async with client.get(base+'/health') as r:self.assertFalse((await r.json())['rover_bridge'])
    async with client.ws_connect(base+'/'+g['id']+'/vision',headers={'Authorization':'Stream '+token(dict(g,role='server'))}) as receiver:
     async with client.ws_connect(base+'/'+g['id']+'/vision',headers={'Authorization':'Stream '+token(g)}) as sender:
      meta=dict(authority=NODE,epoch=g['epoch'],generation=g['generation'],sequence=1,frame_id='f',source_timestamp=None,processing_timestamp=0,calibration_revision=1,map_revision=1)
      raw=frame(meta,b'jpeg');await sender.send_bytes(raw)
      self.assertEqual(raw,(await receiver.receive(timeout=2)).data)
      g['state']='released'
      self.assertEqual(WSMsgType.CLOSE,(await receiver.receive(timeout=3)).type)

 async def test_reverse_owner_inspection_bypasses_queued_callback_and_revokes(self):
  g=grant()
  async def start(app):
   runner=web.AppRunner(app);await runner.setup();self.addAsyncCleanup(runner.cleanup)
   site=web.TCPSite(runner,'127.0.0.1',0);await site.start()
   return 'http://127.0.0.1:'+str(site._server.sockets[0].getsockname()[1])
  local=web.Application()
  async def inspect(request):
   self.assertEqual('Bearer '+derived(SECRET,'rover-site'),request.headers['Authorization'])
   self.assertEqual(derived(SECRET,'router-inspect'),request.headers['X-Router-Inspect'])
   value=await request.json();self.assertEqual(NODE,value['authority'])
   return web.json_response(dict(g))
  local.router.add_post('/internal/stream/inspect',inspect)
  origin=await start(local)
  with patch('biomerte.public_server._inspect',side_effect=AssertionError('queued callback must not run')):
   base=await start(application({'secrets':{NODE:SECRET},'inspect_url':'https://gateway/inspect','rover_authority':NODE}))
   outbound=Outbound(base,origin,SECRET);task=asyncio.create_task(outbound.run())
   try:
    async with ClientSession() as c:
     for _ in range(100):
      async with c.get(base+'/health') as r:
       if (await r.json())['rover_bridge']:break
      await asyncio.sleep(.01)
     async with c.ws_connect(base+'/'+g['id']+'/vision',headers={'Authorization':'Stream '+token(dict(g,role='server'))}) as receiver:
      async with c.ws_connect(base+'/'+g['id']+'/vision',headers={'Authorization':'Stream '+token(g)}) as sender:
       raw=frame(dict(authority=NODE,epoch=g['epoch'],generation=g['generation'],sequence=1,frame_id='f',source_timestamp=None,processing_timestamp=0,calibration_revision=1,map_revision=1),b'jpeg')
       await sender.send_bytes(raw);self.assertEqual(raw,(await receiver.receive(timeout=2)).data)
       g['state']='revoked'
       self.assertEqual(WSMsgType.CLOSE,(await receiver.receive(timeout=3)).type)
   finally:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):await task

if __name__=='__main__':unittest.main()
