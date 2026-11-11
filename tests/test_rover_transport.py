import asyncio,contextlib,unittest
from aiohttp import web,ClientSession,WSMsgType
from biomerte.rover_transport import Ingress,Outbound,derived,local_router


class LocalRouterTests(unittest.TestCase):
 def test_plaintext_attachment_override_is_loopback_only(self):
  self.assertEqual('ws://127.0.0.1:8751',local_router('ws://127.0.0.1:8751/'))
  for value in ('ws://public.example','http://localhost','ws://localhost/path','ws://user@localhost','ws://localhost?x=1'):
   with self.assertRaises(ValueError):local_router(value)


class ReverseTransportTests(unittest.IsolatedAsyncioTestCase):
 async def asyncSetUp(self):
  self.secret='test-secret-'*8
  self.ingress=Ingress(self.secret)
  ingress_app=web.Application();self.ingress.mount(ingress_app)
  self.public=await self.start(ingress_app)
  local=web.Application()
  async def state(request):
   self.assertEqual('Bearer '+derived(self.secret,'rover-site'),request.headers['Authorization'])
   return web.json_response({'ready':True,'owner':'server'})
  async def stop(request):return web.json_response({'stopped':await request.json()})
  async def lane(request):
   ws=web.WebSocketResponse();await ws.prepare(request)
   await ws.send_bytes(b'fresh-frame')
   async for message in ws:
    if message.type==WSMsgType.TEXT:await ws.send_str(message.data)
   return ws
  async def snapshot(request):return web.json_response({'entities':['x'*1000]*10000,'complete':True})
  self.revoked=False
  async def inspect(request):
   self.assertEqual(derived(self.secret,'router-inspect'),request.headers['X-Router-Inspect'])
   value=await request.json()
   if self.revoked or value['authority']!='owner':raise web.HTTPConflict()
   return web.json_response({'id':value['session'],'authority':'owner','state':'open'})
  local.router.add_post('/internal/stream/inspect',inspect)
  local.router.add_post('/api/map/snapshot',snapshot)
  async def video(request):return web.json_response({'received':len(await request.read())})
  local.router.add_post('/api/video',video)
  local.router.add_get('/api/state',state);local.router.add_post('/api/controls',stop)
  local.router.add_get('/api/live/{lane}',lane)
  self.local=await self.start(local)
  self.outbound=Outbound(self.public,self.local,self.secret)
  self.task=asyncio.create_task(self.outbound.run())
  self.client=ClientSession(headers={'Authorization':'Bearer '+derived(self.secret,'rover-site')})
  for _ in range(100):
   if self.ingress.link is not None:break
   await asyncio.sleep(.01)
  self.assertIsNotNone(self.ingress.link)
 async def start(self,app):
  return await self.start_server(app)
 async def test_recording_chunks_cross_ingress_without_changing_control_limits(self):
  async with self.client.post(self.public+'/api/video',data='x'*350000) as r:
   self.assertEqual(r.status,200);self.assertEqual((await r.json())['received'],350000)
  async with self.client.post(self.public+'/api/controls',data='x'*5000) as r:
   self.assertEqual(r.status,413)
 async def start_server(self,app):
  runner=web.AppRunner(app);await runner.setup();self.addAsyncCleanup(runner.cleanup)
  site=web.TCPSite(runner,'127.0.0.1',0);await site.start()
  return 'http://127.0.0.1:'+str(site._server.sockets[0].getsockname()[1])
 async def asyncTearDown(self):
  self.task.cancel()
  with contextlib.suppress(asyncio.CancelledError):await self.task
  await self.client.close()
 async def test_http_state_stop_and_authorization(self):
  async with self.client.get(self.public+'/api/state') as r:
   self.assertEqual(200,r.status);self.assertEqual('server',(await r.json())['owner'])
  async with self.client.post(self.public+'/api/controls',json={'stop':True}) as r:
   self.assertEqual({'stopped':{'stop':True}},await r.json())
  async with self.client.get(self.public+'/api/state',headers={'Authorization':'Bearer wrong'}) as r:self.assertEqual(401,r.status)
 async def test_large_snapshot_uses_chunks_and_keeps_control_link_alive(self):
  async with self.client.post(self.public+'/api/map/snapshot',json={}) as r:
   self.assertEqual(200,r.status)
   result=await r.json()
   self.assertTrue(result['complete']);self.assertEqual(10000,len(result['entities']))
  async with self.client.get(self.public+'/api/state') as r:self.assertEqual(200,r.status)
  self.assertIsNotNone(self.ingress.link)
 async def test_authoritative_inspection_uses_private_peer_and_fails_closed(self):
  value=await self.ingress.inspect('owner','session')
  self.assertEqual('open',value['state'])
  with self.assertRaises(ConnectionError):await self.ingress.inspect('other-authority','session')
  self.revoked=True
  with self.assertRaises(ConnectionError):await self.ingress.inspect('owner','session')
  self.assertEqual({},self.ingress.pending)
 async def test_independent_live_lanes_and_client_detach_preserve_bridge(self):
  async with self.client.ws_connect(self.public+'/api/live/vision') as vision:
   async with self.client.ws_connect(self.public+'/api/live/control') as control:
    self.assertEqual(b'fresh-frame',(await vision.receive(timeout=2)).data)
    self.assertEqual(b'fresh-frame',(await control.receive(timeout=2)).data)
    await control.send_str('neutral');self.assertEqual('neutral',(await control.receive(timeout=2)).data)
   self.assertIsNotNone(self.ingress.link)
  self.assertIsNotNone(self.ingress.link)
  for _ in range(100):
   if not self.ingress.lanes and not self.outbound.tasks:break
   await asyncio.sleep(.01)
  self.assertEqual({},self.ingress.lanes);self.assertEqual({},self.outbound.tasks)
 async def test_bridge_loss_closes_live_lane_without_replaying_requests(self):
  async with self.client.ws_connect(self.public+'/api/live/control') as control:
   await control.receive(timeout=2)
   await self.ingress.link.close()
   message=await control.receive(timeout=2)
   self.assertIn(message.type,(WSMsgType.CLOSE,WSMsgType.CLOSED,WSMsgType.CLOSING))
  self.assertEqual({},self.ingress.pending)

if __name__=='__main__':unittest.main()
