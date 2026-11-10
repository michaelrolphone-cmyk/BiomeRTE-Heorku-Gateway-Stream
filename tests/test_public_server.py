import asyncio,unittest
from unittest.mock import patch
from aiohttp import web,ClientSession,WSMsgType
from biomerte.public_server import application
from biomerte.stream_router import frame
from test_router_integration import grant,token,NODE,SECRET

class PublicServerTests(unittest.IsolatedAsyncioTestCase):
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

if __name__=='__main__':unittest.main()
