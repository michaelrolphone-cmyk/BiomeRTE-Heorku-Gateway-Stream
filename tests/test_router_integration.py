import asyncio,base64,hashlib,hmac,json,time,unittest
from unittest.mock import patch
from biomerte.stream_router import Router,Session,Fault,credential,encode,_inspect
from biomerte.heroku_stream_router import build_config

NODE='a'*64
SECRET='test-only-'*8
def grant():
 return dict(id='b'*32,authority=NODE,epoch='e',generation=2,contract='vision:1',
  resource='camera',lease='lease',provider='p',server='s',realm='site',site='site',
  state='open',role='provider',credential_expires_at=time.time()+60,expires_at=time.time()+120,
  lanes=['vision'],direction='provider_to_server',delivery='supersedable',
  limits=dict(max_message_bytes=4096,buffer_depth=1,messages_per_second=30))
def token(g):
 raw=encode(g).encode();return base64.urlsafe_b64encode(raw).decode().rstrip('=')+'.'+hmac.new(SECRET.encode(),raw,hashlib.sha256).hexdigest()

class ConfigTests(unittest.TestCase):
 def test_deployment_name_does_not_replace_real_authorities(self):
  c=build_config({'STREAM_ROUTER_SECRET':SECRET,'CONTROLLER_NODE_ID':'BiomeRTE-Heorku-Gateway-Stream'})
  self.assertEqual(2,len(c['secrets']));self.assertTrue(all(len(n)==64 for n in c['secrets']))
  self.assertTrue(c['inspect_url'].startswith('https://'))
 def test_explicit_authority_and_no_secrets_in_errors(self):
  self.assertEqual({NODE:SECRET},build_config({'STREAM_ROUTER_SECRET':SECRET,'CONTROLLER_NODE_ID':NODE})['secrets'])
  with self.assertRaises(ValueError):build_config({})
 def test_inspect_mac_covers_exact_session_authority_and_time(self):
  captured=[]
  class Reply:
   def __enter__(self):return self
   def __exit__(self,*args):pass
   def read(self):return b'{"state":"open"}'
  def open(req,timeout):captured.append(req);return Reply()
  with patch('urllib.request.urlopen',open):_inspect('https://gateway/inspect','b'*32,NODE,SECRET)
  r=captured[0];body=json.loads(r.data)
  self.assertEqual(NODE,body['authority']);self.assertEqual('b'*32,body['session'])
  self.assertLess(abs(time.time()-body['timestamp']),1)
  self.assertEqual('Router '+hmac.new(SECRET.encode(),r.data,hashlib.sha256).hexdigest(),r.headers['Authorization'])
 def test_credential_unknown_or_expired_authority_fails(self):
  g=grant();self.assertEqual(NODE,credential(token(g),{NODE:SECRET})['authority'])
  with self.assertRaises(Fault):credential(token(g),{})
  g['expires_at']=0
  with self.assertRaises(Fault):credential(token(g),{NODE:SECRET})

class RouterTests(unittest.IsolatedAsyncioTestCase):
 async def test_authoritative_validation_rejects_released_and_replaced_epoch(self):
  g=grant();state=dict(g);router=Router({NODE:SECRET},{NODE:lambda sid:dict(state)})
  await router.validate(g)
  state['state']='released'
  with self.assertRaises(Fault):await router.validate(g)
  state.update(state='open',epoch='replacement')
  with self.assertRaises(Fault):await router.validate(g)
 async def test_signature_only_fallback_is_not_valid_lease_evidence(self):
  g=grant();r=Router({NODE:SECRET},{NODE:lambda sid:{'id':sid,'state':'open'}})
  with self.assertRaises(Fault):await r.validate(g)
 async def test_live_peers_pair_frame_and_revocation_closes_both(self):
  from websockets.asyncio.server import serve
  from websockets.asyncio.client import connect
  g=grant();r=Router({NODE:SECRET},{NODE:lambda sid:dict(g)})
  async def handle(ws):
   try:await r.attach(ws,ws.request.path,ws.request.headers['Authorization'])
   except Exception:await ws.close(1008,'Rejected')
  async with serve(handle,'127.0.0.1',0) as server:
   url='ws://127.0.0.1:'+str(server.sockets[0].getsockname()[1])+'/'+g['id']+'/vision'
   sg=dict(g,role='server')
   async with connect(url,additional_headers={'Authorization':'Stream '+token(sg)}) as receiver:
    async with connect(url,additional_headers={'Authorization':'Stream '+token(g)}) as sender:
     from biomerte.stream_router import frame
     metadata=dict(authority=NODE,epoch='e',generation=2,sequence=1,frame_id='f',source_timestamp=None,processing_timestamp=time.time(),calibration_revision=1,map_revision=1)
     data=frame(metadata,b'\xff\xd8jpeg');await sender.send(data)
     self.assertEqual(data,await asyncio.wait_for(receiver.recv(),2))
     g['state']='released'
     await asyncio.wait_for(receiver.wait_closed(),3);await asyncio.wait_for(sender.wait_closed(),3)
     self.assertEqual(1008,receiver.close_code)

if __name__=='__main__':unittest.main()
