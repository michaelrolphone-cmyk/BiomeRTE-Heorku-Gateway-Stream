import asyncio
import contextlib
import time
import unittest
from aiohttp.test_utils import TestClient, TestServer
from mac.site_bridge import SiteBridge
from mac.camera_light import CameraLight

class Rover:
    control_ready=True
    descriptors={}
    def __init__(self): self.sent=[]
    async def start(self): pass
    async def close(self): pass
    def discard_intent(self): pass
    def desired(self,mask,ttl):
        self.sent.append(mask)
        return dict(accepted_sequence=len(self.sent),received_or_applied=False)
    @contextlib.asynccontextmanager
    async def subscribe(self):
        yield {lane:asyncio.Queue(1) for lane in ('vision','map','control')}

class Camera(CameraLight):
    def __init__(self): self.entered=asyncio.Event();self.release=asyncio.Event();self.calls=[]
    async def close(self): pass
    async def request(self,body):
        self.calls.append(body);self.entered.set();await self.release.wait()
        if body.get('led')=={'invalid':True}:raise ValueError('Invalid LED power, brightness or color')
        return dict(led=dict(on=False,brightness=1,color='white'),hardware_verified=False,optical_readback=False)

class CameraSocketTests(unittest.IsolatedAsyncioTestCase):
    async def test_slow_led_preserves_drive_clock_and_correlated_result(self):
        rover=Rover();bridge=SiteBridge(rover,'x'*32,dict(forward=1,reverse=2,left=3,right=4));camera=Camera();bridge.camera_light=camera
        async with TestClient(TestServer(bridge.app())) as client:
            async with client.ws_connect('/api/live/control',headers={'Authorization':'Bearer '+'x'*32}) as ws:
                state=await ws.receive_json();await ws.send_json(dict(type='led',id='first',led=dict(on=False,brightness=1,color='white')))
                await asyncio.wait_for(camera.entered.wait(),1)
                try:
                    await ws.send_json(dict(type='clock',id=17));reply=await asyncio.wait_for(ws.receive_json(),.5);self.assertEqual('clock',reply['type'])
                    await ws.send_json(dict(type='led',id='busy'));reply=await ws.receive_json();self.assertEqual('busy',reply['id']);self.assertEqual('LED_BUSY',reply['error']['code'])
                    await ws.send_json(dict(type='intent',sequence=1,epoch=state['epoch'],stop_generation=state['stop_generation'],issued_at=time.time(),directions=dict(forward=True,reverse=False,left=False,right=False)))
                    reply=await asyncio.wait_for(ws.receive_json(),.5);self.assertEqual('accepted',reply['type']);self.assertEqual([1],rover.sent)
                finally:camera.release.set()
                reply=await asyncio.wait_for(ws.receive_json(),1);self.assertEqual('led',reply['type']);self.assertEqual('first',reply['id']);self.assertTrue(reply['applied']);self.assertFalse(reply['hardware_verified']);self.assertEqual(1,len(camera.calls))
    async def test_led_validation_failure_keeps_socket_usable(self):
        bridge=SiteBridge(Rover(),'x'*32,dict(forward=1,reverse=2,left=3,right=4));camera=Camera();camera.release.set();bridge.camera_light=camera
        async with TestClient(TestServer(bridge.app())) as client:
            async with client.ws_connect('/api/live/control',headers={'Authorization':'Bearer '+'x'*32}) as ws:
                await ws.receive_json();await ws.send_json(dict(type='led',id='bad',led={'invalid':True}));reply=await ws.receive_json();self.assertEqual('bad',reply['id']);self.assertEqual('INVALID_LED',reply['error']['code'])
                await ws.send_json(dict(type='led',id='read'));reply=await ws.receive_json();self.assertEqual('read',reply['id']);self.assertFalse(reply['applied'])
