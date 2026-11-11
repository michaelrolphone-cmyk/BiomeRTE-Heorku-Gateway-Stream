import asyncio
import contextlib
import time
import unittest
from unittest.mock import patch
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from mac.site_bridge import SiteBridge

class Rover:
    control_ready=True
    descriptors={}
    def __init__(self):
        self.sent=[]
        self.all_inputs=asyncio.Event()
    async def start(self):pass
    async def close(self):pass
    def discard_intent(self):pass
    def desired(self,mask,ttl):
        self.sent.append((mask,ttl))
        if len(self.sent)>=3:self.all_inputs.set()
        return dict(accepted_sequence=len(self.sent),received_or_applied=False)
    @contextlib.asynccontextmanager
    async def subscribe(self):
        yield {lane:asyncio.Queue(1) for lane in ('vision','map','control')}

class SlowControlReply(unittest.IsolatedAsyncioTestCase):
    async def test_blocked_receipt_does_not_block_following_inputs(self):
        rover=Rover()
        bridge=SiteBridge(rover,'x'*32,dict(forward=1,reverse=2,left=3,right=4))
        entered=asyncio.Event()
        release=asyncio.Event()
        original=web.WebSocketResponse.send_json
        async def slow(socket,value,*args,**kwargs):
            if value.get('type')=='accepted':
                entered.set()
                await release.wait()
            return await original(socket,value,*args,**kwargs)
        with patch.object(web.WebSocketResponse,'send_json',slow):
            async with TestClient(TestServer(bridge.app())) as client:
                async with client.ws_connect('/api/live/control',headers={'Authorization':'Bearer '+'x'*32}) as ws:
                    state=await ws.receive_json()
                    def intent(sequence,held):
                        return dict(type='intent',sequence=sequence,epoch=state['epoch'],
                            stop_generation=state['stop_generation'],issued_at=time.time(),
                            directions=dict(forward=held,reverse=False,left=False,right=False))
                    try:
                        await ws.send_json(intent(1,True))
                        await asyncio.wait_for(entered.wait(),1)
                        await ws.send_json(intent(2,True))
                        await ws.send_json(intent(3,False))
                        # The release must reach the native owner while the first
                        # receipt remains blocked. This fails on the old adapter.
                        await asyncio.wait_for(rover.all_inputs.wait(),.5)
                        self.assertEqual([(1,500),(1,500),(0,500)],rover.sent)
                    finally:release.set()

if __name__=='__main__':unittest.main()
