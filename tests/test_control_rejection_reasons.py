import asyncio
import contextlib
import time
import unittest
from aiohttp.test_utils import TestClient, TestServer
from mac.site_bridge import SiteBridge

class Rover:
    control_ready = True
    descriptors = {}
    async def start(self): pass
    async def close(self): pass
    def discard_intent(self): pass
    def desired(self, mask, ttl):
        return {'accepted_sequence': 1, 'received_or_applied': False}
    @contextlib.asynccontextmanager
    async def subscribe(self):
        yield {name: asyncio.Queue() for name in ('vision', 'map', 'control')}

class RejectionReasons(unittest.IsolatedAsyncioTestCase):
    async def test_wire_distinguishes_expired_epoch_and_malformed_intents(self):
        bridge = SiteBridge(Rover(), 'x'*32,
                            {'forward':1, 'reverse':2, 'left':3, 'right':4})
        async with TestClient(TestServer(bridge.app())) as client:
            async with client.ws_connect('/api/live/control',
                    headers={'Authorization':'Bearer '+'x'*32}) as socket:
                state = await socket.receive_json()
                base = dict(type='intent', sequence=1, epoch=state['epoch'],
                            stop_generation=state['stop_generation'],
                            issued_at=time.time(),
                            directions=dict(forward=True, reverse=False, left=False, right=False))
                for change, expected in [
                    ({'issued_at':time.time()-2}, 'Expired browser intent'),
                    ({'epoch':'old'}, 'Superseded browser control epoch'),
                    ({'directions':{}}, 'Expected four Boolean directions'),
                ]:
                    await socket.send_json(dict(base, **change))
                    reply = await socket.receive_json()
                    self.assertEqual(reply['type'], 'rejected')
                    self.assertTrue(reply['message'].startswith(expected), reply)
                await socket.send_json(dict(base, issued_at=time.time()))
                reply = await socket.receive_json()
                self.assertEqual(reply['type'], 'accepted')
