import json
import unittest
from aiohttp import web, ClientSession, WSMsgType
from aiohttp.test_utils import TestServer
from biomerte.rover_transport import splice


class ClosePropagation(unittest.IsolatedAsyncioTestCase):
    async def test_error_and_close_reason_survive_both_reverse_ingress_hops(self):
        servers=[]
        async with ClientSession() as session:
            async def origin(request):
                socket=web.WebSocketResponse();await socket.prepare(request)
                msg=await socket.receive()
                self.assertEqual(b'payload',msg.data)
                await socket.send_json({'type':'error','reason':'Expected desired output state'})
                await socket.close(code=1008,message=b'Expected desired output state')
                return socket
            app=web.Application();app.router.add_get('/',origin)
            upstream=TestServer(app);await upstream.start_server();servers.append(upstream)
            for _ in range(2):
                target=str(upstream.make_url('/'))
                async def proxy(request,target=target):
                    socket=web.WebSocketResponse();await socket.prepare(request)
                    async with session.ws_connect(target) as local:
                        await splice(socket,local)
                    return socket
                app=web.Application();app.router.add_get('/',proxy)
                upstream=TestServer(app);await upstream.start_server();servers.append(upstream)
            try:
                async with session.ws_connect(upstream.make_url('/')) as socket:
                    await socket.send_bytes(b'payload')
                    error=await socket.receive_json(timeout=2)
                    self.assertEqual('Expected desired output state',error['reason'])
                    closed=await socket.receive(timeout=2)
                    self.assertEqual(WSMsgType.CLOSE,closed.type)
                    self.assertEqual(1008,closed.data)
                    self.assertEqual(error['reason'],closed.extra)
            finally:
                for server in reversed(servers):await server.close()
