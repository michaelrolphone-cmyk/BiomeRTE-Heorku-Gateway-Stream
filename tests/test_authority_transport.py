import asyncio
import contextlib
import hashlib
import hmac
import json
import time
import unittest
from unittest.mock import patch
from aiohttp import web, ClientSession, WSServerHandshakeError
from biomerte.authority_transport import Authorities, authority_key
from biomerte.public_server import application
from biomerte.rover_transport import derived

NODE = 'a' * 64
OTHER = 'b' * 64
SECRET = 'private-fixture-secret-' * 3
SID = 'c' * 32


class AuthorityTransportTests(unittest.IsolatedAsyncioTestCase):
    async def start(self, app):
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, '127.0.0.1', 0)
        await site.start()
        return 'http://127.0.0.1:' + str(site._server.sockets[0].getsockname()[1])

    def request(self, authority=NODE, session=SID, timestamp=None, secret=SECRET):
        raw = json.dumps(dict(authority=authority, session=session,
                              timestamp=int(time.time()) if timestamp is None else timestamp)).encode()
        signature = hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()
        return raw, {'Content-Type': 'application/json', 'Authorization': 'Router ' + signature}

    async def peer(self, client, base, node=NODE):
        return await client.ws_connect(base + '/authority/' + node + '/connect',
            headers={'Authorization': 'Authority ' + authority_key(SECRET, node)})

    async def test_coexists_with_mac_bridge_and_revalidates_every_read(self):
        base = await self.start(application(dict(secrets={NODE:SECRET, OTHER:SECRET},
            inspect_url='https://queued.invalid/inspect', router_owner='mac_bridge',
            rover_authority=OTHER)))
        async with ClientSession() as client:
            async with client.ws_connect(base + '/rover/bridge',
                    headers={'Authorization':'Bridge ' + derived(SECRET, 'rover-link')}) as mac:
                async with await self.peer(client, base) as pi:
                    async with client.get(base + '/health') as response:
                        health = await response.json()
                        self.assertTrue(health['rover_bridge'])
                        self.assertEqual(health['authority_inspection_links'], [NODE])
                    raw, headers = self.request()
                    ids = []
                    for status, generation in ((200,7),(200,8),(409,None)):
                        request = asyncio.create_task(client.post(base + '/stream/inspect',
                                                                  data=raw, headers=headers))
                        value = await pi.receive_json(timeout=2)
                        self.assertEqual((value['authority'], value['session']), (NODE, SID))
                        ids.append(value['id'])
                        reply = dict(type='inspect_result', id=value['id'], status=status)
                        if status == 200:
                            reply['grant'] = dict(authority=NODE, id=SID, generation=generation)
                        await pi.send_json(reply)
                        response = await request
                        async with response:
                            self.assertEqual(response.status, status)
                            if status == 200:
                                self.assertEqual((await response.json())['generation'], generation)
                    self.assertEqual(len(set(ids)), 3)
                    self.assertFalse(mac.closed)

    async def test_bad_or_stale_hmac_and_unknown_authority_never_reach_peer(self):
        authorities = Authorities({NODE:SECRET})
        app = web.Application(client_max_size=4096)
        authorities.mount(app)
        base = await self.start(app)
        async with ClientSession() as client:
            for args in ({'secret':'wrong'}, {'timestamp':int(time.time())-60},
                         {'authority':OTHER}, {'session':'bad-session'},
                         {'timestamp':True}):
                raw, headers = self.request(**args)
                async with client.post(base + '/stream/inspect', data=raw, headers=headers) as response:
                    self.assertEqual(response.status, 401)
            self.assertEqual(authorities.pending, {})
            with self.assertRaises(WSServerHandshakeError):
                await client.ws_connect(base + '/authority/' + NODE + '/connect',
                                        headers={'Authorization':'Authority wrong'})

    async def test_disconnect_fails_pending_read_and_reconnect_has_no_old_reply(self):
        authorities = Authorities({NODE:SECRET})
        app = web.Application()
        authorities.mount(app)
        base = await self.start(app)
        async with ClientSession() as client:
            peer = await self.peer(client, base)
            raw, headers = self.request()
            pending = asyncio.create_task(client.post(base + '/stream/inspect',data=raw,headers=headers))
            old = await peer.receive_json(timeout=2)
            await peer.close()
            async with await asyncio.wait_for(pending, 2) as response:
                self.assertEqual(response.status, 503)
            self.assertEqual(authorities.pending, {})
            async with await self.peer(client, base) as new:
                pending = asyncio.create_task(client.post(base + '/stream/inspect',data=raw,headers=headers))
                current = await new.receive_json(timeout=2)
                self.assertNotEqual(current['id'], old['id'])
                await new.send_json(dict(type='inspect_result',id=old['id'],status=200,
                                         grant=dict(id=SID,authority=NODE,generation=1)))
                await asyncio.sleep(0)
                self.assertFalse(pending.done())
                await new.send_json(dict(type='inspect_result',id=current['id'],status=409))
                async with await pending as response:
                    self.assertEqual(response.status, 409)

    async def test_wrong_peer_or_binding_cannot_satisfy_request(self):
        authorities = Authorities({NODE:SECRET,OTHER:SECRET})
        app = web.Application(); authorities.mount(app)
        base = await self.start(app)
        async with ClientSession() as client:
            async with await self.peer(client,base) as pi, await self.peer(client,base,OTHER) as other:
                raw, headers = self.request()
                pending=asyncio.create_task(client.post(base+'/stream/inspect',data=raw,headers=headers))
                value=await pi.receive_json(timeout=2)
                await other.send_json(dict(type='inspect_result',id=value['id'],status=200,
                                           grant=dict(id=SID,authority=NODE)))
                await pi.send_json(dict(type='inspect_result',id=value['id'],status=200,
                                        grant=dict(id='d'*32,authority=NODE)))
                await asyncio.sleep(0)
                self.assertFalse(pending.done())
                await pi.send_json(dict(type='inspect_result',id=value['id'],status=200,
                                        grant=dict(id=SID,authority=NODE,generation=9)))
                async with await pending as response:
                    self.assertEqual((await response.json())['generation'],9)

    async def test_timeout_is_unavailable_and_pending_memory_is_released(self):
        authorities=Authorities({NODE:SECRET},timeout=.02)
        app=web.Application(); authorities.mount(app)
        base=await self.start(app)
        async with ClientSession() as client:
            async with await self.peer(client,base):
                raw,headers=self.request()
                async with client.post(base+'/stream/inspect',data=raw,headers=headers) as response:
                    self.assertEqual(response.status,503)
                self.assertEqual(authorities.pending,{})


if __name__ == '__main__':
    unittest.main()
